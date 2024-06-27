mod endpoint;
mod raft;
use std::{
	any::TypeId,
	collections::VecDeque,
	iter,
	marker::PhantomData,
	net::SocketAddr,
	sync::{
		atomic::{AtomicBool, Ordering},
		Arc,
	},
	time::{Duration, Instant},
};

use derive_more::*;
use endpoint::{ActivePeer, Endpoint};
use hashbrown::HashMap;
use left_right::{Absorb, ReadHandle, ReadHandleFactory, WriteHandle};
use raft::{ChannelId, Node, Role};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::sync::mpsc::{self, Receiver, Sender, UnboundedReceiver, UnboundedSender};
use tracing::{debug, error, info, trace};
use uuid::Uuid;

#[derive(
	Serialize,
	Deserialize,
	Debug,
	Clone,
	Copy,
	Hash,
	PartialEq,
	Eq,
	PartialOrd,
	Ord,
	Deref,
	DerefMut,
)]
pub struct Id(pub Uuid);

enum Request {
	Create {
		channel_id: ChannelId,
		status_write: WriteHandle<Status, StatusOp>,
	},
}

enum Response {
	Created {
		channel_id: ChannelId,
		tx: UnboundedSender<Vec<u8>>,
		rx: UnboundedReceiver<Vec<u8>>,
	},
}

pub struct Cluster {
	req_tx: Sender<Request>,
	res_rx: Receiver<Response>,
}

pub struct ChannelInfo {
	tx: UnboundedSender<Vec<u8>>,
	rx: UnboundedReceiver<Vec<u8>>,
	status_write: WriteHandle<Status, StatusOp>,
	started: Instant,
	last_stat_update: Instant,
}

impl Cluster {
	pub fn connect(
		local_address: SocketAddr,
		remote_addresses: impl IntoIterator<Item = SocketAddr> + Send + 'static,
	) -> Self {
		let node_id = Uuid::new_v4();
		let node_timeout = Duration::from_millis(1000);

		let (req_tx, mut req_rx) = mpsc::channel(8192);
		let (mut res_tx, res_rx) = mpsc::channel(8192);

		let mut transmitted = 0;

		let connection_handle = tokio::spawn(async move {
			let mut node = raft::Node::new(node_id, node_timeout);
			let mut endpoint = Endpoint::connect(Id(node_id), local_address, remote_addresses);
			let mut channels = HashMap::<ChannelId, ChannelInfo>::new();
			loop {
				tokio::time::sleep(Duration::from_micros(100)).await;
				let mut reqs = vec![];
				while let Ok(req) = req_rx.try_recv() {
					reqs.push(req);
				}
				for req in reqs {
					match req {
						Request::Create {
							channel_id,
							status_write,
						} => {
							let (tx_a, rx_a) = mpsc::unbounded_channel();
							let (tx_b, rx_b) = mpsc::unbounded_channel();
							let (tx, rx) = (tx_a, rx_b);
							let (c_tx, c_rx) = (tx_b, rx_a);

							channels.insert(
								channel_id,
								ChannelInfo {
									tx: c_tx,
									rx: c_rx,
									status_write,
									started: Instant::now(),
									last_stat_update: Instant::now(),
								},
							);

							res_tx
								.send(Response::Created { channel_id, tx, rx })
								.await
								.unwrap();
						}
					}
				}
				for (&channel_id, channel_info) in &mut channels {
					let ChannelInfo {
						tx,
						rx,
						status_write,
						started,
						last_stat_update,
					} = channel_info;
					let mut update_stats = || {
						let (ping, jitter) = {
							const MICROS_IN_SECOND: u128 = Duration::from_secs(1).as_micros();
							let all_pings = node
								.self_stats
								.net
								.iter()
								.flat_map(|(_, stat)| stat.pings.iter())
								.collect::<Vec<_>>();
							let ping = all_pings
								.iter()
								.copied()
								.map(Duration::as_micros)
								.sum::<u128>() as f32 / all_pings.len() as f32
								/ MICROS_IN_SECOND as f32;
							let highest_ping = all_pings
								.iter()
								.copied()
								.map(Duration::as_micros)
								.fold(0, u128::max);
							let lowest_ping = all_pings
								.iter()
								.copied()
								.map(Duration::as_micros)
								.fold(u128::MAX, u128::min);
							let jitter =
								(highest_ping - lowest_ping) as f32 / MICROS_IN_SECOND as f32;
							(ping, jitter)
						};
						status_write.append(StatusOp::Ping(ping));
						status_write.append(StatusOp::Jitter(jitter));
						let channel = node.channel(channel_id);
						let throughput =
							transmitted as f32 / (Instant::now() - *started).as_secs_f32();
						status_write.append(StatusOp::Throughput(throughput));
						status_write.append(StatusOp::Term(channel.term));
						status_write.append(StatusOp::State(match channel.role {
							Role::Leader { .. } => State::Leader,
							Role::Candidate { .. } => State::Candidate,
							_ => State::Follower,
						}));
						status_write.append(StatusOp::Commit(channel.commit_index));
						status_write.publish();
					};
					if Instant::now() > *last_stat_update + Duration::from_millis(200) {
						*last_stat_update = Instant::now();
						(update_stats)();
					}
					let channel = node.channel(channel_id);
					while let Ok(data) = rx.try_recv() {
						transmitted += 1;
						Node::put(channel, data).unwrap();
					}
					for data in Node::ready(channel).drain(..) {
						transmitted += 1;
						tx.send(data).unwrap();
					}
				}
				Self::tick(&mut node, &mut endpoint).await;
			}
		});

		Self { req_tx, res_rx }
	}

	pub async fn tick(node: &mut Node, endpoint: &mut Endpoint) {
		endpoint.step().await;

		node.peers = endpoint.peer_ids();

		for msg in node.step() {
			trace!("SEND {:?}", msg);

			endpoint
				.send(msg.to, bincode::serialize(&msg).unwrap())
				.await;
		}

		while let Some((_, data)) = endpoint.recv().await {
			let Ok(msg) = bincode::deserialize(serde_bytes::Bytes::new(&data)) else {
				continue;
			};
			trace!("RECV {:?}", msg);

			for msg in node.process(msg) {
				trace!("SEND {:?}", msg);
				endpoint
					.send(msg.to, bincode::serialize(&msg).unwrap())
					.await;
			}
		}
	}
}

pub trait Datatype {
	fn id() -> Uuid;
}

pub struct Dataset<T> {
	tx: UnboundedSender<Vec<u8>>,
	rx: UnboundedReceiver<Vec<u8>>,
	backlog: VecDeque<T>,
	status_read: ReadHandle<Status>,
	marker: PhantomData<T>,
}

impl<T: Datatype + Serialize + DeserializeOwned> Dataset<T> {
	pub async fn create(cluster: &mut Cluster, identifier: &[u8]) -> Result<Self, ()> {
		let (status_write, status_read) = left_right::new::<Status, StatusOp>();
		cluster
			.req_tx
			.send(Request::Create {
				channel_id: ChannelId(Uuid::new_v5(&T::id(), identifier)),
				status_write,
			})
			.await
			.map_err(|_| ())?;
		let Some(Response::Created { tx, rx, .. }) = cluster.res_rx.recv().await else {
			Err(())?
		};

		Ok(Self {
			tx,
			rx,
			status_read,
			backlog: VecDeque::default(),
			marker: PhantomData,
		})
	}

	pub fn status(&self) -> Status {
		*self.status_read.enter().unwrap()
	}

	pub async fn send(&mut self, data: &T) -> Result<(), ()> {
		self.tx
			.send(bincode::serialize(data).map_err(|_| ())?)
			.map_err(|_| ())
	}

	pub async fn recv(&mut self) -> Result<Option<T>, ()> {
		while let Ok(bytes) = self.rx.try_recv() {
			self.backlog
				.push_back(bincode::deserialize(&bytes).map_err(|_| ())?);
		}
		Ok(self.backlog.pop_front())
	}
}

#[derive(Clone, Copy, Default, Debug)]
pub enum State {
	#[default]
	Follower,
	Candidate,
	Leader,
}

#[derive(Clone, Copy, Default, Debug)]
pub struct Status {
	pub state: State,
	pub term: usize,
	pub commit: usize,
	pub throughput: f32,
	pub ping: f32,
	pub jitter: f32,
}

#[derive(Clone, Copy)]
pub enum StatusOp {
	State(State),
	Term(usize),
	Commit(usize),
	Throughput(f32),
	Ping(f32),
	Jitter(f32),
}

impl Absorb<StatusOp> for Status {
	fn absorb_first(&mut self, operation: &mut StatusOp, _: &Self) {
		match *operation {
			StatusOp::Commit(commit) => self.commit = commit,
			StatusOp::Jitter(jitter) => self.jitter = jitter,
			StatusOp::Ping(ping) => self.ping = ping,
			StatusOp::State(state) => self.state = state,
			StatusOp::Term(term) => self.term = term,
			StatusOp::Throughput(throughput) => self.throughput = throughput,
		}
	}

	fn sync_with(&mut self, first: &Self) {
		*self = *first;
	}
}
