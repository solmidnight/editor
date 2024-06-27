use super::{Cluster, Id};
use core::fmt;
use derive_more::*;
use hashbrown::{HashMap, HashSet};
use ordered_float::OrderedFloat;
use rand::{thread_rng, Rng};
use serde::{Deserialize, Serialize};
use std::{
	cmp::Ordering,
	collections::{BTreeMap, VecDeque},
	convert::identity,
	iter,
	ops::{Add, Not},
	time::{Duration, Instant},
};
use sysinfo::System;
use tracing::{debug, error, info, trace, warn};
use uuid::Uuid;

const PING_BUFFER_COUNT: usize = 4;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Entry {
	pub term: usize,
	#[serde(with = "serde_bytes")]
	pub data: Vec<u8>,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Action {
	Election {
		last_log_index: Option<usize>,
		last_log_term: Option<usize>,
	},
	Append {
		prev_log_index: Option<usize>,
		prev_log_term: Option<usize>,
		commit_index: usize,
		entries: Vec<Entry>,
	},
	Heartbeat,
	Vote(Vote),
	Appended {
		success: bool,
	},
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Vote {
	Election { granted: bool },
	Recall
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Datagram {
	pub term: usize,
	pub from: Id,
	pub to: Id,
	pub channel: ChannelId,
	pub msg: Message,
}

#[derive(Serialize, Deserialize, Debug, Default, PartialEq, Eq, Clone)]
pub struct PeerNetStats {
	pub ping: Option<OrderedFloat<f32>>,
	pub jitter: Option<OrderedFloat<f32>>,
}

impl PartialOrd for PeerNetStats {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for PeerNetStats {
	fn cmp(&self, other: &Self) -> std::cmp::Ordering {
		match (self.ping, other.ping, self.jitter, other.jitter) {
			(Some(ping), Some(other_ping), Some(jitter), Some(other_jitter)) => {
				ping.cmp(&other_ping).then(jitter.cmp(&other_jitter))
			}
			(_, _, Some(_), Some(_)) => Ordering::Less,
			(Some(_), Some(_), _, _) => Ordering::Greater,
			(_, _, _, _) => Ordering::Equal,
		}
	}
}

impl From<&SelfNetStats> for PeerNetStats {
	fn from(net_stats: &SelfNetStats) -> Self {
		if net_stats.pings.len() != PING_BUFFER_COUNT {
			return Self {
				ping: None,
				jitter: None,
			};
		}

		const MICROS_IN_SECOND: u128 = Duration::from_secs(1).as_micros();
		let potential_ping = net_stats
			.pings
			.iter()
			.map(Duration::as_micros)
			.sum::<u128>() as f32
			/ net_stats.pings.len() as f32
			/ MICROS_IN_SECOND as f32;

		let (ping, jitter) = if !potential_ping.is_nan() {
			let highest_ping = net_stats
				.pings
				.iter()
				.map(Duration::as_micros)
				.fold(0, u128::max);
			let lowest_ping = net_stats
				.pings
				.iter()
				.map(Duration::as_micros)
				.fold(u128::MAX, u128::min);
			let jitter = (highest_ping - lowest_ping) as f32 / MICROS_IN_SECOND as f32;
			(Some(potential_ping), Some(jitter))
		} else {
			(None, None)
		};

		let (ping, jitter) = (ping.map(Into::into), jitter.map(Into::into));

		Self { ping, jitter }
	}
}

#[derive(Serialize, Deserialize, Debug, Default, PartialOrd, Ord, PartialEq, Eq, Clone)]
pub struct PeerStats {
	net: PeerNetStats,
	compute: ComputeStats,
	cluster: ClusterStats,
}

#[derive(Default, Clone)]
pub struct SelfNetStats {
	transmissions: HashMap<u64, Instant>,
	pub(crate) pings: VecDeque<Duration>,
}
#[derive(Serialize, Deserialize, Debug, Default, Clone, Copy, PartialOrd, PartialEq, Eq, Ord)]
pub struct ComputeStats {
	cpu: OrderedFloat<f32>,
	memory: OrderedFloat<f32>,
	io: OrderedFloat<f32>,
}
#[derive(Serialize, Deserialize, Debug, Default, Clone, Copy, PartialOrd, Ord, PartialEq, Eq)]
pub struct ClusterStats {
	elect_count: usize,
	total_leaders: usize,
	total_rx_recv: usize,
}

#[derive(Default, Clone)]
pub struct SelfStats {
	pub net: HashMap<Id, SelfNetStats>,
	pub compute: ComputeStats,
	pub cluster: ClusterStats,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Ping {
	nonce: u64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Pong {
	nonce: u64,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Message {
	Action(Action),
	Stats(PeerStats),
	Ping(Ping),
	Pong(Pong),
}

#[derive(Default, Clone, Copy)]
pub struct Status {
	pub next_index: usize,
	pub match_index: usize,
}

#[derive(Clone, Copy, Debug)]
pub struct FollowerCandidate {
	term: usize,
	last_log_index: Option<usize>,
	last_log_term: Option<usize>,
}

#[derive(Clone)]
pub enum Follower {
	Active,
	Election {
		candidates: HashMap<Id, FollowerCandidate>,
	},
}

#[derive(Clone)]
pub enum Role {
	Leader {
		follower_status: HashMap<Id, Status>,
		recalls: HashMap<Id, Instant>,
	},
	Candidate {
		votes: HashSet<Id>,
	},
	Follower(Follower),
}

impl fmt::Display for Role {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(
			f,
			"{}",
			match self {
				Self::Leader { .. } => "Leader",
				Self::Candidate { .. } => "Candidate",
				Self::Follower { .. } => "Follower",
			}
		)
	}
}

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
pub struct ChannelId(pub Uuid);

pub struct Channel {
	pub id: ChannelId,
	pub role: Role,
	pub term: usize,
	pub commit_index: usize,
	pub ready: Vec<Vec<u8>>,
	pub log: Vec<Entry>,
	pub timings: Timings,
}

pub struct Node {
	pub id: Id,
	//settings
	pub timeout: Duration,
	//operations
	pub peers: Vec<Option<Id>>,
	pub channels: HashMap<ChannelId, Channel>,
	//statistics
	pub peer_stats: HashMap<Id, PeerStats>,
	pub self_stats: SelfStats,
	pub system: System,
}

impl Node {
	pub fn new(id: Uuid, timeout: Duration) -> Self {
		Self {
			id: Id(id),
			timeout,
			peers: vec![],
			channels: HashMap::new(),
			peer_stats: HashMap::new(),
			self_stats: Default::default(),
			system: System::new_all(),
		}
	}
}

pub struct Timings {
	pub delay: Duration,
	pub range: Duration,
	pub health_check: Duration,
	pub timeout: Duration,
	pub heartbeat: Duration,
	pub vote: Duration,
	pub last_action: Instant,
	pub last_stat: Instant,
	pub back_off: usize,
}

impl Timings {
	pub fn new(timeout: Duration) -> Self {
		let range = timeout / 2;
		let heartbeat = timeout / 4;
		let health_check = timeout * 2;
		let vote = range / 2;
		Self {
			delay: Self::gen_delay(range, 0),
			range,
			timeout,
			heartbeat,
			vote,
			health_check,
			back_off: 0,
			last_action: Instant::now() - timeout,
			last_stat: Instant::now(),
		}
	}

	fn recall(&mut self, start: Instant) -> Instant {
		start + self.vote
	}

	fn act(&mut self) {
		trace!("took action");
		self.last_action = Instant::now();
	}

	fn reset_stats(&mut self) {
		self.last_stat = Instant::now();
	}

	fn reset_back_off(&mut self) {
		self.back_off = 0;
	}

	fn new_delay(&mut self) {
		self.delay = Self::gen_delay(self.range, self.back_off);
		self.back_off += 1;
	}

	fn gen_delay(range: Duration, backoff: usize) -> Duration {
		let mut x = Duration::default();
		for _ in 0..=backoff {
			x += Duration::from_secs_f64(range.as_secs_f64() * thread_rng().gen::<f64>());
		}
		x
	}

	fn heartbeat(&self) -> Instant {
		self.last_action + self.heartbeat
	}

	fn timeout(&self, total_leaders: usize) -> Instant {
		self.last_action + self.timeout * total_leaders as u32 + self.delay
	}

	fn vote(&self) -> Instant {
		self.last_action + self.vote
	}

	fn stat(&self) -> Instant {
		self.last_stat + self.health_check
	}
}

impl Node {
	pub fn channel(&mut self, channel: ChannelId) -> &mut Channel {
		self.channels.entry(channel).or_insert_with(|| Channel {
			id: channel,
			role: Role::Follower(Follower::Active),
			term: 0,
			commit_index: 0,
			ready: vec![],
			log: vec![],
			timings: Timings::new(self.timeout),
		})
	}

	pub fn put(channel: &mut Channel, data: Vec<u8>) -> Result<Vec<Datagram>, ()> {
		match channel.role {
			Role::Leader { .. } => {
				channel.log.push(Entry {
					term: channel.term,
					data,
				});
				Ok(vec![])
			}
			_ => Err(()),
		}
	}
	pub fn ready(channel: &mut Channel) -> &mut Vec<Vec<u8>> {
		&mut channel.ready
	}
	pub fn process(&mut self, datagram: Datagram) -> Vec<Datagram> {
		self.self_stats.cluster.total_rx_recv += 1;
		let self_id = self.id;
		let peer_count = self.peers.len();
		let peer_stats = self.peer_stats.clone();
		match datagram.msg {
			Message::Action(Action::Election {
				last_log_index,
				last_log_term,
			}) => {
				let quorum = peer_count / 3;
				let channel = self.channel(datagram.channel);
				let out_of_date = datagram.term > channel.term;
				let is_follower = matches!(channel.role, Role::Follower(_));
				if !is_follower && out_of_date {
					trace!("Becoming a follower. {}", &channel.role);
					channel.role = Role::Follower(Follower::Active);
					channel.term = datagram.term;
				}

				if let Role::Follower(Follower::Active) = &mut channel.role {
					channel.role = Role::Follower(Follower::Election {
						candidates: HashMap::default(),
					});
				}

				let mut response = vec![];

				if let Role::Follower(Follower::Election { candidates, .. }) = &mut channel.role {
					channel.timings.act();
					channel.timings.reset_back_off();

					candidates.insert(
						datagram.from,
						FollowerCandidate {
							term: datagram.term,
							last_log_index,
							last_log_term,
						},
					);

					if candidates.len() >= quorum && candidates.len() == 1 {
						if let Ok(Some(candidate)) = Self::elect(&peer_stats, channel) {
							response.push(Datagram {
								from: self_id,
								term: channel.term,
								channel: datagram.channel,
								to: candidate,
								msg: Message::Action(Action::Vote(Vote::Election { granted: true })),
							});
						}
					}
				}

				response
			}
			Message::Action(Action::Append { prev_log_index, prev_log_term, commit_index, entries }) => {
				let action = Action::Append { prev_log_index, prev_log_term, commit_index, entries };
				let channel = self.channel(datagram.channel);
				let Role::Follower { .. } = &mut channel.role else {
					return vec![];
				};

				let result: Result<(), ()> = Self::append_follower(action, channel, datagram.term);

				vec![Datagram {
					from: self_id,
					term: channel.term,
					channel: datagram.channel,
					to: datagram.from,
					msg: Message::Action(Action::Appended {
						success: result.is_ok(),
					}),
				}]
			}
			Message::Action(Action::Appended { success }) => {
				let channel = self.channel(datagram.channel);
				let Role::Leader { follower_status, .. } = &mut channel.role else {
					panic!("");
				};

				if datagram.term < channel.term {
					return vec![];
				}

				let status = follower_status.get_mut(&datagram.from).unwrap();

				if success {
					// If successful, update next and match indices
					status.match_index = status.next_index;
					status.next_index = channel.log.len();
				} else {
					// If failed, decrement next index
					status.next_index = status.next_index.saturating_sub(1);
				}

				// Update commit index if a majority of followers have replicated the entry
				let mut match_indices: Vec<usize> = follower_status
					.values()
					.map(|status| status.match_index)
					.collect();
				match_indices.sort();
				let majority_match_index = match_indices[(peer_count - 1) / 2];

				if majority_match_index != channel.log.len()
					&& majority_match_index > channel.commit_index
					&& channel.log[majority_match_index].term == channel.term
				{
					channel.ready.extend(
						channel.log[channel.commit_index..majority_match_index]
							.iter()
							.map(|entry| entry.data.clone()),
					);
					channel.commit_index = majority_match_index;
				}

				vec![]
			}
			Message::Action(Action::Heartbeat) => {
				trace!("received heartbeat");
				let channel = self.channel(datagram.channel);
				channel.timings.act();
				channel.timings.reset_back_off();

				let is_follower = matches!(channel.role, Role::Follower { .. });
				let is_leader = matches!(channel.role, Role::Follower { .. });
				let is_higher_term = datagram.term > channel.term;

				if is_higher_term {
					channel.term = datagram.term;
				}

				if is_follower {
					return vec![];
				}

				if is_leader && !is_higher_term {
					return vec![];
				}

				trace!("I was not elected in time, becoming a follower.");
				channel.role = Role::Follower(Follower::Active);

				vec![]
			}
			Message::Action(Action::Vote(vote)) => match vote {
				Vote::Election { granted } => {
				let majority = self.peers.len() / 2 + 1;
				let peers = self.peers.iter().copied().flatten().collect::<Vec<_>>();
				let channel = self.channel(datagram.channel);
				let Role::Candidate { votes } = &mut channel.role else {
					return vec![];
				};

				if granted {
					votes.insert(datagram.from);
					trace!("votes {} >= majority {majority}", votes.len());
				} else {
					votes.remove(&datagram.from);
					trace!("votes {} >= majority {majority}", votes.len());
				}

				if votes.len() >= majority {
					trace!("I have been elected as leader.");
					channel.role = Role::Leader {
						follower_status: peers
							.into_iter()
							.map(|peer| {
								(
									peer,
									Status {
										next_index: channel.log.len().saturating_sub(1),
										match_index: 0,
									},
								)
							})
							.collect(),
        recalls: Default::default(),
					};
					channel.timings.last_action = Instant::now() - channel.timings.heartbeat;
				}

				vec![]
			}
			Vote::Recall => {
				let channel = self.channel(datagram.channel);
				
				let Role::Leader { recalls, .. } = &mut channel.role else {
					return vec![];
				};

				recalls.insert(datagram.from, Instant::now());

				let mut remove = vec![];
				for (&peer, &notify) in &*recalls {
					if Instant::now() > channel.timings.recall(notify) {
						remove.push(peer);
					}
				}
				for peer in remove {
					recalls.remove(&peer);
				}
				
				let majority = peer_count / 2;

				if recalls.len() > majority {
					channel.role = Role::Follower(Follower::Active);
				}

				vec![]
			}
		},
			Message::Stats(stats) => {
				*self.peer_stats.entry(datagram.from).or_default() = stats;
				vec![]
			}
			Message::Ping(Ping { nonce }) => {
				vec![Datagram {
					term: self.channel(datagram.channel).term,
					from: self.id,
					to: datagram.from,
					channel: datagram.channel,
					msg: Message::Pong(Pong { nonce }),
				}]
			}
			Message::Pong(Pong { nonce }) => {
				let self_net_stats = &mut self.self_stats.net.entry(datagram.from).or_default();

				let Some(sent) = self_net_stats.transmissions.remove(&nonce) else {
					return vec![];
				};
				let ping = Instant::now() - sent;
				self_net_stats.pings.push_back(ping);
				if self_net_stats.pings.len() > PING_BUFFER_COUNT {
					self_net_stats.pings.pop_front();
				}
				vec![]
			}
		}
	}

	pub fn step(&mut self) -> Vec<Datagram> {
		let pid = sysinfo::get_current_pid().unwrap();
		let this_process = self.system.process(pid).unwrap();
		self.self_stats.compute.cpu = this_process.cpu_usage().into();
		self.self_stats.compute.memory =
			((this_process.memory() as f32) / (self.system.available_memory() as f32)).into();
		self.self_stats.compute.io = (this_process.disk_usage().read_bytes as f32
			+ this_process.disk_usage().written_bytes as f32)
			.into();
		let total_leaders = self.channels.iter().map(|(_, channel)| matches!(&channel.role, Role::Leader { .. }) as usize).sum::<usize>();
		self.self_stats.cluster.total_leaders = total_leaders;
		let self_id = self.id;
		let peers = self.peers();
		let mut self_stats = self.self_stats.clone();
		let mut response = vec![];
		let mut nonces = HashMap::new();
		for (&channel_id, channel) in &mut self.channels {
			if Instant::now() > channel.timings.stat() {
				channel.timings.reset_stats();
				response.extend(peers.iter().copied().map(|peer| Datagram {
					term: channel.term,
					from: self.id,
					to: peer,
					channel: channel_id,
					msg: Message::Stats(PeerStats {
						net: PeerNetStats::from(&*self.self_stats.net.entry(peer).or_default()),
						compute: self.self_stats.compute,
						cluster: self.self_stats.cluster,
					}),
				}));
				let mut gen_nonce = |peer: Id| -> u64 {
					let nonce = thread_rng().gen();
					nonces.insert(peer, (nonce, Instant::now()));
					nonce
				};
				response.extend(
					peers
						.iter()
						.copied()
						.filter(|peer| {
							self_stats
								.net
								.entry(*peer)
								.or_default()
								.transmissions
								.is_empty()
						})
						.map(|peer| Datagram {
							term: channel.term,
							from: self.id,
							to: peer,
							channel: channel_id,
							msg: Message::Ping(Ping {
								nonce: (gen_nonce)(peer),
							}),
						}),
				);
			}
			match channel.role.clone() {
				Role::Leader { follower_status, .. } => {
					let mut messages = vec![];
					if Instant::now() > channel.timings.heartbeat() {
						trace!("I am leader, sending out heartbeats...");
						self.self_stats.cluster.elect_count += 1;
						channel.timings.act();
						channel.timings.reset_back_off();
						'peer: for peer in self.peers.iter().map(|x| *x).filter_map(identity) {
							// Create and send Heartbeat messages
							messages.push(Datagram {
								from: self_id,
								term: channel.term,
								channel: channel_id,
								to: peer,
								msg: Message::Action(Action::Heartbeat),
							});

							// Create and send Append messages
							if let Some(status) = follower_status.get(&peer) {
								if status.next_index == channel.log.len() {
									continue 'peer;
								}
								let entries: Vec<Entry> = channel.log[status.next_index..].to_vec();
								let prev_log_index = if status.next_index > 0 {
									Some(status.next_index - 1)
								} else {
									None
								};
								let prev_log_term =
									prev_log_index.map(|index| channel.log[index].term);
								let append_message = Datagram {
									from: self_id,
									term: channel.term,
									channel: channel_id,
									to: peer,
									msg: Message::Action(Action::Append {
										prev_log_index,
										prev_log_term,
										commit_index: channel.commit_index,
										entries,
									}),
								};
								messages.push(append_message);
							}
						}
					}
					response.extend(messages);

					continue;
				}
				Role::Candidate { .. } => {
					if Instant::now() > channel.timings.timeout(total_leaders) {
						channel.timings.new_delay();
						info!("I am retrying for candidate.");
						channel.timings.act();
						channel.term += 1;
						response.extend(Self::elect_me(self_id, peers.clone(), channel));
						continue;
					}
				}
				Role::Follower(follower) => {
					if Instant::now() > channel.timings.timeout(total_leaders) {
						channel.timings.new_delay();
						info!("Im trying for candidate.");
						channel.timings.act();
						channel.role = Role::Candidate {
							votes: HashSet::default(),
						};
						channel.term += 1;
						response.extend(Self::elect_me(self_id, peers.clone(), channel));
					}
					if let Follower::Election { .. } = follower {
						if Instant::now() > channel.timings.vote() {
							let Some(candidate) = Self::elect(&self.peer_stats, channel).unwrap()
							else {
								info!("no eligible candidates");
								return vec![];
							};

							trace!("granted vote to {candidate:?}");
							response.extend(vec![Datagram {
								from: self_id,
								term: channel.term,
								channel: channel_id,
								to: candidate,
								msg: Message::Action(Action::Vote(Vote::Election { granted: true })),
							}]);
						}
					}
				}
			};
		}

		for (peer, (nonce, sent)) in nonces {
			self.self_stats
				.net
				.entry(peer)
				.or_default()
				.transmissions
				.insert(nonce, sent);
		}

		response
	}

	fn append_follower(action: Action, channel: &mut Channel, datagram_term: usize) -> Result<(), ()> {
		let Action::Append {
			prev_log_index,
			prev_log_term,
			commit_index,
			entries,
		} = action else {
			panic!("?")
		};

	if entries.is_empty() {
		Err(())?;
	}

	if datagram_term < channel.term {
		Err(())?;
	}

	let term_mismatch =
		prev_log_index
			.zip(prev_log_term)
			.map_or(false, |(index, term)| {
				index >= channel.log.len() || channel.log[index].term != term
			});

	if term_mismatch {
		Err(())?;
	}

	let index = prev_log_index.map_or(0, |i| i + 1);

	if index < channel.log.len() && channel.log[index].term != entries[0].term {
		channel.log.truncate(index);
	}

	channel
		.ready
		.extend(entries.iter().map(|entry| entry.data.clone()));
	channel.log.extend(entries);

	if commit_index > channel.commit_index {
		channel.commit_index = std::cmp::min(commit_index, channel.log.len() - 1);
	}

	Ok(())
}

	fn peers(&self) -> Vec<Id> {
		self.peers
			.iter()
			.map(|x| *x)
			.filter_map(identity)
			.collect::<Vec<_>>()
	}

	fn elect_me(self_id: Id, peers: Vec<Id>, channel: &mut Channel) -> Vec<Datagram> {
		peers
			.into_iter()
			.map(|peer| {
				let action = Action::Election {
					last_log_index: channel.log.len().checked_sub(1),
					last_log_term: channel.log.last().map(|entry| entry.term),
				};
				Datagram {
					from: self_id,
					term: channel.term,
					channel: channel.id,
					to: peer,
					msg: Message::Action(action),
				}
			})
			.collect()
	}

	fn elect(stats: &HashMap<Id, PeerStats>, channel: &mut Channel) -> Result<Option<Id>, ()> {
		let Role::Follower(Follower::Election { candidates }) = &channel.role else {
			return Err(());
		};
		channel.timings.act();
		channel.timings.reset_back_off();
		let valid_candidate = |&FollowerCandidate {
		                           term: candidate_term,
		                           last_log_index,
		                           last_log_term,
		                       }| {
			let up_to_date = candidate_term > channel.term;
			let log_correct = match (last_log_index, last_log_term) {
				(Some(last_log_index), Some(last_log_term)) => {
					let self_last_log_term = channel.log.last().map_or(0, |entry| entry.term);
					let self_last_log_index = channel.log.len().saturating_sub(1);
					last_log_term > self_last_log_term
						|| (last_log_term == self_last_log_term
							&& last_log_index >= self_last_log_index)
				}
				(None, None) => channel.log.is_empty(),
				_ => panic!("what the fuck?"),
			};
			up_to_date && log_correct
		};
		let potential_candidates = candidates
			.iter()
			.filter(|(_, candidate)| valid_candidate(*candidate))
			.map(|(id, _)| *id)
			.collect::<Vec<_>>();
		debug!("considering {potential_candidates:?}");
		let mut sorted_candidates = BTreeMap::new();
		for candidate in potential_candidates {
			sorted_candidates.insert(stats.get(&candidate).unwrap().clone(), candidate);
		}

		Ok(sorted_candidates
			.pop_first()
			.map(|(_, candidate)| candidate))
	}
}
