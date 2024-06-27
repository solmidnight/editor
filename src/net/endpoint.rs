use std::{mem, net::SocketAddr, ops::Add, sync::Arc, time::Duration};

use super::Id;
use bimap::BiMap;
use futures::Future;
use hashbrown::HashMap;
use quinn::{
	crypto::rustls::{QuicClientConfig, QuicServerConfig},
	Connection, ConnectionError, ReadError, ReadExactError, RecvStream, SendStream,
	TransportConfig, VarInt, WriteError,
};
use rand::{thread_rng, Rng};
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer, ServerName, UnixTime};
use tokio::{
	io::AsyncWriteExt,
	sync::mpsc::{
		self,
		error::{TryRecvError, TrySendError},
		Receiver, Sender,
	},
	task::JoinHandle,
	time::Instant,
};
use tracing::{error, info, trace, warn};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct PeerId(usize);

const MAX_BACKOFF: u32 = 5;
const DEADBEEF: u32 = 0xDEADBEEF;

pub struct PartitionedPeer {
	last_attempt: Instant,
	delay: Duration,
	attempt_backoff: u32,
}

pub struct ConnectedPeer {
	conn: Connection,
	handles: HashMap<usize, EstablishHandle>,
	handle_factories: Vec<Box<dyn Fn(Uuid, Connection) -> EstablishHandle + Send + 'static>>,
}

pub struct ActivePeer {
	pub id: Id,
	conn: Connection,
	stream: Stream,
}

pub enum Peer {
	Partitioned(PartitionedPeer),
	Connected(ConnectedPeer),
	Active(ActivePeer),
}

pub struct Stream {
	tx: Sender<Vec<u8>>,
	rx: Receiver<Vec<u8>>,
	tx_handle: JoinHandle<()>,
	rx_handle: JoinHandle<()>,
}
impl Stream {
	async fn close(&mut self) {
		self.tx_handle.abort();
		self.rx_handle.abort();
	}
}

impl Peer {
	fn init() -> Self {
		Self::Partitioned(PartitionedPeer {
			last_attempt: Instant::now(),
			attempt_backoff: 1,
			delay: Duration::from_secs_f64(thread_rng().gen()),
		})
	}

	fn next_attempt(&self) -> Instant {
		let Self::Partitioned(PartitionedPeer {
			last_attempt,
			attempt_backoff,
			delay,
		}) = self
		else {
			panic!("attempted to poll active peer");
		};
		*last_attempt + Duration::from_secs(1) * *attempt_backoff + *delay
	}

	fn attempt(&mut self) {
		let Self::Partitioned(PartitionedPeer {
			last_attempt,
			attempt_backoff,
			delay,
		}) = self
		else {
			panic!("attempted to poll active peer");
		};
		*attempt_backoff = attempt_backoff.add(1).min(MAX_BACKOFF);
		*last_attempt = Instant::now();
		*delay = Duration::from_secs_f64(thread_rng().gen());
	}
}

pub type ConnectJoinHandle = JoinHandle<Result<Connection, ConnectionError>>;

pub struct Endpoint {
	id: Id,
	mapping: HashMap<Id, PeerId>,
	addresses: BiMap<PeerId, SocketAddr>,
	connect_handles: HashMap<PeerId, ConnectJoinHandle>,
	accept_handle: Option<ConnectJoinHandle>,
	peers: Vec<Peer>,
	endpoint: quinn::Endpoint,
	backlog: Vec<(Id, Vec<u8>)>,
}

impl Endpoint {
	pub fn connect(
		id: Id,
		listen: SocketAddr,
		input: impl IntoIterator<Item = SocketAddr>,
	) -> Self {
		let mut addresses = BiMap::default();
		let mut peers = vec![];

		for (index, address) in input.into_iter().enumerate() {
			let id = PeerId(index);
			addresses.insert(id, address);
			peers.push(Peer::init());
		}

		let endpoint = endpoint(listen);

		Self {
			id,
			mapping: Default::default(),
			connect_handles: Default::default(),
			accept_handle: Default::default(),
			backlog: vec![],
			addresses,
			peers,
			endpoint,
		}
	}

	pub fn peer_ids(&self) -> Vec<Option<Id>> {
		(0..self.addresses.len())
			.flat_map(|id| {
				self.peers.get(id).map(|peer| match peer {
					Peer::Active(ActivePeer { id, .. }) => Some(*id),
					_ => None,
				})
			})
			.collect()
	}

	pub async fn send(&mut self, id: Id, data: Vec<u8>) {
		let Some(PeerId(peer_index)) = self.mapping.get(&id) else {
			return;
		};
		let Peer::Active(ActivePeer {
			stream, id, conn, ..
		}) = &mut self.peers[*peer_index]
		else {
			return;
		};
		let id = *id;
		if let Err(e) = stream.tx.try_send(data) {
			trace!("{:?}", e);
		}
	}

	pub async fn recv(&mut self) -> Option<(Id, Vec<u8>)> {
		loop {
			let mut empty = 0;
			for index in 0..self.peers.len() {
				if let Peer::Active(ActivePeer {
					stream, id, conn, ..
				}) = &mut self.peers[index]
				{
					match stream.rx.try_recv() {
						Ok(data) => {
							self.backlog.push((*id, data));
						}
						Err(TryRecvError::Empty) => {
							empty += 1;
						}
						_ => {
							stream.close().await;
							let handle_factories = establish_handle_factories(true);
							let handles = HashMap::new();
							trace!("downgrading conn");
							self.mapping.remove(id);
							self.peers[index] = Peer::Connected(ConnectedPeer {
								conn: conn.clone(),
								handles,
								handle_factories,
							});
							continue;
						}
					}
				}
			}
			if empty == self.active_peers().count() {
				break;
			}
		}

		self.backlog.pop()
	}

	pub async fn step(&mut self) {
		self.accept().await;
		self.poll().await;
		self.open().await;
		self.establish().await;
	}

	async fn poll(&mut self) {
		for (&id, &addr) in &self.addresses {
			let PeerId(index) = id;
			let peer = &mut self.peers[index];

			let Peer::Partitioned(PartitionedPeer {
				attempt_backoff, ..
			}) = peer
			else {
				continue;
			};
			let attempt_backoff = *attempt_backoff;

			if Instant::now() < peer.next_attempt() {
				continue;
			}

			if self.connect_handles.contains_key(&id) {
				continue;
			}

			peer.attempt();

			let connect_endpoint = self.endpoint.clone();

			self.connect_handles.insert(
				id,
				tokio::spawn(
					async move { connect_endpoint.connect(addr, "0.0.0.0").unwrap().await },
				),
			);
		}
	}

	async fn accept(&mut self) {
		self.start_accept();

		if !self.accept_handle.as_ref().unwrap().is_finished() {
			return;
		}

		let accept_handle = self.accept_handle.take().unwrap();

		self.start_accept();

		let Ok(join_result) = accept_handle.await else {
			return;
		};

		let Ok(conn) = join_result else {
			return;
		};

		let Some(PeerId(index)) = self.addresses.get_by_right(&conn.remote_address()) else {
			return;
		};

		let peer = &mut self.peers[*index];
		match peer {
			Peer::Partitioned(_) => {}
			Peer::Connected(ConnectedPeer {
				conn: other_conn,
				handles,
				..
			}) => {
				trace!("close their conn");
				other_conn.close(VarInt::from_u32(43), b"lol2");
				handles.into_iter().for_each(|(_, handle)| handle.abort());
			}
			Peer::Active(ActivePeer {
				id,
				conn: other_conn,
				..
			}) => {
				trace!("close my conn");
				conn.close(VarInt::from_u32(41), b"lol1");
				return;
			}
			_ => panic!("what?"),
		}

		let Id(uuid) = self.id;
		let handle_factories = establish_handle_factories(false);
		let handles = HashMap::new();

		*peer = Peer::Connected(ConnectedPeer {
			conn,
			handles,
			handle_factories,
		});
	}

	async fn open(&mut self) {
		let mut connected_handles = HashMap::default();
		mem::swap(&mut self.connect_handles, &mut connected_handles);
		for (id, handle) in connected_handles.into_iter() {
			let PeerId(index) = id;
			let peer = &mut self.peers[index];
			if !matches!(peer, Peer::Partitioned(_)) {
				continue;
			}
			if handle.is_finished() {
				match handle.await.unwrap() {
					Ok(conn) => {
						let handle_factories = establish_handle_factories(true);
						let handles = HashMap::new();

						*peer = Peer::Connected(ConnectedPeer {
							conn,
							handles,
							handle_factories,
						});
					}
					_ => {}
				}
			} else {
				self.connect_handles.insert(id, handle);
			}
		}
	}

	async fn establish(&mut self) {
		let mut activate = HashMap::<PeerId, Vec<ActivePeer>>::new();

		'a: for (index, peer) in self.peers.iter_mut().enumerate() {
			let peer_id = PeerId(index);

			let Peer::Connected(ConnectedPeer {
				conn,
				handles,
				handle_factories,
			}) = peer
			else {
				continue;
			};

			let conn = conn.clone();

			for (index, handle_factory) in handle_factories.iter().enumerate() {
				if !handles.contains_key(&index) {
					handles.insert(index, (handle_factory)(self.id.0, conn.clone()));
				}
			}

			let mut _handles = HashMap::new();
			mem::swap(handles, &mut _handles);

			for (factory_index, handle) in _handles {
				if handle.is_finished() {
					match handle.await.unwrap() {
						Ok((id, tx, rx, tx_handle, rx_handle)) => {
							let id = Id(id);
							activate.entry(peer_id).or_default().push(ActivePeer {
								id,
								conn,
								stream: Stream {
									tx,
									rx,
									tx_handle,
									rx_handle,
								},
							});
							break;
						}
						Err(Ok(_)) => {
							trace!("err ok")
						}
						Err(Err(_)) => {
							*peer = Peer::init();
							trace!("resetting conn");
							continue 'a;
						}
					}
				} else {
					handles.insert(factory_index, handle);
				}
			}
		}

		for (peer_id, mut active_peers) in activate {
			match active_peers.len() {
				1 => {
					let peer = active_peers.pop().unwrap();
					self.mapping.insert(peer.id, peer_id);
					trace!("admitting peer {:?} {:?}", peer.id, peer_id);
					self.peers[peer_id.0] = Peer::Active(peer);
				}
				2 => {
					let has_authority =
						self.id.as_u128() > active_peers.last().unwrap().id.as_u128();
					if has_authority {
						trace!("i have authority over peer {:?}", peer_id);
					} else {
						trace!("no authority over peer {:?}", peer_id);
					}

					if has_authority {
						let chosen = active_peers.pop().unwrap();
						let dead = active_peers.pop().unwrap();
						dead.conn.close(VarInt::from_u32(42), b"");
						trace!("admitting peer {:?} {:?}", chosen.id, peer_id);
						self.mapping.insert(chosen.id, peer_id);
						self.peers[peer_id.0] = Peer::Active(chosen);
					} else {
						let mut opt_a = active_peers.pop().unwrap();
						let mut opt_b = active_peers.pop().unwrap();
						tokio::select! {
							_ = opt_a.conn.closed() => {},
							_ = opt_b.conn.closed() => { mem::swap(&mut opt_a, &mut opt_b)},
						}
						let chosen = opt_a;
						let _dead = opt_b;
						trace!("admitting peer {:?} {:?}", chosen.id, peer_id);
						self.mapping.insert(chosen.id, peer_id);
						self.peers[peer_id.0] = Peer::Active(chosen);
					}
				}
				_ => panic!("?"),
			}
		}
	}

	pub fn active_peers(&self) -> impl Iterator<Item = &ActivePeer> {
		self.peers.iter().filter_map(|peer| match peer {
			Peer::Active(x) => Some(x),
			_ => None,
		})
	}

	fn start_accept(&mut self) {
		if self.accept_handle.is_none() {
			let accept_endpoint = self.endpoint.clone();
			self.accept_handle = Some(tokio::spawn(async move {
				let incoming = loop {
					let r = accept_endpoint.accept().await;
					let Some(incoming) = r else {
						tokio::time::sleep(Duration::from_millis(200)).await;
						continue;
					};
					break incoming;
				};
				incoming.await
			}));
		}
	}
}

fn establish_handle_factories(
	open: bool,
) -> Vec<Box<dyn Fn(Uuid, Connection) -> EstablishHandle + Send + 'static>> {
	let accept_handle = Box::new(|id: Uuid, conn: Connection| {
		tokio::spawn(establish(id, async move { conn.accept_bi().await }))
	}) as Box<dyn Fn(Uuid, Connection) -> EstablishHandle + Send + 'static>;
	let mut handles = vec![accept_handle];
	if open {
		let open_handle = Box::new(|id: Uuid, conn: Connection| {
			tokio::spawn(establish(id, async move { conn.open_bi().await }))
		}) as Box<dyn Fn(Uuid, Connection) -> EstablishHandle + Send + 'static>;
		handles.push(open_handle);
	}

	handles
}

fn random_duration(min: Duration, max: Duration) -> Duration {
	let mut rng = thread_rng();
	let range = max - min;
	let random_millis = rng.gen_range(0..range.as_millis() as u64);
	min + Duration::from_millis(random_millis)
}

type EstablishPayload = (
	Uuid,
	Sender<Vec<u8>>,
	Receiver<Vec<u8>>,
	JoinHandle<()>,
	JoinHandle<()>,
);
type EstablishError = Result<(), ()>;
type EstablishHandle = JoinHandle<Result<EstablishPayload, EstablishError>>;

async fn send_write(mut tx: SendStream, mut tx_rx: Receiver<Vec<u8>>) {
	loop {
		let mut big_buf = vec![];
		while let Ok(data) = tx_rx.try_recv() {
			let mut buf = (data.len() as u64).to_le_bytes().to_vec();
			buf.extend(data);
			big_buf.extend(buf);
			continue;
		}

		match tx.write_all(&big_buf).await {
			Ok(_) => {}
			Err(WriteError::Stopped(code)) => {
				trace!("stopped {code:?}");
				continue;
			}
			Err(e) => {
				trace!("err {e:?}");
				return;
			}
		};
		if let Err(e) = tx.flush().await {
			trace!("failed to flush {e:?}");
		}

		tokio::time::sleep(Duration::from_millis(50)).await;
	}
}

fn send(tx: SendStream) -> (JoinHandle<()>, Sender<Vec<u8>>) {
	let (tx_tx, tx_rx) = mpsc::channel(8192);

	let send_handle = tokio::spawn(async move {
		send_write(tx, tx_rx).await;
	});

	(send_handle, tx_tx)
}

async fn establish(
	id: Uuid,
	fut: impl Future<Output = Result<(SendStream, RecvStream), ConnectionError>> + Send + 'static,
) -> Result<EstablishPayload, EstablishError> {
	let r = fut.await;

	let Ok((tx, rx)) = r else { Err(Err(()))? };
	let (recv_handle, mut rx) = recv(rx);
	let (send_handle, tx) = send(tx);
	let Ok(_) = tx.send(id.as_bytes().to_vec()).await else {
		trace!("failed to write id");
		Err(Err(()))?
	};
	let Ok(id) = receive_id(&mut rx).await else {
		trace!("failed to receive id");
		return Err(Ok(()));
	};
	trace!("sending conn stream info");
	Ok((id, tx, rx, send_handle, recv_handle))
}

async fn receive_id(rx: &mut Receiver<Vec<u8>>) -> Result<Uuid, ()> {
	let Some(data) = rx.recv().await else {
		return Err(());
	};

	if data.len() != mem::size_of::<Uuid>() {
		return Err(());
	}

	let mut buf = [0u8; mem::size_of::<Uuid>()];

	for i in 0..mem::size_of::<Uuid>() {
		buf[i] = data[i];
	}

	Ok(Uuid::from_bytes(buf))
}

fn recv(rx: RecvStream) -> (JoinHandle<()>, Receiver<Vec<u8>>) {
	let (rx_tx, rx_rx) = mpsc::channel(8192);

	let recv_handle = tokio::spawn(async move {
		recv_read(rx, rx_tx).await;
	});

	(recv_handle, rx_rx)
}

async fn read_exact(
	rx: &mut RecvStream,
	buf: &mut [u8],
	storage: &mut Option<Vec<u8>>,
) -> Result<Result<(), ()>, ReadError> {
	let buf_len = buf.len();
	let mut cursor = 0;
	if let Some(previous) = storage.take() {
		buf[..previous.len()].copy_from_slice(&previous);
		cursor = previous.len();
	}
	match rx.read_exact(&mut buf[cursor..buf_len]).await {
		Ok(()) => Ok(Ok(())),
		Err(ReadExactError::FinishedEarly(bytes_read)) => {
			cursor += bytes_read;
			*storage = Some(buf[..cursor].to_vec());
			Ok(Err(()))
		}
		Err(ReadExactError::ReadError(e)) => Err(e),
	}
}

async fn recv_read(mut rx: RecvStream, rx_tx: mpsc::Sender<Vec<u8>>) {
	let mut buf = vec![0u8; 50_000_000];
	let buf_len = buf.len();
	let mut storage = None;

	loop {
		trace!("reading len...");
		if let Err(e) = read_exact_or_yield(&mut rx, &mut buf[..mem::size_of::<u64>()], &mut storage).await {
  				trace!("read_exact_or_yield failed {e:?}");
  				return;
  			}

		let mut len_bytes = [0u8; mem::size_of::<u64>()];
		len_bytes.copy_from_slice(&buf[..mem::size_of::<u64>()]);

		let len = u64::from_le_bytes(len_bytes) as usize;
		let mut remaining = len;
		let mut data = vec![];
		loop {
			trace!("reading data...");
			let bytes_read = remaining.min(buf_len);
			if let Err(e) = read_exact_or_yield(&mut rx, &mut buf[..bytes_read], &mut storage).await {
   					trace!("read_exact_or_yield failed {e:?}");
   					return;
   				}
			data.extend(&buf[..bytes_read]);
			remaining -= bytes_read;
			if remaining == 0 {
				break;
			} else {
				tokio::time::sleep(Duration::from_millis(10)).await;
			}
		}

		loop {
			match rx_tx.try_send(data) {
				Ok(_) => break,
				Err(TrySendError::Full(x)) => {
					tokio::time::sleep(Duration::from_millis(1)).await;
					trace!("channel was full");
					data = x
				}
				Err(TrySendError::Closed(_)) => {
					trace!("channel was closed");
					return;
				}
			}
		}

		tokio::time::sleep(Duration::from_millis(50)).await;
	}
}

async fn read_exact_or_yield(
	rx: &mut RecvStream,
	buf: &mut [u8],
	storage: &mut Option<Vec<u8>>,
) -> Result<(), ReadError> {
	loop {
		match read_exact(rx, buf, storage).await {
			Ok(Ok(_)) => return Ok(()),
			Ok(Err(_)) => {
				tokio::task::yield_now().await;
				continue;
			}
			Err(e) => {
				return Err(e);
			}
		}
	}
}

fn endpoint(listen: SocketAddr) -> quinn::Endpoint {
	let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();

	let key = rustls::pki_types::PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
		cert.key_pair.serialize_der(),
	));

	let crypto = rustls::ServerConfig::builder()
		.with_no_client_auth()
		.with_single_cert(vec![cert.cert.into()], key)
		.unwrap();

	let mut server_config =
		quinn::ServerConfig::with_crypto(Arc::new(QuicServerConfig::try_from(crypto).unwrap()));

	server_config.transport_config(Arc::new(transport_config()));

	let client_crypto = rustls::ClientConfig::builder()
		.dangerous()
		.with_custom_certificate_verifier(SkipServerVerification::new())
		.with_no_client_auth();

	let mut client_config =
		quinn::ClientConfig::new(Arc::new(QuicClientConfig::try_from(client_crypto).unwrap()));

	client_config.transport_config(Arc::new(transport_config()));

	let mut endpoint = quinn::Endpoint::server(server_config, listen).unwrap();

	endpoint.set_default_client_config(client_config);

	endpoint
}

fn transport_config() -> TransportConfig {
	let mut tc = TransportConfig::default();
	tc.max_concurrent_uni_streams(VarInt::from_u32(0));
	tc.max_concurrent_bidi_streams(VarInt::from_u32(64));
	tc.stream_receive_window(VarInt::from_u32(1e+7 as u32));
	tc
}

#[derive(Debug)]
struct SkipServerVerification(Arc<rustls::crypto::CryptoProvider>);

impl SkipServerVerification {
	fn new() -> Arc<Self> {
		Arc::new(Self(Arc::new(rustls::crypto::ring::default_provider())))
	}
}

impl rustls::client::danger::ServerCertVerifier for SkipServerVerification {
	fn verify_server_cert(
		&self,
		_end_entity: &CertificateDer<'_>,
		_intermediates: &[CertificateDer<'_>],
		_server_name: &ServerName<'_>,
		_ocsp: &[u8],
		_now: UnixTime,
	) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
		Ok(rustls::client::danger::ServerCertVerified::assertion())
	}

	fn verify_tls12_signature(
		&self,
		message: &[u8],
		cert: &CertificateDer<'_>,
		dss: &rustls::DigitallySignedStruct,
	) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
		rustls::crypto::verify_tls12_signature(
			message,
			cert,
			dss,
			&self.0.signature_verification_algorithms,
		)
	}

	fn verify_tls13_signature(
		&self,
		message: &[u8],
		cert: &CertificateDer<'_>,
		dss: &rustls::DigitallySignedStruct,
	) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
		rustls::crypto::verify_tls13_signature(
			message,
			cert,
			dss,
			&self.0.signature_verification_algorithms,
		)
	}

	fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
		self.0.signature_verification_algorithms.supported_schemes()
	}
}
