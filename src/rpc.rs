use crate::AsyncResult;
use bytes::{BufMut, BytesMut};
use futures::SinkExt;
use log::{error, info};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::result::Result;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};
use tokio_stream::StreamExt;
use tokio_util::codec::{Decoder, Encoder, Framed};

const BACKOFF: u64 = 128;

mod raft {
    use log::info;
    use rand::prelude::*;
    use tokio::time::Instant;

    #[derive(Clone, PartialEq)]
    pub enum State {
        Follower,
        Candidate,
        Leader,
    }

    struct LogEntry {
        // Current term of the command being issued
        term: i32,
        // The actual command to be applied to the state machine
        command: String,
    }

    pub struct Machine {
        // Identifier of the current node on the cluster
        pub id: String,
        // Latest term server has seen, initialized to 0 on first boot, increases
        // monotonically
        pub current_term: i32,
        // Candidate Id that received vote in the current term or nil if none
        pub voted_for: Option<String>,
        // Log entries; each entry contains command for state machine, and term
        // when entry was received by leader, first index is 1
        log: Vec<LogEntry>,
        // Index of the highest log entry known to be commited, initialized to 0,
        // increases monotonically
        pub commit_index: i32,
        // Index of the highest log entry applied to state machine, initialized to
        // 0, increases monotonically
        pub last_applied: i32,
        // State of the machine, can be either CANDIDATE, FOLLOWER or LEADER
        pub state: State,
        // Election timer timeout
        pub election_timeout: u64,
        // Last received heartbeat from leader
        pub latest_heartbeat: Instant,
    }

    impl Machine {
        pub fn new(id: String) -> Self {
            Self {
                id: id.clone(),
                current_term: 0,
                voted_for: None,
                log: Vec::new(),
                commit_index: 0,
                last_applied: 0,
                state: State::Follower,
                election_timeout: rand::thread_rng().gen_range(150..300),
                latest_heartbeat: Instant::now(),
            }
        }

        pub fn update_latest_heartbeat(&mut self) {
            self.latest_heartbeat = Instant::now();
        }

        pub fn become_leader(&mut self) {
            self.state = State::Leader;
        }

        pub fn become_follower(&mut self, term: i32) {
            self.state = State::Follower;
            self.current_term = term;
            self.voted_for = None;
            info!("{} become follower", self.id);
        }
    }
}

type SharedMachine = Arc<Mutex<raft::Machine>>;

#[derive(Debug)]
struct RpcError(String);

impl fmt::Display for RpcError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "RPC error: {}", self.0)
    }
}

/// RequestVote call as described in Raft paper, it carries:
///
/// - the candidate's term
/// - the candidate's id
/// - the index of the candidate's last log entry
/// - the term of the candidate's last log entry
pub struct RequestVote {
    // Candidate's term
    pub term: i32,
    // Candidate's identifier
    pub candidate_id: String,
    // Index of the candidate's last log entry
    pub last_log_index: i32,
    // Term of the candidate's last log entry
    pub last_log_term: i32,
}

/// RequestVoteReply contains the current term for candidate to update, and a bool indication of
/// received vote
pub struct RequestVoteReply {
    // Current term for candidate to update itself
    pub term: i32,
    // True means candidate received vote
    pub vote_granted: bool,
}

/// AppendEntries call, contains the current term for the candidate and the Id of the leader node
pub struct AppendEntries {
    // Current term of the requesting node
    pub term: i32,
    // Leader ID
    pub leader_id: String,
}

/// AppendEntriesReply call, contains the current term for candidate and a bool indication of
/// received vote
pub struct AppendEntriesReply {
    // Current term for candidate to update itself
    pub term: i32,
    // True means candidate received vote
    pub success: bool,
}

#[derive(Deserialize, Serialize)]
pub enum RpcMessage {
    RequestVote(i32, String, i32, i32),
    RequestVoteReply(i32, bool),
    AppendEntries(i32, String),
    AppendEntriesReply(i32, bool),
}

struct RpcServer {
    listener: TcpListener,
    /// Tcp exponential backoff threshold
    backoff: u64,
    machine: SharedMachine,
}

pub struct RpcClient {
    peers: HashMap<String, Framed<TcpStream, BinCodec<RpcMessage>>>,
}

impl RpcClient {
    pub fn new() -> Self {
        Self {
            peers: HashMap::new(),
        }
    }

    pub fn peers_number(&self) -> usize {
        self.peers.len()
    }

    pub async fn connect(&mut self, addr: &String) -> AsyncResult<()> {
        let stream = TcpStream::connect(addr.clone()).await?;
        let bin_codec: BinCodec<RpcMessage> = BinCodec::new();
        let framed_stream = Framed::new(stream, bin_codec);
        self.peers.insert(addr.clone(), framed_stream);
        Ok(())
    }

    pub async fn request_vote(&mut self, r: RequestVote) -> AsyncResult<Vec<RequestVoteReply>> {
        let mut responses: Vec<RpcMessage> = Vec::new();

        for stream in self.peers.values_mut() {
            let rv = RpcMessage::RequestVote(
                r.term,
                r.candidate_id.clone(),
                r.last_log_term,
                r.last_log_index,
            );
            stream.send(rv).await?;
            if let Some(reply) = stream.next().await {
                responses.push(reply?);
            }
        }

        let mut response = Vec::new();
        for resp in responses {
            if let RpcMessage::RequestVoteReply(term, vote_granted) = resp {
                response.push(RequestVoteReply {
                    term: term,
                    vote_granted: vote_granted,
                });
            }
        }
        Ok(response)
    }

    pub async fn send_heartbeat(&mut self, r: AppendEntries) -> AsyncResult<(i32, bool)> {
        for stream in self.peers.values_mut() {
            let append_entries = RpcMessage::AppendEntries(r.term, r.leader_id.clone());
            stream.send(append_entries).await?;
            if let Some(reply) = stream.next().await {
                match reply {
                    Ok(re) => {
                        if let RpcMessage::AppendEntriesReply(term, success) = re {
                            if term > r.term {
                                return Ok((term, true));
                            }
                        }
                    }
                    Err(e) => error!("{}", e),
                }
            };
        }
        Ok((r.term, false))
    }

    pub async fn request_vote_reply(&mut self, peer: &str, r: RequestVoteReply) -> AsyncResult<()> {
        let stream = self.peers.get_mut(peer).unwrap();
        let rvp = RpcMessage::RequestVoteReply(r.term, r.vote_granted);
        stream.send(rvp).await?;
        Ok(())
    }

    pub async fn append_entries_reply(
        &mut self,
        peer: &str,
        r: AppendEntriesReply,
    ) -> AsyncResult<()> {
        let stream = self.peers.get_mut(peer).unwrap();
        let reply = RpcMessage::AppendEntriesReply(r.term, r.success);
        stream.send(reply).await?;
        Ok(())
    }
}

impl RpcServer {
    /// Create a new Server and run.
    ///
    /// Listen for inbound connections. For each inbound connection, spawn a
    /// task to process that connection.
    ///
    /// # Errors
    ///
    /// Returns `Err` if accepting returns an error. This can happen for a
    /// number reasons that resolve over time. For example, if the underlying
    /// operating system has reached an internal limit for max number of
    /// sockets, accept will fail.
    pub async fn run<'a>(&mut self) -> AsyncResult<()> {
        // Loop forever on new connections, accept them and pass the handling
        // to a worker.
        loop {
            // Accepts a new connection, obtaining a valid socket.
            let (stream, peer) = self.accept().await?;
            info!("connection from {}", peer.to_string());
            // Create a clone reference of the shared raft state to be used by this connection.
            let machine = self.machine.clone();
            // Spawn a new task to process the connection, moving the ownership of the cloned
            // db into the async closure.
            tokio::spawn(async move {
                // Create a binary codec for `RpcMessage` type, this way it's possible leverage
                // the `Framed` struct of `tokio_util` crate to handle the serialization
                let bin_codec: BinCodec<RpcMessage> = BinCodec::new();
                let mut messages = Framed::new(stream, bin_codec);
                while let Some(msg) = messages.next().await {
                    match msg {
                        Ok(m) => {
                            if let Some(response) = handle_request(&machine, m).await {
                                if let Err(e) = messages.send(response).await {
                                    error!("error sending response: {:?}", e);
                                }
                            }
                        }
                        Err(e) => {
                            error!("error on deconding from stream: {:?}", e);
                        }
                    }
                }
                info!("{} disconnected", peer.to_string());
            });
        }
    }

    /// Accept an inbound connection.
    ///
    /// Errors are handled by backing off and retrying. An exponential backoff
    /// strategy is used. After the first failure, the task waits for 1 second.
    /// After the second failure, the task waits for 2 seconds. Each subsequent
    /// failure doubles the wait time. If accepting fails on the 6th try after
    /// waiting for 64 seconds, then this function returns with an error.
    async fn accept(&mut self) -> AsyncResult<(TcpStream, SocketAddr)> {
        let mut backoff = 1;

        // Try to accept a few times
        loop {
            // Perform the accept operation. If a socket is successfully
            // accepted, return it. Otherwise, save the error.
            match self.listener.accept().await {
                Ok((socket, peer)) => return Ok((socket, peer)),
                Err(err) => {
                    if backoff > self.backoff {
                        // Accept has failed too many times. Return the error.
                        return Err(err.into());
                    }
                }
            }

            // Pause execution until the back off period elapses.
            sleep(Duration::from_secs(backoff)).await;

            // Double the back off
            backoff *= 2;
        }
    }
}

async fn handle_request(machine: &SharedMachine, msg: RpcMessage) -> Option<RpcMessage> {
    let mut shared = machine.lock().await;
    match msg {
        RpcMessage::RequestVote(term, id, last_id, last_log) => {
            let candidate_id = Some(id.clone());
            if term > shared.current_term {
                shared.become_follower(term);
            }
            let vote_granted = if shared.current_term == term
                && (shared.voted_for.is_none() || shared.voted_for == candidate_id)
            {
                shared.voted_for = candidate_id;
                true
            } else {
                false
            };
            Some(RpcMessage::RequestVoteReply(
                shared.current_term,
                vote_granted,
            ))
        }
        RpcMessage::AppendEntries(term, id) => {
            let success = shared.current_term == term;
            shared.update_latest_heartbeat();
            if shared.current_term < term || (success && shared.state != raft::State::Follower) {
                shared.become_follower(term);
            }
            Some(RpcMessage::AppendEntriesReply(shared.current_term, success))
        }
        _ => None,
    }
}

struct BinCodec<T> {
    phantom_data: PhantomData<T>,
}

impl<T> BinCodec<T> {
    pub fn new() -> Self {
        BinCodec {
            phantom_data: PhantomData,
        }
    }
}

impl<T> Decoder for BinCodec<T>
where
    for<'de> T: Deserialize<'de> + Serialize,
{
    type Item = T;
    type Error = bincode::Error;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if buf.len() == 0 {
            return Ok(None);
        }
        let object: T = bincode::deserialize(&buf[..])?;
        let offset = bincode::serialized_size(&object)?;
        let _ = buf.split_to(offset as usize);
        Ok(Some(object))
    }
}

impl<T> Encoder<T> for BinCodec<T>
where
    T: Serialize,
{
    type Error = bincode::Error;

    fn encode(&mut self, object: T, buf: &mut BytesMut) -> Result<(), Self::Error> {
        let size = bincode::serialized_size(&object)?;
        buf.reserve(size as usize);
        let bytes = bincode::serialize(&object)?;
        buf.put_slice(&bytes[..]);
        Ok(())
    }
}

impl<T> fmt::Debug for BinCodec<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("BinCodec").finish()
    }
}

/// Run a tokio async server, init the shared filters database and accepts and handle new
/// connections asynchronously.
///
/// Requires single, already bound `TcpListener` argument
pub async fn run(id: String, listener: TcpListener, peers: Vec<String>) -> AsyncResult<()> {
    let mut client = RpcClient::new();
    for peer in peers.iter() {
        client.connect(peer).await?;
    }
    let shared = Arc::new(Mutex::new(raft::Machine::new(id)));
    let cloned_machine = shared.clone();
    tokio::spawn(async move {
        if let Err(e) = start_election_timer(&cloned_machine, &Arc::new(Mutex::new(client))).await {
            error!("can't spawn `start_election_timer` worker: {:?}", e);
        }
    });
    info!("listening on {}", listener.local_addr()?);
    let mut server = RpcServer {
        listener,
        backoff: BACKOFF,
        machine: shared,
    };
    server.run().await
}

async fn start_election_timer(
    shared: &SharedMachine,
    client: &Arc<Mutex<RpcClient>>,
) -> AsyncResult<()> {
    let new_election = {
        let machine = shared.lock().await;
        let term_started = machine.current_term;
        let election_timeout = machine.election_timeout;
        let timeout = Duration::from_millis(election_timeout);
        // Pause execution until the back off period elapses.
        sleep(timeout).await;
        // Skip election if already a Leader or if the term has already been changed
        // by another RequestVoteRPC from another Candidate
        if machine.state == raft::State::Leader || term_started != machine.current_term {
            return Ok(());
        }

        // Start the election, random initialization of election timeout should
        // guarantee that one node will send his RequestVoteRPC before any other
        machine.latest_heartbeat.elapsed() > Duration::from_millis(election_timeout)
    };
    if new_election {
        start_election(&shared.clone(), &client.clone()).await?;
    }
    loop {
        sleep(Duration::from_millis(50)).await;
        let mut machine = shared.lock().await;
        if machine.state != raft::State::Leader {
            continue;
        }
        let append_entries = AppendEntries {
            term: machine.current_term,
            leader_id: machine.id.clone(),
        };
        info!("{} sending heartbeats", machine.id);
        let (term, resign) = client.lock().await.send_heartbeat(append_entries).await?;
        if resign {
            machine.become_follower(term);
        }
    }
}

async fn start_election(shared: &SharedMachine, client: &Arc<Mutex<RpcClient>>) -> AsyncResult<()> {
    let rv = {
        let mut machine = shared.lock().await;
        info!(
            "{} started election on term {}",
            machine.id, machine.current_term
        );
        machine.state = raft::State::Candidate;
        machine.current_term += 1;
        machine.voted_for = Some(machine.id.clone());
        RequestVote {
            term: machine.current_term,
            candidate_id: machine.id.clone(),
            last_log_term: 0,
            last_log_index: 0,
        }
    };
    let replies = client.lock().await.request_vote(rv).await?;
    {
        let mut machine = shared.lock().await;
        if machine.state != raft::State::Candidate {
            return Ok(());
        }

        if has_quorum(replies, client.lock().await.peers_number())
            && machine.state == raft::State::Candidate
        {
            info!(
                "{} has won the election on term {}",
                machine.id, machine.current_term,
            );
            machine.become_leader();
            info!(
                "{} become leader on term {}",
                machine.id, machine.current_term
            );
        }
    }
    Ok(())
}

fn has_quorum(response: Vec<RequestVoteReply>, peers_number: usize) -> bool {
    let number_of_servers = peers_number + 1; // All peers + current server
    let votes = response.iter().filter(|r| r.vote_granted).count();
    let quorum = (number_of_servers as f64 / 2 as f64).floor();
    (votes + 1) > quorum as usize
}
