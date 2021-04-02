use crate::AsyncResult;
use bytes::{BufMut, BytesMut};
use futures::SinkExt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::marker::PhantomData;
use std::result::Result;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};
use tokio_stream::StreamExt;
use tokio_util::codec::{Decoder, Encoder, Framed};

const BACKOFF: u64 = 128;

mod RaftState {
    use super::{RequestVote, RequestVoteReply, RpcClient};
    use crate::AsyncResult;
    // use math::round;
    use rand::prelude::*;
    use std::io;
    use tokio::time::{sleep, Duration, Instant};

    #[derive(Clone, PartialEq)]
    enum State {
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

    pub struct RaftMachine {
        // Identifier of the current node on the cluster
        id: String,
        // Latest term server has seen, initialized to 0 on first boot, increases
        // monotonically
        current_term: i32,
        // Candidate Id that received vote in the current term or nil if none
        voted_for: Option<String>,
        // Log entries; each entry contains command for state machine, and term
        // when entry was received by leader, first index is 1
        log: Vec<LogEntry>,
        // Index of the highest log entry known to be commited, initialized to 0,
        // increases monotonically
        commit_index: i32,
        // Index of the highest log entry applied to state machine, initialized to
        // 0, increases monotonically
        last_applied: i32,
        // State of the machine, can be either CANDIDATE, FOLLOWER or LEADER
        state: State,
        // Election timer timeout
        election_timeout: u64,
        // Last received heartbeat from leader
        latest_heartbeat: u64,
        // rpc client
        client: RpcClient,
    }

    impl RaftMachine {
        pub fn new(id: String, client: RpcClient) -> Self {
            Self {
                id: id.clone(),
                current_term: 0,
                voted_for: None,
                log: Vec::new(),
                commit_index: 0,
                last_applied: 0,
                state: State::Follower,
                election_timeout: rand::thread_rng().gen_range(150..300),
                latest_heartbeat: 0,
                client: client,
            }
        }

        pub async fn start_election_timer(&mut self) -> AsyncResult<()> {
            let term_started = self.current_term;
            let timeout = Duration::from_millis(self.election_timeout);
            let now = Instant::now();
            // Pause execution until the back off period elapses.
            sleep(timeout).await;
            // Skip election if already a Leader or if the term has already been changed
            // by another RequestVoteRPC from another Candidate
            if (self.state != State::Candidate && self.state != State::Follower)
                || term_started != self.current_term
            {
                return Ok(());
            }
            // Start the election, random initialization of election timeout should
            // guarantee that one node will send his RequestVoteRPC before any other
            if now.elapsed() > Duration::from_millis(self.election_timeout) {
                self.start_election().await?;
            }
            Ok(())
        }

        async fn start_election(&mut self) -> AsyncResult<()> {
            self.state = State::Candidate;
            self.current_term += 1;
            self.voted_for = Some(self.id.clone());
            let rv = RequestVote {
                term: self.current_term,
                candidate_id: self.id.clone(),
                last_log_term: 0,
                last_log_index: 0,
            };
            let replies = self.client.request_vote(rv).await?;
            if self.state != State::Candidate {
                return Ok(());
            }

            if self.has_quorum(replies) {
                println!("Become leader");
            }

            Ok(())
        }

        async fn become_follower(&mut self, term: i32) -> io::Result<()> {
            self.state = State::Follower;
            self.current_term = term;
            self.voted_for = None;
            println!("Become follower");
            Ok(())
        }

        fn has_quorum(&self, response: Vec<RequestVoteReply>) -> bool {
            let number_of_servers = self.client.peers_number() + 1; // All peers + current server
            let votes = response.iter().filter(|r| r.vote_granted).count();
            let quorum = (number_of_servers as f64 / 2 as f64).floor();
            (votes + 1) > quorum as usize && State::Candidate == self.state
        }
    }
}

type SharedMachine = Arc<Mutex<RaftState::RaftMachine>>;

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
enum RpcMessage {
    RequestVote(i32, String, i32, i32),
    RequestVoteReply(i32, bool),
    AppendEntries(i32, String),
    AppendEntriesReply(i32, bool),
}

struct RpcServer {
    machine: SharedMachine,
    listener: TcpListener,
    /// Tcp exponential backoff threshold
    backoff: u64,
}

pub struct RpcClient {
    peers: HashMap<String, TcpStream>,
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
        self.peers.insert(addr.clone(), stream);
        Ok(())
    }

    pub async fn request_vote(&mut self, r: RequestVote) -> AsyncResult<Vec<RequestVoteReply>> {
        let rv = RpcMessage::RequestVote(r.term, r.candidate_id, r.last_log_term, r.last_log_index);
        let serialized = bincode::serialize(&rv).unwrap();
        let mut rpc_responses: Vec<RpcMessage> = Vec::new();

        for stream in self.peers.values_mut() {
            stream.write(&serialized).await?;

            let mut buffer = [0; 256];
            stream.read(&mut buffer).await?;
            rpc_responses.push(bincode::deserialize(&buffer).unwrap());
        }

        let mut response = Vec::new();
        for rpc_resp in rpc_responses {
            if let RpcMessage::RequestVoteReply(term, vote_granted) = rpc_resp {
                response.push(RequestVoteReply {
                    term: term,
                    vote_granted: vote_granted,
                });
            }
        }

        Ok(response)
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
    pub async fn run(&mut self) -> AsyncResult<()> {
        // Loop forever on new connections, accept them and pass the handling
        // to a worker.
        loop {
            // Accepts a new connection, obtaining a valid socket.
            let stream = self.accept().await?;
            println!("New connection accepted");
            // Create a clone reference of the filters database to be used by this connection.
            let machine = self.machine.clone();
            // Spawn a new task to process the connection, moving the ownership of the cloned
            // db into the async closure.
            tokio::spawn(async move {
                let bin_codec: BinCodec<RpcMessage> = BinCodec::new();
                let mut messages = Framed::new(stream, bin_codec);
                while let Some(msg) = messages.next().await {
                    match msg {
                        Ok(m) => {
                            if let Some(response) = handle_request(&m, &machine) {
                                if let Err(e) = messages.send(response).await {
                                    println!("error sending response: {:?}", e);
                                }
                            }
                        }
                        Err(e) => {
                            println!("error on deconding from stream: {:?}", e);
                        }
                    }
                }
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
    async fn accept(&mut self) -> AsyncResult<TcpStream> {
        let mut backoff = 1;

        // Try to accept a few times
        loop {
            // Perform the accept operation. If a socket is successfully
            // accepted, return it. Otherwise, save the error.
            match self.listener.accept().await {
                Ok((socket, _)) => return Ok(socket),
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

fn handle_request(msg: &RpcMessage, machine: &SharedMachine) -> Option<RpcMessage> {
    match msg {
        RpcMessage::RequestVote(term, id, last_id, last_log) => {
            println!("RequestVote received");
            Some(RpcMessage::RequestVoteReply(*term, true))
        }
        RpcMessage::AppendEntries(term, id) => {
            println!("AppendEntries received");
            Some(RpcMessage::AppendEntriesReply(*term, true))
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
        buf.split_to(offset as usize);
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
pub async fn run(listener: TcpListener, peers: Vec<String>) -> AsyncResult<()> {
    let mut client = RpcClient::new();
    for peer in peers.iter() {
        client.connect(peer).await?;
    }
    let shared = Arc::new(Mutex::new(RaftState::RaftMachine::new(
        "test".into(),
        client,
    )));
    let cloned_machine = shared.clone();
    tokio::spawn(async move {
        let mut m = cloned_machine.lock().await;
        if let Err(e) = m.start_election_timer().await {
            println!("Can't spawn `start_election_timer` worker: {:?}", e);
        }
    });
    let mut server = RpcServer {
        listener,
        backoff: BACKOFF,
        machine: shared,
    };
    server.run().await
}
