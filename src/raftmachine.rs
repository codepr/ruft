use futures::{future, prelude::*};
use rand::prelude::*;
use std::boxed::Box;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::{
    io,
    net::{IpAddr, SocketAddr},
};
use tarpc::{
    client, context,
    serde::{Deserialize, Serialize},
    server::{self, Channel, Incoming},
    tokio_serde::formats::Json,
};
use tokio::sync::Notify;
use tokio::time::{sleep, Duration, Instant};

#[derive(Clone, PartialEq)]
enum State {
    Follower,
    Candidate,
    Leader,
}

/// RequestVote call as described in Raft paper, it carries:
///
/// - the candidate's term
/// - the candidate's id
/// - the index of the candidate's last log entry
/// - the term of the candidate's last log entry
#[derive(Debug, Serialize, Deserialize)]
struct RequestVote {
    // Candidate's term
    term: i32,
    // Candidate's identifier
    candidate_id: String,
    // Index of the candidate's last log entry
    last_log_index: i32,
    // Term of the candidate's last log entry
    last_log_term: i32,
}

/// RequestVoteReply contains the current term for candidate to update, and a bool indication of
/// received vote
#[derive(Debug, Serialize, Deserialize)]
struct RequestVoteReply {
    // Current term for candidate to update itself
    pub term: i32,
    // True means candidate received vote
    pub vote_granted: bool,
}

/// AppendEntries call, contains the current term for the candidate and the Id of the leader node
#[derive(Debug, Serialize, Deserialize)]
struct AppendEntries {
    // Current term of the requesting node
    term: i32,
    // Leader ID
    leader_id: String,
}

/// AppendEntriesReply call, contains the current term for candidate and a bool indication of
/// received vote
#[derive(Debug, Serialize, Deserialize)]
struct AppendEntriesReply {
    // Current term for candidate to update itself
    term: i32,
    // True means candidate received vote
    success: bool,
}

#[tarpc::service]
pub trait Raft {
    async fn request_vote(r: RequestVote) -> RequestVoteReply;
    async fn append_entries(a: AppendEntries) -> AppendEntriesReply;
}

#[derive(Clone)]
struct LogEntry {
    // Current term of the command being issued
    term: i32,
    // The actual command to be applied to the state machine
    command: String,
}

/// The state machine to manage the state of a single node, can assume 3 different states:
///
/// - CANDIDATE
/// - LEADER
/// - FOLLOWER
#[derive(Clone)]
struct RaftMachine {
    // Identifier of the current node on the cluster
    id: String,
    // Latest term server has seen, initialized to 0 on first boot, increases
    // monotonically
    current_term: i32,
    // Candidate Id that received vote in the current term or nil if none
    voted_for: String,
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
    // Cluster reference to all nodes
    cluster: Arc<Mutex<RaftCluster>>,
}

#[tarpc::server]
impl Raft for RaftMachine {
    async fn request_vote(mut self, _: context::Context, r: RequestVote) -> RequestVoteReply {
        let mut re = RequestVoteReply {
            term: 0,
            vote_granted: false,
        };
        if r.term > self.current_term {
            self.state = State::Follower;
            self.current_term = r.term;
            self.voted_for = "".into();
        }
        if self.current_term == r.term
            && (self.voted_for.is_empty() || self.voted_for == r.candidate_id)
        {
            self.voted_for = r.candidate_id.clone();
            re.vote_granted = true;
        }
        re.term = self.current_term;
        return re;
    }

    async fn append_entries(self, _: context::Context, a: AppendEntries) -> AppendEntriesReply {
        AppendEntriesReply {
            term: 0,
            success: false,
        }
    }
}

impl RaftMachine {
    pub fn new(cluster: Arc<Mutex<RaftCluster>>, addr: String) -> Self {
        Self {
            id: addr.clone(),
            current_term: 0,
            voted_for: "".into(),
            log: Vec::new(),
            commit_index: 0,
            last_applied: 0,
            state: State::Follower,
            election_timeout: rand::thread_rng().gen_range(150..300),
            latest_heartbeat: 0,
            cluster: cluster,
        }
    }

    async fn start_election_timer(&self) {
        println!("Election");
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
            return;
        }
        // Start the election, random initialization of election timeout should
        // guarantee that one node will send his RequestVoteRPC before any other
        if now.elapsed() > Duration::from_millis(self.election_timeout) {
            println!("Start election");
        }
    }

    async fn start_election(&mut self) -> io::Result<()> {
        self.state = State::Candidate;
        self.current_term += 1;
        self.voted_for = self.id.clone();
        let cluster = self.cluster.lock().unwrap();
        for client in cluster.nodes.values() {
            let rv = RequestVote {
                term: self.current_term,
                candidate_id: self.id.clone(),
                last_log_term: 0,
                last_log_index: 0,
            };
            let reply = client.request_vote(context::current(), rv).await?;
            if self.state != State::Candidate {
                return Ok(());
            }
            if reply.term > self.current_term {
                self.state = State::Follower;
                self.current_term = reply.term;
                self.voted_for = "".into();
                break;
            } else if reply.term == self.current_term {
                if reply.vote_granted {
                    // Check for leader quorum here
                    self.state = State::Leader;
                    println!("Become leader");
                    return Ok(());
                }
            }
        }
        self.start_election_timer().await;
        Ok(())
    }

    async fn become_follower(&mut self, term: i32) -> io::Result<()> {
        self.state = State::Follower;
        self.current_term = term;
        self.voted_for = "".into();
        println!("Become follower");
        Ok(())
    }
}

pub struct RaftCluster {
    pub nodes: HashMap<String, RaftClient>,
    notify: Arc<Notify>,
    connection_retries: i32,
}

impl RaftCluster {
    pub fn new(notify: Arc<Notify>) -> Self {
        Self {
            nodes: HashMap::new(),
            notify: notify,
            connection_retries: 3,
        }
    }

    pub async fn connect(&mut self, addr: &str) -> io::Result<()> {
        let mut transport = tarpc::serde_transport::tcp::connect(addr, Json::default);
        transport.config_mut().max_frame_length(usize::MAX);

        // RaftClient is generated by the service attribute. It has a constructor `new` that takes a
        // config and any Transport as input.
        let client = RaftClient::new(client::Config::default(), transport.await?).spawn()?;
        self.nodes.insert(addr.into(), client);
        Ok(())
    }

    pub async fn start(&mut self, peers: Vec<String>) -> io::Result<()> {
        for peer in peers.iter() {
            for i in 0..self.connection_retries {
                match self.connect(peer).await {
                    Ok(_) => break,
                    Err(_) => println!("Re-try"),
                }
            }
        }
        self.notify.notify_one();
        Ok(())
    }
}

pub async fn run(
    cluster: Arc<Mutex<RaftCluster>>,
    notify: Arc<Notify>,
    addr: &str,
) -> io::Result<()> {
    let mut listener = tarpc::serde_transport::tcp::listen(addr, Json::default).await?;
    listener.config_mut().max_frame_length(usize::MAX);
    // let server = server::BaseChannel::with_defaults(listener);
    listener
        // Ignore accept errors.
        .filter_map(|r| future::ready(r.ok()))
        .map(server::BaseChannel::with_defaults)
        // Limit channels to 1 per IP.
        .max_channels_per_key(1, |t| t.as_ref().peer_addr().unwrap().ip())
        // serve is generated by the service attribute. It takes as input any type implementing
        // the generated World trait.
        .map(|channel| {
            let c = cluster.clone();
            let server = Box::new(RaftMachine::new(c, addr.into()));
            let notify = notify.clone();
            let s = server.clone();
            tokio::spawn(async move {
                notify.notified().await;
                server.start_election_timer().await;
            });
            channel.requests().execute(s.serve())
        })
        // Max 10 channels.
        .buffer_unordered(10)
        .for_each(|_| async {})
        .await;
    Ok(())
}
