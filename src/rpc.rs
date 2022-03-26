//! ChiselStore RPC module.

use crate::rpc::proto::rpc_server::Rpc;
use crate::{StoreCommand, StoreServer, StoreTransport};
use async_mutex::Mutex;
use async_trait::async_trait;
use crossbeam::queue::ArrayQueue;
use derivative::Derivative;
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// importing omnipaxos core messages
use omnipaxos_core::messages::{
    AcceptDecide, AcceptStopSign, AcceptSync, Accepted, 
    AcceptedStopSign, Decide, DecideStopSign, FirstAccept, 
    Message, Prepare, Promise, Compaction, PaxosMsg
};
use omnipaxos_core::ballot_leader_election::messages::{
    BLEMessage, HeartbeatMsg, 
    HeartbeatRequest, HeartbeatReply
};
use omnipaxos_core::ballot_leader_election::Ballot;
use omnipaxos_core::util::SyncItem;
use omnipaxos_core::storage::{SnapshotType, StopSign};
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// importing proto messages and service interface
#[allow(missing_docs)]
pub mod proto {
    tonic::include_proto!("proto");
}
use proto::rpc_client::RpcClient;
use crate::rpc::proto::*; // messages

// standard package imports
use std::collections::HashMap;
use std::sync::Arc;
use std::marker::PhantomData;
use tonic::{Request, Response, Status};

type NodeAddrFn = dyn Fn(usize) -> String + Send + Sync;

#[derive(Debug)]
struct ConnectionPool {
    connections: ArrayQueue<RpcClient<tonic::transport::Channel>>,
}

struct Connection {
    conn: RpcClient<tonic::transport::Channel>,
    pool: Arc<ConnectionPool>,
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.pool.replenish(self.conn.clone())
    }
}

impl ConnectionPool {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            connections: ArrayQueue::new(16),
        })
    }

    async fn connection<S: ToString>(&self, addr: S) -> RpcClient<tonic::transport::Channel> {
        let addr = addr.to_string();
        match self.connections.pop() {
            Some(x) => x,
            None => RpcClient::connect(addr).await.unwrap(),
        }
    }

    fn replenish(&self, conn: RpcClient<tonic::transport::Channel>) {
        let _ = self.connections.push(conn);
    }
}

#[derive(Debug, Clone)]
struct Connections(Arc<Mutex<HashMap<String, Arc<ConnectionPool>>>>);

impl Connections {
    fn new() -> Self {
        Self(Arc::new(Mutex::new(HashMap::new())))
    }

    async fn connection<S: ToString>(&self, addr: S) -> Connection {
        let mut conns = self.0.lock().await;
        let addr = addr.to_string();
        let pool = conns
            .entry(addr.clone())
            .or_insert_with(ConnectionPool::new);
        Connection {
            conn: pool.connection(addr).await,
            pool: pool.clone(),
        }
    }
}

/// RPC transport.
#[derive(Derivative)]
#[derivative(Debug)]
pub struct RpcTransport {
    /// Node address mapping function.
    #[derivative(Debug = "ignore")]
    node_addr: Box<NodeAddrFn>,
    connections: Connections,
}

impl RpcTransport {
    /// Creates a new RPC transport.
    pub fn new(node_addr: Box<NodeAddrFn>) -> Self {
        RpcTransport {
            node_addr,
            connections: Connections::new(),
        }
    }
}

/** Implements the methods of StoreTransport for RpcTransport, let the 
* server send messages to the replica instance with id 'id', by creating 
* a connection to the destination and sending the message using async 
* transport channels
*/

#[async_trait]
impl StoreTransport for RpcTransport {

    /// Converts a paxos message into the correct Rpc type an sends it to the destination replica
    fn send_seqpaxos(&self, to_id: u64, msg: Message<StoreCommand, ()>) {
        let message = sp_message_to_rpc(msg.clone());
        let peer = (self.node_addr)(to_id as usize);
        let pool = self.connections.clone();
        tokio::task::spawn(async move {
            let mut client = pool.connection(peer).await;
            let request = tonic::Request::new(message);
            client.conn.handle_sp_message(request).await.unwrap();
        });
    }

    /// Converts a BLE message into the correct Rpc type an sends it to the destination replica
    fn send_ble(&self, to_id: u64, msg: BLEMessage) {
        let message = ble_message_to_rpc(msg.clone());
        let peer = (self.node_addr)(to_id as usize);
        let pool = self.connections.clone();
        tokio::task::spawn(async move {
            let mut client = pool.connection(peer).await;
            let request = tonic::Request::new(message);
            client.conn.handle_ble_message(request).await.unwrap();
        });
    }
    
}

/// RPC service.
#[derive(Debug)]
pub struct RpcService {
    /// The ChiselStore server access via this RPC service.
    pub server: Arc<StoreServer<RpcTransport>>,
}

impl RpcService {
    /// Creates a new RPC service.
    pub fn new(server: Arc<StoreServer<RpcTransport>>) -> Self {
        Self { server }
    }
}

/** Implements the rpc service interface that was generated in proto.rs. 
 * Through the implemented methods, clients can execute queries on the 
 * SQLite system and other replicas can send paxos and BLE messages to 
 * this server
*/
#[tonic::async_trait]
impl Rpc for RpcService {

    /// Executes an query and returns the result to client
    async fn execute(
        &self,
        request: Request<Query>,
    ) -> Result<Response<QueryResults>, tonic::Status> {
        let query = request.into_inner();
        let server = self.server.clone();
        let results = match server.query(query.sql).await {
            Ok(results) => results,
            Err(e) => return Err(Status::internal(format!("{}", e))),
        };
        let mut rows = vec![];
        for row in results.rows {
            rows.push(QueryRow {
                values: row.values.clone(),
            })
        }
        Ok(Response::new(QueryResults { rows }))
    }

    // handle sequence paxos message
    async fn handle_sp_message(&self, request: Request<RpcMessage>) -> Result<Response<Void>, tonic::Status> {
        let message = sp_message_from_rpc(request.into_inner());
        let server = self.server.clone();
        server.handle_sp_msg(message);
        Ok(Response::new(Void {}))
    }

    // handle ballot leader election message
    async fn handle_ble_message(&self, request: Request<RpcBlemessage>) -> Result<Response<Void>, tonic::Status> {
        let message = ble_message_from_rpc(request.into_inner());
        let server = self.server.clone();
        server.handle_ble_msg(message);
        Ok(Response::new(Void {}))
    }

}

/// Transform sp messages to RPC format
fn sp_message_to_rpc(message: Message<StoreCommand, ()>) -> RpcMessage {
    RpcMessage {
        from: message.from,
        to: message.to,
        msg: Some(match message.msg {
            PaxosMsg::PrepareReq => rpc_message::Msg::PrepareReq(RpcPrepareRequest {}),
            PaxosMsg::Prepare(prepare) => rpc_message::Msg::Prepare(prepare_rpc(prepare)),
            PaxosMsg::Promise(promise) => rpc_message::Msg::Promise(promise_rpc(promise)),
            PaxosMsg::AcceptSync(accept_sync) => rpc_message::Msg::Acceptsync(accept_sync_rpc(accept_sync)),
            PaxosMsg::FirstAccept(first_accept) => rpc_message::Msg::Firstaccept(first_accept_rpc(first_accept)),
            PaxosMsg::AcceptDecide(accept_decide) => rpc_message::Msg::Acceptdecide(accept_decide_rpc(accept_decide)),
            PaxosMsg::Accepted(accepted) => rpc_message::Msg::Accepted(accepted_rpc(accepted)),
            PaxosMsg::Decide(decide) => rpc_message::Msg::Decide(decide_rpc(decide)),
            PaxosMsg::ProposalForward(proposals) => rpc_message::Msg::Proposalforward(proposal_forward_rpc(proposals)),
            PaxosMsg::Compaction(compaction) => rpc_message::Msg::Compaction(compaction_rpc(compaction)),
            PaxosMsg::ForwardCompaction(compaction) => rpc_message::Msg::ForwardCompaction(compaction_rpc(compaction)),
            PaxosMsg::AcceptStopSign(accept_stop_sign) => rpc_message::Msg::AcceptsSs(accept_stop_sign_rpc(accept_stop_sign)),
            PaxosMsg::AcceptedStopSign(accepted_stop_sign) => rpc_message::Msg::AcceptedSs(accepted_stop_sign_rpc(accepted_stop_sign)),
            PaxosMsg::DecideStopSign(decide_stop_sign) => rpc_message::Msg::DecidedSs(decide_stop_sign_rpc(decide_stop_sign)),
        })
    }
}

/// Transform ble messages to RPC format
fn ble_message_to_rpc(message: BLEMessage) -> RpcBlemessage {
    RpcBlemessage {
        from: message.from,
        to: message.to,
        msg: Some(match message.msg {
            HeartbeatMsg::Request(request) => rpc_blemessage::Msg::Heartbeatrequest(heartbeat_request_rpc(request)),
            HeartbeatMsg::Reply(reply) => rpc_blemessage::Msg::Heartbeatreply(heartbeat_reply_rpc(reply)),
        })
    }
}

// BALLOT
fn ballot_rpc(ballot: Ballot) -> RpcBallot {
    RpcBallot {
        n: ballot.n,
        priority: ballot.priority,
        pid: ballot.pid,
    }
}

// PREPARE
fn prepare_rpc(prepare: Prepare) -> RpcPrepare {
    RpcPrepare {
        n: Some(ballot_rpc(prepare.n)),
        ld: prepare.ld,
        n_accepted: Some(ballot_rpc(prepare.n_accepted)),
        la: prepare.la,
    }
}

// STORE COMMAND
fn store_command_rpc(cmd: StoreCommand) -> RpcEntry {
    RpcEntry {
        id: cmd.id as u64,
        sql: cmd.sql.clone()
    }
}

// SYNC ITEM
fn sync_item_rpc(sync_item: SyncItem<StoreCommand, ()>) -> RpcSyncItem {
    RpcSyncItem {
        item: Some(match sync_item {
            SyncItem::Entries(vec) => rpc_sync_item::Item::Entries(rpc_sync_item::RpcEntries { e: vec.into_iter().map(|e| store_command_rpc(e)).collect() }),
            SyncItem::Snapshot(ss) => match ss {
                SnapshotType::Complete(_) => rpc_sync_item::Item::Snapshot(rpc_sync_item::Snapshot::Complete as i32),
                SnapshotType::Delta(_) => rpc_sync_item::Item::Snapshot(rpc_sync_item::Snapshot::Delta as i32),
                SnapshotType::_Phantom(_) => rpc_sync_item::Item::Snapshot(rpc_sync_item::Snapshot::Phantom as i32),
            },
            SyncItem::None => rpc_sync_item::Item::None(rpc_sync_item::None {}),
        }),
    }
}

// sTOP SIGN
fn stop_sign_rpc(stop_sign: StopSign) -> RpcStopSign {
    RpcStopSign {
        config_id: stop_sign.config_id,
        nodes: stop_sign.nodes,
        metadata: match stop_sign.metadata {
            Some(vec) => Some(RpcMetadata { data: vec.into_iter().map(|m| m as u32).collect() }),
            None => None,
        }
    }
}

// PROMISE
fn promise_rpc(promise: Promise<StoreCommand, ()>) -> RpcPromise {
    RpcPromise {
        n: Some(ballot_rpc(promise.n)),
        n_accepted: Some(ballot_rpc(promise.n_accepted)),
        sync_item: match promise.sync_item {
            Some(s) => Some(sync_item_rpc(s)),
            None => None,
        },
        ld: promise.ld,
        la: promise.la,
        ss: match promise.stopsign {
            Some(ss) => Some(stop_sign_rpc(ss)),
            None => None,
        },
    }
}

// ACCEPTSYNC
fn accept_sync_rpc(accept_sync: AcceptSync<StoreCommand, ()>) -> RpcAcceptSync {
    RpcAcceptSync {
        n: Some(ballot_rpc(accept_sync.n)),
        sync_item: Some(sync_item_rpc(accept_sync.sync_item)),
        sync_idx: accept_sync.sync_idx,
        decide_idx: accept_sync.decide_idx,
        ss: match accept_sync.stopsign {
            Some(ss) => Some(stop_sign_rpc(ss)),
            None => None,
        },
    }
}

// FIRST_ACCEPT
fn first_accept_rpc(first_accept: FirstAccept<StoreCommand>) -> RpcFirstAccept {
    RpcFirstAccept {
        n: Some(ballot_rpc(first_accept.n)),
        entries: first_accept.entries.into_iter().map(|e| store_command_rpc(e)).collect(),
    }
}

// ACCEPT_DECIDE
fn accept_decide_rpc(accept_decide: AcceptDecide<StoreCommand>) -> RpcAcceptDecide {
    RpcAcceptDecide {
        n: Some(ballot_rpc(accept_decide.n)),
        ld: accept_decide.ld,
        entries: accept_decide.entries.into_iter().map(|e| store_command_rpc(e)).collect(),
    }
}

// ACCEPTED
fn accepted_rpc(accepted: Accepted) -> RpcAccepted {
    RpcAccepted {
        n: Some(ballot_rpc(accepted.n)),
        la: accepted.la,
    }
}

// DECIDE
fn decide_rpc(decide: Decide) -> RpcDecide {
    RpcDecide {
        n: Some(ballot_rpc(decide.n)),
        ld: decide.ld,
    }
}

// PROPOSAL_FORWARD
fn proposal_forward_rpc(proposals: Vec<StoreCommand>) -> RpcProposalForward {
    RpcProposalForward {
        pf: proposals.into_iter().map(|e| store_command_rpc(e)).collect(),
    }
}

// COMPACTION
fn compaction_rpc(compaction: Compaction) -> RpcCompaction {
    RpcCompaction {
        compact: Some(match compaction {
            Compaction::Trim(trim) => rpc_compaction::Compact::T(rpc_compaction::Trim { trim:trim }),
            Compaction::Snapshot(ss) => rpc_compaction::Compact::S(ss as i32),
        }),
    }
}

// ACCEPT_STOP_SIGN
fn accept_stop_sign_rpc(accept_stop_sign: AcceptStopSign) -> RpcAcceptStopSign {
    RpcAcceptStopSign {
        n: Some(ballot_rpc(accept_stop_sign.n)),
        ss: Some(stop_sign_rpc(accept_stop_sign.ss)),
    }
}

// ACCEPTED_STOP_SIGN
fn accepted_stop_sign_rpc(accepted_stop_sign: AcceptedStopSign) -> RpcAcceptedStopSign {
    RpcAcceptedStopSign {
        n: Some(ballot_rpc(accepted_stop_sign.n)),
    }
}

// DECIDE_STOP_SIGN
fn decide_stop_sign_rpc(decide_stop_sign: DecideStopSign) -> RpcDecideStopSign {
    RpcDecideStopSign {
        n: Some(ballot_rpc(decide_stop_sign.n)),
    }
}

// HEARTBEAT REQUEST
fn heartbeat_request_rpc(heartbeat_request: HeartbeatRequest) -> RpcHeartbeatRequest {
    RpcHeartbeatRequest {
        round: heartbeat_request.round,
    }
}

// HEARTBEAT REPLY
fn heartbeat_reply_rpc(heartbeat_reply: HeartbeatReply) -> RpcHeartbeatReply {
    RpcHeartbeatReply {
        round: heartbeat_reply.round,
        ballot: Some(ballot_rpc(heartbeat_reply.ballot)),
        majority_connected: heartbeat_reply.majority_connected,
    }
}

// converts rpc messages into sp
fn sp_message_from_rpc(obj: RpcMessage) -> Message<StoreCommand, ()> {
    Message {
        from: obj.from,
        to: obj.to,
        msg: match obj.msg.unwrap() {
            rpc_message::Msg::PrepareReq(_) => PaxosMsg::PrepareReq,
            rpc_message::Msg::Prepare(prepare) => PaxosMsg::Prepare(prepare_from_rpc(prepare)),
            rpc_message::Msg::Promise(promise) => PaxosMsg::Promise(promise_from_rpc(promise)),
            rpc_message::Msg::Acceptsync(accept_sync) => PaxosMsg::AcceptSync(accept_sync_from_rpc(accept_sync)),
            rpc_message::Msg::Firstaccept(first_accept) => PaxosMsg::FirstAccept(first_accept_from_rpc(first_accept)),
            rpc_message::Msg::Acceptdecide(accept_decide) => PaxosMsg::AcceptDecide(accept_decide_from_rpc(accept_decide)),
            rpc_message::Msg::Accepted(accepted) => PaxosMsg::Accepted(accepted_from_rpc(accepted)),
            rpc_message::Msg::Decide(decide) => PaxosMsg::Decide(decide_from_rpc(decide)),
            rpc_message::Msg::Proposalforward(proposals) => PaxosMsg::ProposalForward(proposal_forward_from_rpc(proposals)),
            rpc_message::Msg::Compaction(compaction) => PaxosMsg::Compaction(compaction_from_rpc(compaction)),
            rpc_message::Msg::ForwardCompaction(compaction) => PaxosMsg::ForwardCompaction(compaction_from_rpc(compaction)),
            rpc_message::Msg::AcceptsSs(accept_stop_sign) => PaxosMsg::AcceptStopSign(accept_stop_sign_from_rpc(accept_stop_sign)),
            rpc_message::Msg::AcceptedSs(accepted_stop_sign) => PaxosMsg::AcceptedStopSign(accepted_stop_sign_from_rpc(accepted_stop_sign)),
            rpc_message::Msg::DecidedSs(decide_stop_sign) => PaxosMsg::DecideStopSign(decide_stop_sign_from_rpc(decide_stop_sign)),
        }
    }
}

// converts rpc messages of ble type into blw messages
fn ble_message_from_rpc(obj: RpcBlemessage) -> BLEMessage {
    BLEMessage {
        from: obj.from,
        to: obj.to,
        msg: match obj.msg.unwrap() {
            rpc_blemessage::Msg::Heartbeatrequest(request) => HeartbeatMsg::Request(heartbeat_request_from_rpc(request)),
            rpc_blemessage::Msg::Heartbeatreply(reply) => HeartbeatMsg::Reply(heartbeat_reply_from_rpc(reply)),
        }
    }
}

// BALLOT
fn ballot_from_rpc(obj: RpcBallot) -> Ballot {
    Ballot {
        n: obj.n,
        priority: obj.priority,
        pid: obj.pid,
    }
}

// PREPARE
fn prepare_from_rpc(obj: RpcPrepare) -> Prepare {
    Prepare {
        n: ballot_from_rpc(obj.n.unwrap()),
        ld: obj.ld,
        n_accepted: ballot_from_rpc(obj.n_accepted.unwrap()),
        la: obj.la,
    }
}

// STORE_COMMAND
fn store_command_from_rpc(obj: RpcEntry) -> StoreCommand {
    StoreCommand {
        id: obj.id as usize,
        sql: obj.sql.clone()
    }
}

// SYNC_ITEM
fn sync_item_from_rpc(obj: RpcSyncItem) -> SyncItem<StoreCommand, ()> {
    match obj.item.unwrap() {
        rpc_sync_item::Item::Entries(entries) => SyncItem::Entries(entries.e.into_iter().map(|e| store_command_from_rpc(e)).collect()),
        rpc_sync_item::Item::Snapshot(ss) => match rpc_sync_item::Snapshot::from_i32(ss) {
            Some(rpc_sync_item::Snapshot::Complete) => SyncItem::Snapshot(SnapshotType::Complete(())),
            Some(rpc_sync_item::Snapshot::Delta) => SyncItem::Snapshot(SnapshotType::Delta(())),
            Some(rpc_sync_item::Snapshot::Phantom) => SyncItem::Snapshot(SnapshotType::_Phantom(PhantomData)),
            _ => unimplemented!() 
        },
        rpc_sync_item::Item::None(_) => SyncItem::None,

    }
}

// STOP_SIGN
fn stop_sign_from_rpc(obj: RpcStopSign) -> StopSign {
    StopSign {
        config_id: obj.config_id,
        nodes: obj.nodes,
        metadata: match obj.metadata {
            Some(md) => Some(md.data.into_iter().map(|m| m as u8).collect()),
            None => None,
        },
    }
}

// PROMISE
fn promise_from_rpc(obj: RpcPromise) -> Promise<StoreCommand, ()> {
    Promise {
        n: ballot_from_rpc(obj.n.unwrap()),
        n_accepted: ballot_from_rpc(obj.n_accepted.unwrap()),
        sync_item: match obj.sync_item {
            Some(s) => Some(sync_item_from_rpc(s)),
            None => None,
        },
        ld: obj.ld,
        la: obj.la,
        stopsign: match obj.ss {
            Some(ss) => Some(stop_sign_from_rpc(ss)),
            None => None,
        },
    }
}

// ACCEPT_SYNC
fn accept_sync_from_rpc(obj: RpcAcceptSync) -> AcceptSync<StoreCommand, ()> {
    AcceptSync {
        n: ballot_from_rpc(obj.n.unwrap()),
        sync_item: sync_item_from_rpc(obj.sync_item.unwrap()),
        sync_idx: obj.sync_idx,
        decide_idx: obj.decide_idx,
        stopsign: match obj.ss {
            Some(ss) => Some(stop_sign_from_rpc(ss)),
            None => None,
        },
    }
}

// FIRST_ACCEPT
fn first_accept_from_rpc(obj: RpcFirstAccept) -> FirstAccept<StoreCommand> {
    FirstAccept {
        n: ballot_from_rpc(obj.n.unwrap()),
        entries: obj.entries.into_iter().map(|e| store_command_from_rpc(e)).collect(),
    }
}

// ACCEPT_DECIDE
fn accept_decide_from_rpc(obj: RpcAcceptDecide) -> AcceptDecide<StoreCommand> {
    AcceptDecide {
        n: ballot_from_rpc(obj.n.unwrap()),
        ld: obj.ld,
        entries: obj.entries.into_iter().map(|e| store_command_from_rpc(e)).collect(),
    }
}

// ACCEPTED
fn accepted_from_rpc(obj: RpcAccepted) -> Accepted {
    Accepted {
        n: ballot_from_rpc(obj.n.unwrap()),
        la: obj.la,
    }
}

// DECIDE
fn decide_from_rpc(obj: RpcDecide) -> Decide {
    Decide {
        n: ballot_from_rpc(obj.n.unwrap()),
        ld: obj.ld,
    }
}

// PROPOSAL_FORWARD
fn proposal_forward_from_rpc(obj: RpcProposalForward) -> Vec<StoreCommand> {
    obj.pf.into_iter().map(|e| store_command_from_rpc(e)).collect()
}

// COMPACTION
fn compaction_from_rpc(obj: RpcCompaction) -> Compaction {
    match obj.compact.unwrap() {
        rpc_compaction::Compact::T(trim) => Compaction::Trim(trim.trim),
        rpc_compaction::Compact::S(ss) => Compaction::Snapshot(ss as u64),
    }
}

// ACCEPT_STOP_SIGN
fn accept_stop_sign_from_rpc(obj: RpcAcceptStopSign) -> AcceptStopSign {
    AcceptStopSign {
        n: ballot_from_rpc(obj.n.unwrap()),
        ss: stop_sign_from_rpc(obj.ss.unwrap()),
    }
}

// ACCEPTED_STOP_SIGN
fn accepted_stop_sign_from_rpc(obj: RpcAcceptedStopSign) -> AcceptedStopSign {
    AcceptedStopSign {
        n: ballot_from_rpc(obj.n.unwrap()),
    }
}

// DECIDE_STOP_SIGN
fn decide_stop_sign_from_rpc(obj: RpcDecideStopSign) -> DecideStopSign {
    DecideStopSign {
        n: ballot_from_rpc(obj.n.unwrap()),
    }
}

// HEARTBEAT_REQUEST
fn heartbeat_request_from_rpc(obj: RpcHeartbeatRequest) -> HeartbeatRequest {
    HeartbeatRequest {
        round: obj.round,
    }
}

// HEARTBEAT_REPLY
fn heartbeat_reply_from_rpc(obj: RpcHeartbeatReply) -> HeartbeatReply {
    HeartbeatReply {
        round: obj.round,
        ballot: ballot_from_rpc(obj.ballot.unwrap()),
        majority_connected: obj.majority_connected,
    }
}