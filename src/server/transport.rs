// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::raft_serverpb::RaftMessage;

use crate::server::raft_client::RaftClient;
use crate::server::resolve::StoreAddrResolver;
use engine_traits::KvEngine;
use raftstore::router::RaftStoreRouter;
use raftstore::store::Transport;
use raftstore::Result as RaftStoreResult;
use std::marker::PhantomData;

pub struct ServerTransport<T, S, E>
where
    T: RaftStoreRouter<E> + 'static,
    S: StoreAddrResolver + 'static,
    E: KvEngine,
{
    raft_client: RaftClient<S, T, E>,
    engine: PhantomData<E>,
}

impl<T, S, E> Clone for ServerTransport<T, S, E>
where
    T: RaftStoreRouter<E> + 'static,
    S: StoreAddrResolver + 'static,
    E: KvEngine,
{
    fn clone(&self) -> Self {
        ServerTransport {
            raft_client: self.raft_client.clone(),
            engine: PhantomData,
        }
    }
}

impl<T, S, E> ServerTransport<T, S, E>
where
    E: KvEngine,
    T: RaftStoreRouter<E> + 'static,
    S: StoreAddrResolver + 'static,
{
    pub fn new(raft_client: RaftClient<S, T, E>) -> ServerTransport<T, S, E> {
        ServerTransport {
            raft_client,
            engine: PhantomData,
        }
    }
}
// 负责将 Raft 消息发送到指定的 store。
impl<T, S, E> Transport for ServerTransport<T, S, E>
where
    T: RaftStoreRouter<E> + Unpin + 'static,
    S: StoreAddrResolver + Unpin + 'static,
    E: KvEngine,
{
    // TiKV 实际运行时使用的 Transport的实现（Transporttrait 的定义在 raftstore 中），
    // 其内部包含一个 RaftClient用于进行 RPC 通信。
    // 发送消息时，ServerTransport通过上面说到的 Resolver 将消息中的 store id
    // 解析为地址，并将解析的结果存入 raft_client.addrs中；下次向同一个 store 发送消息时便不再需要再次解析
    fn send(&mut self, msg: RaftMessage) -> RaftStoreResult<()> {
        match self.raft_client.send(msg) {
            Ok(()) => Ok(()),
            Err(reason) => Err(raftstore::Error::Transport(reason)),
        }
    }

    fn need_flush(&self) -> bool {
        self.raft_client.need_flush()
    }

    fn flush(&mut self) {
        self.raft_client.flush();
    }
}
