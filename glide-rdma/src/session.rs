// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

//! A handshake accomplishes two things:
//!
//! 1. The client learns the server node's fabric addresses and inserts them into
//!    its local address vector so an RDMA transfer the node initiates is accepted.
//! 2. The node learns the client's fabric address via `LO.HELLO`.
//!
//! The second is why a session belongs to a connection. The node holds the
//! client's address against the connection the handshake arrived on, so a
//! transfer that reaches it on any other connection finds no session.

/// The outcome of one handshake with one server node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Handshake {
    /// Every fabric address the server node may initiate a transfer from.
    pub peers: Vec<Vec<u8>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_handshake_carries_every_address_the_node_may_transfer_from() {
        let handshake = Handshake {
            peers: vec![vec![1, 2, 3]],
        };
        assert_eq!(handshake.peers, vec![vec![1, 2, 3]]);
    }
}
