//! Owner-socket peer credential policy tests.
//!
//! The runtime reads SO_PEERCRED into `ConnectionContext`; these tests exercise
//! Lojix's policy over synthetic kernel credential triples without requiring a
//! second Unix user on the test host.

use lojix::Error;
use lojix::daemon::OwnerPeerAuthority;
use triad_runtime::ConnectionContext;

#[test]
fn owner_peer_authority_accepts_matching_user_and_group() {
    let authority = OwnerPeerAuthority::new(1000, 100);
    let context = ConnectionContext::new(1000, 100, 42);

    authority.authorize(&context).expect("matching peer");
}

#[test]
fn owner_peer_authority_rejects_user_mismatch() {
    let authority = OwnerPeerAuthority::new(1000, 100);
    let context = ConnectionContext::new(1001, 100, 42);

    let error = authority.authorize(&context).expect_err("user mismatch");

    assert!(matches!(
        error,
        Error::UnauthorizedOwnerPeer {
            peer_user_id: 1001,
            peer_group_id: 100,
            daemon_user_id: 1000,
            daemon_group_id: 100,
        }
    ));
}

#[test]
fn owner_peer_authority_rejects_group_mismatch() {
    let authority = OwnerPeerAuthority::new(1000, 100);
    let context = ConnectionContext::new(1000, 101, 42);

    let error = authority.authorize(&context).expect_err("group mismatch");

    assert!(matches!(
        error,
        Error::UnauthorizedOwnerPeer {
            peer_user_id: 1000,
            peer_group_id: 101,
            daemon_user_id: 1000,
            daemon_group_id: 100,
        }
    ));
}
