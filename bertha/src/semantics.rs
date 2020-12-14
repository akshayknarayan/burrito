use crate::negotiate::Apply;
use crate::{Client, Serve};

/// Declared semantics specifying how a connection will be used.
///
/// This is different from negotiation, which is a process where the client and server agree to a
/// set of Chunnel semantics. These semantics specify how the connection object will be used
/// locally.
#[derive(Debug, Clone, Copy)]
pub enum ConnectionSemantics {
    /// No specification.
    Generic,
    /// For each call to `send()`, there will be a single corresponding subsequent call to
    /// `recv()`.
    RequestResponse,
}

pub trait SemanticsPicker {
    fn pick_client(self, semantics: ConnectionSemantics) -> impl Apply;
}

impl<A> SemanticsPicker for A
where
    A: Apply,
{
    fn pick_client(self, semantics: ConnectionSemantics) -> impl Apply {
        self
    }
}

impl<H, T> SemanticsPicker for CxList<H, T>
where
    H: SemanticsPicker,
    T: SemanticsPicker,
{
    fn pick_client(self, semantics: ConnectionSemantics) -> impl Apply {
        CxList {
            head: self.head.pick_client(semantics),
            tail: self.tail.pick_client(semantics),
        }
    }
}
