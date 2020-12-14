use crate::{either::Either, negotiate::Offer};
use crate::{Apply, Client, CxList, CxNil, Serve};
use color_eyre::eyre::{self, Report};

/// Declared semantics specifying how a connection will be used.
///
/// This is different from negotiation, which is a process where the client and server agree to a
/// set of Chunnel semantics. These semantics specify how the connection object will be used
/// locally.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub enum ConnectionSemantics {
    /// No specification.
    Generic,
    /// For each call to `send()`, there will be a single corresponding subsequent call to
    /// `recv()`.
    RequestResponse,
}

pub trait SemanticsPicker {
    // Return value should impl Apply, but we can't express this since we don't know what Applied
    // should be.
    // TODO this will need to be fixed, currently there's no way to pass config options to the
    // picked chunnel
    fn pick_client(self, semantics: ConnectionSemantics) -> Box<dyn std::any::Any>;
}

impl<A> SemanticsPicker for A
where
    A: Apply + 'static,
{
    fn pick_client(self, _semantics: ConnectionSemantics) -> Box<dyn std::any::Any> {
        Box::new(self) as _
    }
}

//impl<H, T> SemanticsPicker for CxList<H, T>
//where
//    H: SemanticsPicker,
//    T: SemanticsPicker,
//{
//    fn pick_client(self, semantics: ConnectionSemantics) -> Box<dyn std::any::Any> {
//        CxList {
//            head: self.head.pick_client(semantics),
//            tail: self.tail.pick_client(semantics),
//        }
//    }
//}
