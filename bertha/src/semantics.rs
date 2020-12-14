use crate::{either::Either, negotiate::Offer};
use crate::{Apply, Client, CxList, CxNil, Serve};
use color_eyre::eyre::{self, Report};

/// Declared semantics specifying how a connection will be used.
///
/// This is different from negotiation, which is a process where the client and server agree to a
/// set of Chunnel semantics. These semantics specify how the connection object will be used
/// locally.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub enum ConnectionSemantics<A1, A2> {
    /// No specification.
    Generic(Option<A1>),
    /// For each call to `send()`, there will be a single corresponding subsequent call to
    /// `recv()`.
    RequestResponse(Option<A2>),
}

impl<A1, A2> Apply for ConnectionSemantics<A1, A2>
where
    A1: Apply,
    A2: Apply,
{
    type Applied = Either<A1::Applied, A2::Applied>;
    fn apply(self, offers: Vec<Offer>) -> Result<(Self::Applied, Option<Vec<Offer>>), Report> {
        use ConnectionSemantics::*;
        Ok(match self {
            Generic(Some(a)) => {
                let (e, o) = a.apply(offers)?;
                (Either::Left(e), o)
            }
            RequestResponse(Some(a)) => {
                let (e, o) = a.apply(offers)?;
                (Either::Right(e), o)
            }
            _ => {
                eyre::bail!("Semantics is not populated but apply was called");
            }
        })
    }
}

pub trait SemanticsPicker<A1, A2> {
    // TODO this will need to be fixed, currently there's no way to pass config options to the
    // picked chunnel
    fn pick_client(self, semantics: ConnectionSemantics<A1, A2>) -> ConnectionSemantics<A1, A2>;
}

impl<A1, A2> SemanticsPicker<A1, A2> for A1
where
    A1: Apply,
{
    fn pick_client(self, semantics: ConnectionSemantics<A1, A2>) -> ConnectionSemantics<A1, A2> {
        ConnectionSemantics::Generic(Some(self))
    }
}

//impl<H, T> SemanticsPicker for CxList<H, T>
//where
//    H: SemanticsPicker,
//    T: SemanticsPicker,
//{
//    fn pick_client(self, semantics: ConnectionSemantics) -> Box<dyn Apply> {
//        CxList {
//            head: self.head.pick_client(semantics),
//            tail: self.tail.pick_client(semantics),
//        }
//    }
//}
