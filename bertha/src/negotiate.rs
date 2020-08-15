//! Chunnel wrapper types to negotiate between multiple implementations.

use crate::{
    ChunnelConnection, ChunnelConnector, ChunnelListener, Client, ConnectAddress, CxList, CxNil,
    Either, Endedness, ListenAddress, Scope, Serve,
};
use color_eyre::{eyre, Report};
use futures_util::stream::{Stream, TryStreamExt};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

/// A type that can list out the `universe()` of possible values it can have.
pub trait CapabilitySet: core::fmt::Debug + PartialEq + Sized {
    /// All possible values this type can have.
    // TODO make return an unordered collection
    fn universe() -> Vec<Self>;

    fn guid() -> u64;
}

impl CapabilitySet for () {
    fn universe() -> Vec<Self> {
        vec![()]
    }

    fn guid() -> u64 {
        0
    }
}

impl<T, U> CapabilitySet for (Vec<T>, Vec<U>)
where
    T: CapabilitySet + Clone,
    U: CapabilitySet,
{
    fn universe() -> Vec<Self> {
        vec![(T::universe(), U::universe())]
    }

    fn guid() -> u64 {
        T::guid() + U::guid()
    }
}

pub trait NegotiateDummy {}

/// Define an enum that implements the `CapabilitySet` trait.
///
/// Invoke with enum name (with optional `pub`) followed by variant names.
///
/// # Example
/// ```rust
/// # use bertha::enumerate_enum;
/// enumerate_enum!(pub Foo, A, B, C);
/// enumerate_enum!(Bar, A, B, C);
/// fn main() {
///     let f = Foo::B;
///     let b = Bar::C;
///     println!("{:?}, {:?}", f, b);
/// }
/// ```
#[macro_export]
macro_rules! enumerate_enum {
    (pub $name:ident, $($variant:ident),+) => {
        #[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
        pub enum $name {
            $(
                $variant
            ),+
        }

        impl $crate::negotiate::CapabilitySet for $name {
            fn universe() -> Vec<Self> {
                vec![
                    $($name::$variant),+
                ]
            }

            fn guid() -> u64 {
                0xe898734df758d0c0 // TODO eventually we'll have an enum type that is not ShardFns
            }
        }

        impl $crate::negotiate::NegotiateDummy for $name {}
    };
    ($(keyw:ident)* $name:ident, $($variant:ident),+) => {
        #[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
        enum $name {
            $(
                $variant
            ),+
        }

        impl $crate::negotiate::CapabilitySet for $name {
            fn universe() -> Vec<Self> {
                vec![
                    $($name::$variant),+
                ]
            }

            fn guid() -> u64 {
                0xe898734df758d0c0
            }
        }

        impl $crate::negotiate::NegotiateDummy for $name {}
    };
}

/// Expresses the ability to negotiate chunnel implementations over a set of capabilities enumerated
/// by the `Capability` type.
///
/// Read: `Negotiate` *over* `Capability`.
///
/// TODO Add endedness to this trait: onesided_capabilities vs bothsided_capabilities
pub trait Negotiate {
    type Capability: CapabilitySet;
    fn capabilities() -> Vec<Self::Capability> {
        vec![]
    }
}

impl Negotiate for CxNil {
    type Capability = ();
    fn capabilities() -> Vec<Self::Capability> {
        vec![()]
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Offer {
    capability_guid: u64,
    available: Vec<u8>,
}

fn get_offer<T>() -> Offer
where
    T: Negotiate,
    <T as Negotiate>::Capability: Serialize + DeserializeOwned,
{
    Offer {
        capability_guid: T::Capability::guid(),
        available: bincode::serialize(&T::capabilities()).unwrap(),
    }
}

pub trait GetOffers {
    fn offers(&self) -> Vec<Vec<Offer>>;
}

impl<H, T> GetOffers for CxList<H, T>
where
    H: GetOffers,
    T: GetOffers,
{
    fn offers(&self) -> Vec<Vec<Offer>> {
        let mut offers = self.head.offers();
        let rest = self.tail.offers();
        offers.extend(rest);
        offers
    }
}

impl<N> GetOffers for N
where
    N: Negotiate,
    <N as Negotiate>::Capability: Serialize + DeserializeOwned,
{
    fn offers(&self) -> Vec<Vec<Offer>> {
        vec![vec![get_offer::<N>()]]
    }
}

/// Negotiation type to choose between `T1` and `T2`.
pub struct Select<T1, T2>(pub T1, pub T2);

impl<T1, T2> GetOffers for Select<T1, T2>
where
    T1: GetOffers,
    T2: GetOffers,
{
    fn offers(&self) -> Vec<Vec<Offer>> {
        // TODO nested select?
        let t1 = self.0.offers()[0].clone();
        let t2 = self.1.offers()[0].clone();
        vec![t1, t2]
    }
}

pub async fn negotiate_server<H, T, A>(
    stack: CxList<H, T>,
    a: A,
) -> Result<
    impl Stream<Item = Result<<<A as ListenAddress>::Listener as ChunnelListener>::Connection, Report>>,
    Report,
>
where
    A: ListenAddress,
    <<A as ListenAddress>::Listener as ChunnelListener>::Error:
        Into<Report> + Send + Sync + 'static,
    CxList<H, T>: Serve<<<A as ListenAddress>::Listener as ChunnelListener>::Stream>,
    CxList<H, T>: GetOffers,
{
    // 0. CxList of impl Serve/Select<impl Serve, impl Serve> -> Vec<Vec<Offer>>
    let offers = stack.offers();

    // 1. serve Vec<u8> connections.
    let mut listener = a.listener();
    let st = listener.listen(a).await.map_err(|e| e.into())?; // stream of incoming Vec<u8> conns.

    Ok(st
        .and_then(|cn| async move {
            Ok(cn)
            // 2. on new connection, read off Vec<Vec<Offer>> from
            //    client
            // 3. transform the CxList<impl Serve/Select<impl Serve, impl Serve>> into a CxList<impl Serve>
            // 4. new_stack.serve(vec_u8_stream)
        })
        .map_err(|e| e.into()))
}

//pub fn negotiate_client<H, T>(stack: CxList<H, T>, a: impl ConnectAddress) {}

#[cfg(test)]
mod test {
    use super::Negotiate;
    use crate::{enumerate_enum, CxList, CxNil};

    #[test]
    fn negotiate_cxlist() {
        enumerate_enum!(Foo, A, B, C);
        struct FooNeg;
        impl Negotiate<Foo> for FooNeg {
            fn capabilities() -> Vec<Foo> {
                vec![Foo::B, Foo::C]
            }
        }
        enumerate_enum!(Bar, D, E);
        struct BarNeg;
        impl Negotiate<Bar> for BarNeg {
            fn capabilities() -> Vec<Bar> {
                vec![Bar::D, Bar::E]
            }
        }
        enumerate_enum!(Baz, F, G);
        struct BazNeg;
        impl Negotiate<Baz> for BazNeg {
            fn capabilities() -> Vec<Baz> {
                vec![Baz::G]
            }
        }

        let n: Vec<(Vec<Foo>, Vec<(Vec<Bar>, Vec<(Vec<Baz>, Vec<()>)>)>)> =
            CxList::<FooNeg, CxList<BarNeg, CxList<BazNeg, CxNil>>>::capabilities();
        assert_eq!(
            n,
            vec![(
                vec![Foo::B, Foo::C],
                vec![(vec![Bar::D, Bar::E], vec![(vec![Baz::G], vec![()])])]
            )]
        );
    }
}

//macro_rules! cxlist_type {
//    ($head:ty) => (
//        CxList<$head, CxNil>
//    );
//    ($head:ty, $($t: ty),+) => (
//        CxList<$head, cxlist_type!($($t),+)>
//    );
//}
//
//macro_rules! expand_arity {
//    ($($t:ident),*) => {
//        impl < $($t),* > cxlist_type!($($t),*)
//        where
//            $(
//                $t: Negotiate,
//                <$t as Negotiate>::Capability: CapabilitySet + Serialize + DeserializeOwned + Clone
//            ),*
//        {
//            fn offers (&self) -> Vec<Offer> {
//                vec![
//                    $(
//                        get_offer::<$t>()
//                    ),*
//                ]
//            }
//        }
//    };
//}
//
//expand_arity!(T1);
//expand_arity!(T1, T2);
//expand_arity!(T1, T2, T3);
