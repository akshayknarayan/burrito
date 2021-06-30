use super::{CapabilitySet, Negotiate, Select};
use crate::CxList;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::collections::HashMap;
use std::iter::{once, Chain, Once as IterOnce};

/// available is a Vec<T::Capability> where T is a negotiation type.
/// capability_guid identifies T::Capability.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Offer {
    pub capability_guid: u64,
    pub impl_guid: u64,
    /// None => two-sided, Some(Vec<offers>) => one-sided with universe
    pub sidedness: Option<Vec<Vec<u8>>>,
    /// Each serialized T::Capability is the inner Vec<u8>, and the list of them is the list of
    /// capabilities.
    pub available: Vec<Vec<u8>>,
}

fn get_offer<T>() -> Option<Offer>
where
    T: Negotiate,
    <T as Negotiate>::Capability: Serialize + DeserializeOwned,
{
    if T::Capability::guid() != 0 {
        Some(Offer {
            capability_guid: T::Capability::guid(),
            impl_guid: T::guid(),
            sidedness: T::Capability::universe().map(|univ| {
                univ.iter()
                    .map(|c| bincode::serialize(c).unwrap())
                    .collect()
            }),
            available: T::capabilities()
                .iter()
                .map(|c| bincode::serialize(c).unwrap())
                .collect(),
        })
    } else {
        None
    }
}

/// Get an iterator of possible stacks this stack could produce.
///
/// Each stack is represented by a map collecting unique negotiation-capabilities it contains in
/// guid form (u64) -> a list of Offer, which contain in serialized form the capabilities
/// available.
///
/// The number of enumerated stacks will be 2^(number of selects).
///
/// ```rust
/// # use bertha::{reliable::ReliabilityProjChunnel, tagger::OrderedChunnelProj, CxList,
/// negotiate::GetOffers};
/// let ls = CxList::from(OrderedChunnelProj::default()).wrap(ReliabilityProjChunnel::default());
/// let offers: Vec<_> = ls.offers().collect();
/// println!("{:?}", offers);
/// ```
pub trait GetOffers {
    type Iter: Iterator<Item = HashMap<u64, Offer>>;
    fn offers(&self) -> Self::Iter;
}

impl<N, C> GetOffers for N
where
    N: Negotiate<Capability = C>,
    C: CapabilitySet + Serialize + DeserializeOwned,
{
    type Iter = IterOnce<HashMap<u64, Offer>>;

    fn offers(&self) -> Self::Iter {
        let mut h = HashMap::default();
        if let Some(o) = get_offer::<N>() {
            h.insert(C::guid(), o);
        }

        once(h)
    }
}

impl<H, T> GetOffers for CxList<H, T>
where
    H: GetOffers,
    <H as GetOffers>::Iter: Clone,
    T: GetOffers,
{
    type Iter = std::vec::IntoIter<HashMap<u64, Offer>>;

    fn offers(&self) -> Self::Iter {
        let tail_iter = self.tail.offers();
        let head_iter = self.head.offers();

        fn merge(l: HashMap<u64, Offer>, mut r: HashMap<u64, Offer>) -> HashMap<u64, Offer> {
            for (guid, o) in l {
                if let Some(ent) = r.get_mut(&guid) {
                    ent.impl_guid ^= o.impl_guid;
                    ent.available.extend(o.available);
                } else {
                    r.insert(guid, o);
                }
            }

            r
        }

        let mut opts = vec![];
        for tail_opt in tail_iter {
            for head_opt in head_iter.clone() {
                opts.push(merge(head_opt, tail_opt.clone()));
            }
        }

        opts.into_iter()
    }
}

impl<T1, T2, I> GetOffers for Select<T1, T2, I>
where
    T1: GetOffers,
    T2: GetOffers,
{
    type Iter = Chain<T1::Iter, T2::Iter>;

    fn offers(&self) -> Self::Iter {
        let left = self.left.offers();
        let right = self.right.offers();
        left.chain(right)
    }
}
