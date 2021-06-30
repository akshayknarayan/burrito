use super::{CapabilitySet, Negotiate, Offer, Select};
use crate::{either::MakeEither, CxList, Either, FlipEither};
use color_eyre::eyre::{eyre, Report, WrapErr};
use serde::de::DeserializeOwned;
use std::collections::{HashMap, HashSet};
use tracing::{debug, trace};

#[derive(Debug, Clone)]
pub struct ApplyResult<A> {
    pub(crate) applied: A,
    pub(crate) picked: HashMap<u64, Offer>,
    pub(crate) touched: HashSet<u64>,
    pub(crate) score: usize,
}

pub trait Apply {
    type Applied;
    fn apply(
        self,
        picked_offers: HashMap<u64, Offer>,
    ) -> Result<ApplyResult<Self::Applied>, Report>;
}

impl<N> Apply for N
where
    N: Negotiate,
    <N as Negotiate>::Capability: DeserializeOwned + Ord,
{
    type Applied = Self;
    fn apply(
        self,
        mut picked_offers: HashMap<u64, Offer>,
    ) -> Result<ApplyResult<Self::Applied>, Report> {
        let cap_guid = N::Capability::guid();
        if cap_guid == 0 {
            return Ok(ApplyResult {
                applied: self,
                picked: picked_offers,
                touched: Default::default(),
                score: 0,
            });
        }

        if let Some(offer) = picked_offers.get_mut(&cap_guid) {
            if offer.sidedness.is_none() {
                offer.impl_guid ^= N::guid();
            }
        } else {
            return Err(eyre!(
                "Offer didn't contain needed capability guid: guid={:?}",
                cap_guid,
            ));
        }

        Ok(ApplyResult {
            applied: self,
            picked: picked_offers,
            touched: [N::Capability::guid()].iter().copied().collect(),
            score: 0,
        })
    }
}

impl<H, T> Apply for CxList<H, T>
where
    H: Apply,
    T: Apply,
{
    type Applied = CxList<H::Applied, T::Applied>;

    fn apply(
        self,
        picked_offers: HashMap<u64, Offer>,
    ) -> Result<ApplyResult<Self::Applied>, Report> {
        let ApplyResult {
            applied: head_pick,
            picked,
            score: h_score,
            touched: h_touched,
        } = self.head.apply(picked_offers)?;
        let ApplyResult {
            applied: tail_pick,
            picked,
            score: t_score,
            touched: t_touched,
        } = self.tail.apply(picked)?;
        Ok(ApplyResult {
            applied: CxList {
                head: head_pick,
                tail: tail_pick,
            },
            picked,
            touched: h_touched.union(&t_touched).copied().collect(),
            score: h_score + t_score,
        })
    }
}

pub(crate) fn check_apply<T: Apply>(
    t: T,
    picked: HashMap<u64, Offer>,
) -> Result<ApplyResult<T::Applied>, Report> {
    let ar = t.apply(picked)?;
    let p = &ar.picked;
    trace!(checking = ?p, "checking apply");
    if ar.touched.iter().all(|c| match p.get(c) {
        Some(Offer {
            sidedness: None,
            impl_guid,
            ..
        }) if *impl_guid == 0 => true,
        Some(Offer {
            sidedness: Some(_), ..
        }) => true,
        _ => false,
    }) {
        Ok(ar)
    } else {
        Err(eyre!("Offers mismatched"))
    }
}

impl<T1, T2, Inner, E> Apply for Select<T1, T2, Inner>
where
    T1: Apply,
    T2: Apply,
    Inner: MakeEither<<T1 as Apply>::Applied, <T2 as Apply>::Applied, Either = E>
        + MakeEither<<T2 as Apply>::Applied, <T1 as Apply>::Applied>,
    <Inner as MakeEither<<T2 as Apply>::Applied, <T1 as Apply>::Applied>>::Either:
        FlipEither<Flipped = E>,
{
    type Applied = E;

    fn apply(
        self,
        picked_offers: HashMap<u64, Offer>,
    ) -> Result<ApplyResult<Self::Applied>, Report> {
        fn apply_in_preference_order<T1, T2, Inner>(
            first_pick: T1,
            second_pick: T2,
            picked_offers: HashMap<u64, Offer>,
        ) -> Result<ApplyResult<Inner::Either>, Report>
        where
            T1: Apply,
            T2: Apply,
            Inner: MakeEither<<T1 as Apply>::Applied, <T2 as Apply>::Applied>,
        {
            match check_apply(first_pick, picked_offers.clone()) {
                Ok(ApplyResult {
                    applied: t1_applied,
                    picked,
                    touched,
                    score,
                }) => Ok(ApplyResult {
                    applied: Inner::left(t1_applied),
                    picked,
                    touched,
                    score: score + 1,
                }),
                Err(e) => {
                    debug!(t1 = std::any::type_name::<T1>(), err = %format!("{:#}", &e), "Select::T1 mismatched");
                    let ApplyResult {
                        applied: t2_applied,
                        picked,
                        touched,
                        score,
                    } = check_apply(second_pick, picked_offers).wrap_err(e)?;
                    Ok(ApplyResult {
                        applied: Inner::right(t2_applied),
                        picked,
                        touched,
                        score,
                    })
                }
            }
        }

        match self.prefer {
            Either::Left(_) => {
                apply_in_preference_order::<T1, T2, Inner>(self.left, self.right, picked_offers)
            }
            Either::Right(_) => {
                let ApplyResult {
                    applied,
                    picked,
                    touched,
                    score,
                } = apply_in_preference_order::<T2, T1, Inner>(
                    self.right,
                    self.left,
                    picked_offers,
                )?;
                Ok(ApplyResult {
                    applied: applied.flip(),
                    picked,
                    touched,
                    score,
                })
            }
        }
    }
}
