use super::{have_all, CapabilitySet, Negotiate, Offer, Select, StackNonce};
use crate::{either::MakeEither, CxList, Either, FlipEither};
use color_eyre::eyre::{eyre, Report, WrapErr};
use serde::{de::DeserializeOwned, Serialize};
use std::collections::HashSet;
use std::fmt::Debug;

/// Result of a `Pick`.
///
/// `filtered_pairs` is a set of pairs that is consistent with `P`.
/// `touched_cap_guids` enumerates the capability guids that this operation touched.
#[derive(Debug, Clone)]
pub struct PickResult<P> {
    pub(crate) stack: P,
    pub(crate) filtered_pairs: Vec<(StackNonce, StackNonce)>,
    pub(crate) touched_cap_guids: HashSet<u64>,
}

/// Trait to monomorphize a CxList with possible `Select`s into something that impls Chunnel
pub trait Pick {
    type Picked;

    /// input: set of valid (client, server) offer pairs
    ///
    /// Returns (new_stack, mutated_pairs, handled_cap_guids).
    fn pick(
        self,
        offer_pairs: Vec<(StackNonce, StackNonce)>,
    ) -> Result<PickResult<Self::Picked>, Report>;
}

impl<N, C> Pick for N
where
    N: Negotiate<Capability = C>,
    C: CapabilitySet + Serialize + DeserializeOwned + Clone,
{
    type Picked = Self;

    fn pick(
        self,
        offer_pairs: Vec<(StackNonce, StackNonce)>,
    ) -> Result<PickResult<Self::Picked>, Report> {
        if C::guid() == 0 {
            return Ok(PickResult {
                stack: self,
                filtered_pairs: offer_pairs,
                touched_cap_guids: Default::default(),
            });
        }

        let filtered_pairs = offer_pairs
            .into_iter()
            .filter_map(|(client, server)| {
                let client = client.0;
                let mut server = server.0;
                let cap_guid = C::guid();
                if let Some(offer) = server.get_mut(&cap_guid) {
                    // one-sided checked in `check_touched`
                    if offer.sidedness.is_none() {
                        // check client matches:
                        if let Some(cl_of) = client.get(&cap_guid) {
                            // client and server must have the same set.
                            if !have_all(&cl_of.available, &offer.available)
                                || !have_all(&offer.available, &cl_of.available)
                            {
                                return None;
                            }

                            offer.impl_guid ^= N::guid();
                        } else {
                            // if this cap_guid is not in the client list, it's not valid because
                            // they must match
                            return None;
                        }
                    }
                } else {
                    // if client has it and we don't, no match.
                    if client.contains_key(&cap_guid) {
                        return None;
                    }
                }

                Some((StackNonce(client), StackNonce(server)))
            })
            .collect();

        Ok(PickResult {
            stack: self,
            filtered_pairs,
            touched_cap_guids: [C::guid()].iter().copied().collect(),
        })
    }
}

impl<T> Pick for Option<T>
where
    T: Pick,
{
    type Picked = Option<T::Picked>;

    fn pick(
        self,
        offer_pairs: Vec<(StackNonce, StackNonce)>,
    ) -> Result<PickResult<Self::Picked>, Report> {
        if let Some(inner) = self {
            let PickResult {
                stack,
                filtered_pairs,
                touched_cap_guids,
            } = inner.pick(offer_pairs)?;
            Ok(PickResult {
                stack: Some(stack),
                filtered_pairs,
                touched_cap_guids,
            })
        } else {
            Ok(PickResult {
                stack: None,
                filtered_pairs: offer_pairs,
                touched_cap_guids: Default::default(),
            })
        }
    }
}

impl<H, T> Pick for CxList<H, T>
where
    H: Pick,
    T: Pick,
{
    type Picked = CxList<H::Picked, T::Picked>;

    fn pick(
        self,
        offer_pairs: Vec<(StackNonce, StackNonce)>,
    ) -> Result<PickResult<Self::Picked>, Report> {
        let PickResult {
            stack: head_pick,
            filtered_pairs,
            touched_cap_guids: head_caps,
        } = self
            .head
            .pick(offer_pairs)
            .wrap_err_with(|| eyre!("CxList head pick failed: {:?}", std::any::type_name::<H>()))?;
        let PickResult {
            stack: tail_pick,
            filtered_pairs,
            touched_cap_guids: tail_caps,
        } = self
            .tail
            .pick(filtered_pairs)
            .wrap_err_with(|| eyre!("CxList tail pick failed: {:?}", std::any::type_name::<T>()))?;

        Ok(PickResult {
            stack: CxList {
                head: head_pick,
                tail: tail_pick,
            },
            filtered_pairs,
            touched_cap_guids: head_caps.union(&tail_caps).copied().collect(),
        })
    }
}

pub(crate) fn check_touched_complete<T: Pick>(
    t: T,
    pairs: Vec<(StackNonce, StackNonce)>,
) -> Result<PickResult<T::Picked>, Report>
where
    T::Picked: Debug,
{
    let pr = t
        .pick(pairs.clone())
        .wrap_err_with(|| eyre!("pick failed: {:?}", pairs))?;
    let touched = &pr.touched_cap_guids;
    let pairs: Vec<_> = pr
        .filtered_pairs
        .into_iter()
        .map(|(client, server)| {
            let client_m = &client.0;
            let server_m = &server.0;

            // if client has something with sidedness none, that means the server has to match its
            // capabilities. So if we didn't touch it, then invalid.
            if !client_m
                .iter()
                .all(|(guid, of)| of.sidedness.is_some() || touched.get(guid).is_some())
            {
                return Err(eyre!(
                    "t: {:?} pairs: {:?}, touched set {:?} missing touched guids: {:?}",
                    std::any::type_name::<T>(),
                    pairs,
                    touched,
                    client_m
                        .iter()
                        .filter_map(|(guid, of)| {
                            if of.sidedness.is_none() && touched.get(guid).is_none() {
                                Some(guid)
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<_>>()
                ));
            }

            let impls_matched = touched.iter().all(|t| {
                // for all the things we did touch, the impls should match.
                let of = if let Some(o) = server_m.get(t) {
                    o
                } else {
                    return false;
                };
                match of {
                    Offer { impl_guid, .. } if *impl_guid == 0 => true,
                    Offer {
                        sidedness: Some(univ),
                        available,
                        ..
                    } => {
                        if let Some(cl_of) = client_m.get(t) {
                            let h: HashSet<&[u8]> = cl_of
                                .available
                                .iter()
                                .map(Vec::as_slice)
                                .chain(available.iter().map(Vec::as_slice))
                                .collect();
                            h.len() == univ.len()
                        } else {
                            available.len() == univ.len()
                        }
                    }
                    _ => false,
                }
            });
            if impls_matched {
                Ok((client, server))
            } else {
                Err(eyre!("impls_matched: {:?}", ()))
            }
        })
        .collect();

    if pairs.iter().all(|r| r.is_err()) {
        let mut report = eyre!("No remaining valid (client, server) offer pairs");

        for p in pairs.into_iter() {
            report = report.wrap_err(p.unwrap_err())
        }

        Err(report)
    } else {
        let filtered_pairs = pairs.into_iter().filter_map(|r| r.ok()).collect();
        Ok(PickResult {
            filtered_pairs,
            ..pr
        })
    }
}

fn check_touched_partial<T: Pick>(
    t: T,
    pairs: Vec<(StackNonce, StackNonce)>,
) -> Result<PickResult<T::Picked>, Report>
where
    T::Picked: Debug,
{
    let pr = t
        .pick(pairs.clone())
        .wrap_err_with(|| eyre!("pick failed: {:?}", pairs))?;
    let touched = &pr.touched_cap_guids;
    let pairs: Vec<_> = pr
        .filtered_pairs
        .into_iter()
        .map(|(client, server)| {
            let client_m = &client.0;
            let server_m = &server.0;

            let impls_matched = touched.iter().all(|t| {
                // if the server has touched something not available on the client, that is invalid.
                match client_m.get(t) {
                    Some(Offer { .. }) => (),
                    None => return false,
                }

                // for all the things we did touch, the impls should match.
                // We cannot check exhaustively (we might be mid-stack), so don't error on a
                // missing entry on the server side
                match server_m.get(t) {
                    Some(Offer { impl_guid, .. }) if *impl_guid == 0 => true,
                    Some(Offer {
                        sidedness: Some(univ),
                        available,
                        ..
                    }) => {
                        if let Some(cl_of) = client_m.get(t) {
                            let h: HashSet<&[u8]> = cl_of
                                .available
                                .iter()
                                .map(Vec::as_slice)
                                .chain(available.iter().map(Vec::as_slice))
                                .collect();
                            h.len() == univ.len()
                        } else {
                            available.len() == univ.len()
                        }
                    }
                    Some(_) => false,
                    None => true,
                }
            });
            if impls_matched {
                Ok((client, server))
            } else {
                Err(eyre!("impls_matched: {:?}", ()))
            }
        })
        .collect();

    if pairs.iter().all(|r| r.is_err()) {
        let mut report = eyre!("No remaining valid (client, server) offer pairs");

        for p in pairs.into_iter() {
            report = report.wrap_err(p.unwrap_err())
        }

        Err(report)
    } else {
        let filtered_pairs = pairs.into_iter().filter_map(|r| r.ok()).collect();
        Ok(PickResult {
            filtered_pairs,
            ..pr
        })
    }
}

impl<T1, T2, Inner, E> Pick for Select<T1, T2, Inner>
where
    T1: Pick,
    T2: Pick,
    <T1 as Pick>::Picked: Debug,
    <T2 as Pick>::Picked: Debug,
    Inner: MakeEither<T1::Picked, T2::Picked, Either = E> + MakeEither<T2::Picked, T1::Picked>,
    <Inner as MakeEither<T2::Picked, T1::Picked>>::Either: FlipEither<Flipped = E>,
{
    type Picked = E;

    fn pick(
        self,
        offer_pairs: Vec<(StackNonce, StackNonce)>,
    ) -> Result<PickResult<Self::Picked>, Report> {
        fn pick_in_preference_order<T1, T2, Inner>(
            first_pick: T1,
            second_pick: T2,
            offer_pairs: Vec<(StackNonce, StackNonce)>,
        ) -> Result<PickResult<Inner::Either>, Report>
        where
            T1: Pick,
            T2: Pick,
            <T1 as Pick>::Picked: Debug,
            <T2 as Pick>::Picked: Debug,
            Inner: MakeEither<T1::Picked, T2::Picked>,
        {
            let first_err = match check_touched_partial(first_pick, offer_pairs.clone()) {
                Ok(PickResult {
                    stack,
                    filtered_pairs,
                    touched_cap_guids,
                }) if !filtered_pairs.is_empty() => {
                    return Ok(PickResult {
                        stack: Inner::left(stack),
                        filtered_pairs,
                        touched_cap_guids,
                    });
                }
                Ok(_) => eyre!("first choice pick left no options"),
                Err(e) => e.wrap_err("first choice pick erred"),
            };

            match check_touched_partial(second_pick, offer_pairs) {
                Ok(PickResult {
                    stack,
                    filtered_pairs,
                    touched_cap_guids,
                }) if !filtered_pairs.is_empty() => Ok(PickResult {
                    stack: Inner::right(stack),
                    filtered_pairs,
                    touched_cap_guids,
                }),
                Ok(_) => Err(eyre!("both select sides not satisfied").wrap_err(first_err)),
                Err(e) => Err(e.wrap_err("second choice pick erred").wrap_err(first_err)),
            }
        }

        match self.prefer {
            Either::Left(_) => {
                pick_in_preference_order::<T1, T2, Inner>(self.left, self.right, offer_pairs)
            }
            Either::Right(_) => {
                let PickResult {
                    stack,
                    filtered_pairs,
                    touched_cap_guids,
                } = pick_in_preference_order::<T2, T1, Inner>(self.right, self.left, offer_pairs)?;
                Ok(PickResult {
                    stack: stack.flip(),
                    filtered_pairs,
                    touched_cap_guids,
                })
            }
        }
    }
}
