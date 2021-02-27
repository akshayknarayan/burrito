//! Provides ordering semantics for data pieces in "rounds".
//!
//! Let's imagine we are playing Overcooked. We have a list of dishes to prepare, and they must
//! be cooked in that order. However, each dish involves multiple steps, and those steps (chopping
//! vegetables, etc) can be done in any order.
//!
//! Or, administering vaccines in phases: the order of people within a phase (65+, etc) is
//! unimportant, but we want ordering between the phases.
//!
//! So, say we have dishes A, B, C, ..., and dish D has components D1, D2, ..., Dn. The following
//! is a valid order: `A3, A1, A2, B1, B3, B2, C2, C1, C3`. One way to enforce this is to implement
//! a total ordering, like: `A1, A2, A3, B1, B2, B3, ...`, but the semantics we care about are more
//! relaxed.

use bertha::{Chunnel, ChunnelConnection};
use color_eyre::eyre::Report;
use dashmap::DashMap;
use futures_util::future::{ready, Ready};
use std::collections::HashMap;
use std::collections::VecDeque;
use std::future::Future;
use std::hash::Hash;
use std::pin::Pin;
use std::sync::Arc;
use tracing::{trace, trace_span};
use tracing_futures::Instrument;

/// inner semantics: at-least-once, unordered delivery
/// outer semantics: at-most-once, round-ordered delivery
#[derive(Debug, Clone)]
pub struct RoundOrderChunnel;

impl<A, D, InC> Chunnel<InC> for RoundOrderChunnel
where
    InC: ChunnelConnection<Data = (A, RoundOrderMsg<D>)> + Send + Sync + 'static,
    A: Eq + Hash + Clone + std::fmt::Debug + Send + Sync + 'static,
    D: Send + Sync + 'static,
{
    type Future = Ready<Result<Self::Connection, Self::Error>>;
    type Connection = RoundOrderCn<InC, A, D>;
    type Error = std::convert::Infallible;

    fn connect_wrap(&mut self, cn: InC) -> Self::Future {
        unimplemented!()
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct RoundOrderMsg<D> {
    round_number: u32,
    round_messages: u32,
    msg_number: u32,
    payload: D,
}

struct State<D> {
    current_round: u32,
    current_round_delivered: Vec<u32>,
    // round id -> list of received round messages. Use VecDeque as a best-effort preservation of
    // inner ordering, even though it's not semantically necessary.
    active_rounds: HashMap<u32, VecDeque<RoundOrderMsg<D>>>,
}

impl<D> Default for State<D> {
    fn default() -> Self {
        State {
            current_round: 0,
            current_round_delivered: Default::default(),
            active_rounds: Default::default(),
        }
    }
}

pub struct RoundOrderCn<C, A, D> {
    inner: Arc<C>,
    state: Arc<DashMap<A, State<D>>>,
}

impl<C, A, D> From<C> for RoundOrderCn<C, A, D>
where
    A: Eq + Hash + Clone + std::fmt::Debug + Send + Sync + 'static,
{
    fn from(inner: C) -> Self {
        Self {
            inner: Arc::new(inner),
            state: Default::default(),
        }
    }
}

impl<C, A, D> ChunnelConnection for RoundOrderCn<C, A, D>
where
    C: ChunnelConnection<Data = (A, RoundOrderMsg<D>)> + Send + Sync + 'static,
    A: Eq + Hash + Clone + std::fmt::Debug + Send + Sync + 'static,
    D: Send + Sync + 'static,
{
    type Data = (A, RoundOrderMsg<D>);

    fn send(
        &self,
        d: Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'static>> {
        self.inner.send(d)
    }

    fn recv(&self) -> Pin<Box<dyn Future<Output = Result<Self::Data, Report>> + Send + 'static>> {
        let inner = Arc::clone(&self.inner);
        let state = Arc::clone(&self.state);
        Box::pin(
            async move {
                // check for saved current-round messages.
                for mut map_ref in state.iter_mut() {
                    let (from_addr, st) = map_ref.pair_mut();
                    trace!(?from_addr, "checking pending ordered messages");
                    match st.active_rounds.get_mut(&st.current_round) {
                        // will be None if no messages in the round have arrived.
                        // will be empty if all the messages we got, have already been delivered.
                        Some(msgs) if !msgs.is_empty() => {
                            let msg = msgs.pop_front().unwrap();
                            assert!(!st.current_round_delivered.contains(&msg.msg_number));
                            st.current_round_delivered.push(msg.msg_number);
                            trace!(?from_addr, ?st.current_round, "returning pending message");
                            return Ok((from_addr.clone(), msg));
                        }
                        _ => (),
                    }
                }

                loop {
                    trace!("calling inner recv");
                    let (from, msg) = inner.recv().await?;
                    trace!(?from, ?msg.round_number, ?msg.msg_number, "got inner recv");
                    let State {
                        ref mut current_round,
                        ref mut current_round_delivered,
                        ref mut active_rounds,
                    } = *state.entry(from.clone()).or_default();
                    if msg.round_number < *current_round {
                        continue; // this is an old message
                    }

                    // if the message is part of the current round, we're going to return it.
                    if *current_round == msg.round_number {
                        // if this is the last message in the round, advance the round.
                        // current_round_delivered is the number of round messages already
                        // delivered, +1 since we are delivering one now.
                        if current_round_delivered.len() + 1 == msg.round_messages as usize {
                            active_rounds.remove(current_round);
                            *current_round += 1;
                            *current_round_delivered = Vec::new();
                        } else {
                            // otherwise, if not already delivered, add this message into the
                            // current round's delivered list. if already delivered, don't
                            // re-deliver and go to the next message.
                            if current_round_delivered.contains(&msg.msg_number) {
                                continue;
                            } else {
                                current_round_delivered.push(msg.msg_number);
                            }
                        }

                        return Ok((from, msg));
                    } else {
                        // got a message from a future round. save it.
                        let msg_round = active_rounds
                            .entry(msg.round_number)
                            .or_insert_with(VecDeque::new);
                        if !msg_round.iter().any(|m| m.round_number == msg.round_number) {
                            msg_round.push_back(msg);
                        }
                    }
                }
            }
            .instrument(trace_span!("round_order_recv")),
        )
    }
}
