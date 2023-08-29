//! Take in messages containing one or more server log lines, and publish messages grouped by
//! client IP, ordered by timestamp.
//!
//! Listen on HTTP/HTTPS/QUIC (with load balancing across multiple instances / threads) and publish
//! on pub/sub chunnel with reconfigurable ordering.

fn main() {
    println!("Hello, world!");
}
