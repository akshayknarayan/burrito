FROM rust:1.39

WORKDIR /app
ADD . .
RUN rustup override set nightly
RUN rustup component add rustfmt
RUN cargo +nightly build --release

ENTRYPOINT ["cargo", "run", "--release", "--bin"]
CMD ["server", "--", "--addr", "0.0.0.0:4242"]
