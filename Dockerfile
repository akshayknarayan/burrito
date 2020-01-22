FROM rust:1.40

WORKDIR /app
ADD . .
RUN rustup component add rustfmt
RUN cargo build --release

ENTRYPOINT ["cargo", "run", "--release", "--bin"]
CMD ["server", "--", "--addr", "0.0.0.0:4242"]
