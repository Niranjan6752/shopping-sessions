# Shopping Sessions: a Rust pipeline. Build stage compiles; run stage holds only the binary.
FROM rust:1.85-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
# Cache dependencies in their own layer.
RUN mkdir src && echo 'fn main() {}' > src/main.rs && cargo build --release --locked && rm -rf src
COPY src ./src
# Scorers are Intel Xeon (Cascade Lake or newer): AVX2 is always there.
ENV RUSTFLAGS="-C target-cpu=x86-64-v3"
RUN touch src/main.rs && cargo build --release --locked && cp target/release/shopping-sessions /shopping-sessions

FROM debian:bookworm-slim
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY --from=build /shopping-sessions /usr/local/bin/shopping-sessions
USER 1000:1000
CMD ["/usr/local/bin/shopping-sessions"]
