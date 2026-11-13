# syntax=docker/dockerfile:1
FROM rust:1.95.0-trixie AS build
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY proto ./proto
RUN cargo build --locked --release -p sanctum-server

FROM debian:trixie-slim
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /build/target/release/sanctum-server /usr/local/bin/sanctum-server
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/sanctum-server"]
