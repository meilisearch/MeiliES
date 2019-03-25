FROM rust:1.33.0

COPY . .

RUN cargo build --release

EXPOSE 6480

CMD ["./target/release/meilies-server", "--hostname", "0.0.0.0"]

