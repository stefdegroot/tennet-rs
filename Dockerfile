FROM rust:1.91 AS build-env
WORKDIR /app
COPY . /app
RUN cargo build --release

FROM gcr.io/distroless/cc
COPY --from=build-env /app/target/release/tennet /
VOLUME /data
CMD ["./tennet"]