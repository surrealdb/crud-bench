FROM cgr.dev/chainguard/glibc-dynamic:latest

ARG TARGETARCH
COPY artifacts/crud-bench-${TARGETARCH}/crud-bench /crud-bench
COPY config/ /config/
ENV CRUD_BENCH_CONFIG=/config/bench.toml

ENTRYPOINT ["/crud-bench"]
