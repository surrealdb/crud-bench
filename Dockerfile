FROM cgr.dev/chainguard/glibc-dynamic:latest

COPY target/x86_64-unknown-linux-gnu/release/crud-bench /crud-bench

ENTRYPOINT ["/crud-bench"]
