FROM cgr.dev/chainguard/glibc-dynamic:latest

ARG TARGETARCH
COPY artifacts/crud-bench-${TARGETARCH}/crud-bench /crud-bench
RUN chmod +x /crud-bench

ENTRYPOINT ["/crud-bench"]
