FROM ubuntu

ARG TARGETARCH
COPY artifacts/crud-bench-${TARGETARCH}/crud-bench /crud-bench

ENTRYPOINT ["/crud-bench"]
