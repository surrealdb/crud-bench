#!/usr/bin/env bash
# ============================================================
# tikv.sh - Manage TiKV + PD containers for SurrealDB benchmarks
#
# Usage:
#   ./scripts/tikv.sh start       Start PD + TiKV containers
#   ./scripts/tikv.sh stop        Stop and remove containers
#   ./scripts/tikv.sh status      Show container status
#   ./scripts/tikv.sh clean       Stop containers and remove data
#
# The PD endpoint is exposed at 127.0.0.1:2379.
# SurrealDB connects via tikv://127.0.0.1:2379
# ============================================================
set -euo pipefail

ACTION="${1:-help}"
DATADIR="${TIKV_DATADIR:-$(pwd)/tmp/tikv}"
PD_NAME="crud-bench-pd"
TIKV_NAME="crud-bench-tikv"
PD_IMAGE="pingcap/pd:latest"
TIKV_IMAGE="pingcap/tikv:latest"

start() {
    echo "Starting TiKV cluster..."

    # Create data directories
    mkdir -p "$DATADIR/pd" "$DATADIR/tikv"

    # Stop any existing containers
    docker rm -f "$PD_NAME" "$TIKV_NAME" 2>/dev/null || true

    # Start PD (Placement Driver)
    echo "Starting PD..."
    docker run -d \
        --name "$PD_NAME" \
        --net host \
        --user "$(id -u):$(id -g)" \
        --ulimit nofile=1048576:1048576 \
        -v "$DATADIR/pd:/data" \
        "$PD_IMAGE" \
        --name=pd1 \
        --data-dir=/data \
        --client-urls=http://0.0.0.0:2379 \
        --peer-urls=http://0.0.0.0:2380 \
        --advertise-client-urls=http://127.0.0.1:2379 \
        --advertise-peer-urls=http://127.0.0.1:2380

    # Wait for PD to be ready
    echo "Waiting for PD to be ready..."
    for i in $(seq 1 30); do
        if curl -s http://127.0.0.1:2379/pd/api/v1/health >/dev/null 2>&1; then
            echo "PD is ready."
            break
        fi
        if [ "$i" -eq 30 ]; then
            echo "ERROR: PD failed to start within 30 seconds"
            docker logs "$PD_NAME" 2>&1 | tail -20
            exit 1
        fi
        sleep 1
    done

    # Start TiKV
    echo "Starting TiKV..."
    docker run -d \
        --name "$TIKV_NAME" \
        --net host \
        --user "$(id -u):$(id -g)" \
        --ulimit nofile=1048576:1048576 \
        -v "$DATADIR/tikv:/data" \
        "$TIKV_IMAGE" \
        --addr=0.0.0.0:20160 \
        --advertise-addr=127.0.0.1:20160 \
        --data-dir=/data \
        --pd=http://127.0.0.1:2379

    # Wait for TiKV to be ready
    echo "Waiting for TiKV to be ready..."
    for i in $(seq 1 60); do
        STORES=$(curl -s http://127.0.0.1:2379/pd/api/v1/stores 2>/dev/null | grep -c '"state_name": "Up"' || true)
        if [ "$STORES" -ge 1 ]; then
            echo "TiKV is ready ($STORES store(s) up)."
            break
        fi
        if [ "$i" -eq 60 ]; then
            echo "ERROR: TiKV failed to start within 60 seconds"
            docker logs "$TIKV_NAME" 2>&1 | tail -20
            exit 1
        fi
        sleep 1
    done

    echo ""
    echo "TiKV cluster is running."
    echo "  PD endpoint:   http://127.0.0.1:2379"
    echo "  TiKV endpoint: 127.0.0.1:20160"
    echo "  SurrealDB use: tikv://127.0.0.1:2379"
    echo ""
}

stop() {
    echo "Stopping TiKV cluster..."
    docker rm -f "$TIKV_NAME" 2>/dev/null || true
    docker rm -f "$PD_NAME" 2>/dev/null || true
    echo "TiKV cluster stopped."
}

status() {
    echo "TiKV cluster status:"
    echo ""
    for name in "$PD_NAME" "$TIKV_NAME"; do
        STATE=$(docker inspect -f '{{.State.Status}}' "$name" 2>/dev/null || echo "not running")
        echo "  $name: $STATE"
    done
    echo ""
    # Check PD health
    if curl -s http://127.0.0.1:2379/pd/api/v1/health >/dev/null 2>&1; then
        STORES=$(curl -s http://127.0.0.1:2379/pd/api/v1/stores 2>/dev/null | grep -c '"state_name": "Up"' || echo "0")
        echo "  PD healthy, $STORES TiKV store(s) up"
    else
        echo "  PD not reachable"
    fi
    echo ""
}

clean() {
    stop
    echo "Removing TiKV data at $DATADIR..."
    # Data may have been written as root by earlier runs; use docker to clean if needed
    if [ -d "$DATADIR" ]; then
        rm -rf "$DATADIR" 2>/dev/null || \
            docker run --rm -v "$DATADIR:/data" alpine rm -rf /data/pd /data/tikv
        rm -rf "$DATADIR" 2>/dev/null || true
    fi
    echo "TiKV data removed."
}

case "$ACTION" in
    start)  start ;;
    stop)   stop ;;
    status) status ;;
    clean)  clean ;;
    *)
        echo "Usage: $0 {start|stop|status|clean}"
        echo ""
        echo "  start   Start PD + TiKV containers"
        echo "  stop    Stop and remove containers"
        echo "  status  Show container status"
        echo "  clean   Stop containers and remove data"
        exit 1
        ;;
esac
