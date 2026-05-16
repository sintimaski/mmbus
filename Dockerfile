FROM rust:1.78-slim-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
        python3 python3-pip python3-venv \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Cache dependency build: copy manifests first, then source.
COPY Cargo.toml Cargo.lock pyproject.toml ./
COPY src/ src/
COPY python/ python/
COPY tests/ tests/

RUN python3 -m venv .venv \
    && .venv/bin/pip install --quiet maturin \
    && .venv/bin/maturin develop --features python

FROM builder AS test
# Default: run Rust tests then Python smoke tests.
CMD ["sh", "-c", \
    "cargo test && \
     python3 -c \"\
import mmbus, threading, sys; \
received=[]; \
def s(): \
  bus=mmbus.Bus('docker-test'); sub=bus.subscribe('ch',timeout_secs=10.0); received.append(sub.recv()); \
t=threading.Thread(target=s,daemon=True); t.start(); \
pub=mmbus.Bus('docker-test'); pub.wait_for_subscribers('ch',n=1,timeout_secs=10.0); \
pub.publish('ch',b'hello-linux'); t.join(3); \
assert received==[b'hello-linux'],f'got {received}'; \
print('eventfd smoke test PASSED'); \
\""]
