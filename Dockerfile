FROM rust:1.85-slim-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
        python3 python3-pip python3-venv \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Cache dependency build: copy manifests first, then source.
# examples/ and benches/ are required because Cargo.toml declares [[example]]
# and [[bench]] targets — cargo metadata fails otherwise.
COPY Cargo.toml Cargo.lock pyproject.toml ./
COPY src/ src/
COPY python/ python/
COPY tests/ tests/
COPY examples/ examples/
COPY benches/ benches/

RUN python3 -m venv .venv \
    && .venv/bin/pip install --quiet maturin \
    && .venv/bin/maturin develop --features python

FROM builder AS test
# Default: run Rust tests then the Python eventfd smoke test (via the venv
# where mmbus was installed by `maturin develop`).
CMD ["sh", "-c", "cargo test && .venv/bin/python3 python/smoke_test.py"]
