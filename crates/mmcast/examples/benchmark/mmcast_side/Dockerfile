FROM python:3.12-slim

WORKDIR /app

# Install the in-repo mmbus + mmcast from source.  In a published-wheel
# world this would just be `pip install mmbus mmbus-cast[fastapi]`.
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl build-essential \
    && rm -rf /var/lib/apt/lists/*
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y \
    && . $HOME/.cargo/env

# The maturin build (manifest-path = crates/mmbus/Cargo.toml) needs the
# workspace-root Cargo.toml — the crate manifest uses `version.workspace`
# / `edition.workspace` / `license.workspace`, which only resolve against
# the workspace root.  Cargo.lock gives a reproducible build.
COPY ./Cargo.toml /src/mmbus/Cargo.toml
COPY ./Cargo.lock /src/mmbus/Cargo.lock
COPY ./pyproject.toml /src/mmbus/pyproject.toml
COPY ./crates/mmbus /src/mmbus/crates/mmbus
COPY ./python /src/mmbus/python
COPY ./crates/mmcast /src/mmcast

ENV PATH="/root/.cargo/bin:$PATH"
RUN pip install maturin && \
    pip install /src/mmbus && \
    pip install "/src/mmcast[fastapi]"

COPY ./crates/mmcast/examples/benchmark/mmcast_side/app.py /app/app.py

EXPOSE 8000
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
