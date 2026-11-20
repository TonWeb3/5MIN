FROM python:3.12-slim

WORKDIR /app

# Install system dependencies + Rust toolchain for compiling native rust_clob
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable --profile minimal

ENV PATH="/root/.cargo/bin:${PATH}"

# Pre-create logs directory
RUN mkdir -p /app/logs

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Build and install native Rust extension (rust_clob) for Linux
COPY rust_clob/Cargo.toml rust_clob/Cargo.lock rust_clob/pyproject.toml rust_clob/auto_rust_backend.py ./rust_clob/
COPY rust_clob/src ./rust_clob/src
RUN pip install --no-cache-dir --no-build-isolation ./rust_clob \
    && rm -rf /app/rust_clob/target /root/.cargo/registry /root/.cargo/git

COPY . .

# Remove any Windows-only binary copied from local dev and verify Linux native module loads
RUN rm -f /app/rust_clob/*.pyd && rm -rf /app/rust_clob/target \
    && python -c "import rust_clob; assert hasattr(rust_clob, 'post_order')"

# Environment variables
ENV PORT=8000
ENV SYMBOL=BTCUSDT
ENV POLL_INTERVAL_MS=1000

EXPOSE 8000

# Use gunicorn with uvicorn workers for production stability
CMD gunicorn -w 1 -k uvicorn.workers.UvicornWorker main:app --bind 0.0.0.0:$PORT
