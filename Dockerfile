FROM python:3.12-slim

WORKDIR /app

# Install system dependencies + Rust toolchain for compiling native rust_clob
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    ca-certificates \
    && curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable --profile minimal \
    && rm -rf /var/lib/apt/lists/*

ENV PATH="/root/.cargo/bin:${PATH}"

# Pre-create logs directory
RUN mkdir -p /app/logs

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Remove Windows-only .pyd, compile & install native Linux rust_clob, copy .so into /app/rust_clob, and verify import
RUN rm -f /app/rust_clob/rust_clob.pyd \
    && rm -rf /app/rust_clob/target \
    && pip install --no-cache-dir --no-build-isolation --no-deps --force-reinstall /app/rust_clob \
    && python -c "import glob, shutil, sysconfig; sp = sysconfig.get_paths()['purelib']; sos = glob.glob(sp + '/rust_clob/**/*.so', recursive=True) + glob.glob(sp + '/rust_clob*.so'); assert sos, 'rust_clob .so not found after build'; [shutil.copy2(s, '/app/rust_clob/') for s in sos]" \
    && python -c "import bot; print('rust_clob verified OK:', bot.rust_clob.__file__)"

# Environment variables
ENV PORT=8000
ENV SYMBOL=BTCUSDT
ENV POLL_INTERVAL_MS=1000

EXPOSE 8000

# Use gunicorn with uvicorn workers for production stability
CMD gunicorn -w 1 -k uvicorn.workers.UvicornWorker main:app --bind 0.0.0.0:$PORT
