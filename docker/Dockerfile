FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install deps first for layer caching
COPY framework/pyproject.toml /app/framework/pyproject.toml
COPY framework/py/ /app/framework/py/
RUN pip install --no-cache-dir \
    -e "/app/framework[simulation]" \
    "paho-mqtt>=2.0.0" \
    "flwr-datasets[vision]" \
    torch torchvision --extra-index-url https://download.pytorch.org/whl/cpu

COPY examples/quickstart-pytorch/ /app/examples/quickstart-pytorch/
COPY docker/flwr-config.toml /root/.flwr/config.toml
