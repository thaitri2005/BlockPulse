# The BlockPulse image for Kubernetes: the CLI and API with exactly the locked dependencies.
# Build from the repository root (.dockerignore keeps data/ out of the context):
#
#   docker build -f infra/k8s/Dockerfile -t blockpulse:dev .
#
# Behind a TLS-inspecting proxy, pass its CA as a build secret (it is not stored in the image):
#
#   docker build --network host --secret id=ca,src=/path/ca.crt -f infra/k8s/Dockerfile -t blockpulse:dev .
FROM python:3.12-slim

ENV PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1 PYTHONUNBUFFERED=1

WORKDIR /opt/blockpulse
# Dependencies first: this layer is rebuilt only when the lock changes, not on every source edit.
COPY requirements.lock ./
RUN --mount=type=secret,id=ca,required=false \
    if [ -f /run/secrets/ca ]; then export PIP_CERT=/run/secrets/ca SSL_CERT_FILE=/run/secrets/ca; fi; \
    pip install --require-hashes -r requirements.lock

COPY pyproject.toml README.md ./
COPY src ./src
RUN --mount=type=secret,id=ca,required=false \
    if [ -f /run/secrets/ca ]; then export PIP_CERT=/run/secrets/ca SSL_CERT_FILE=/run/secrets/ca; fi; \
    pip install --no-deps .

# Run as an unprivileged user; a pod has no reason to be root.
RUN useradd --uid 10001 --create-home blockpulse
USER 10001
ENTRYPOINT ["python", "-m", "blockpulse"]
CMD ["--help"]
