# The BlockPulse image for Kubernetes: the CLI and API with exactly the locked dependencies.
# Build from the repository root (.dockerignore keeps data/ out of the context):
#
#   docker build -f infra/k8s/Dockerfile -t blockpulse:dev .
#
# Behind a TLS-inspecting proxy, pass its CA as a build secret (it is not stored in the image):
#
#   docker build --network host --secret id=ca,src=/path/ca.crt -f infra/k8s/Dockerfile -t blockpulse:dev .
#
# Two stages. The build stage installs into a virtual environment; the runtime stage copies only that environment,
# so pip's own files, the lock and the build context never reach the final image.
# Override when Docker Hub is rate-limited, for example --build-arg PYTHON_IMAGE=public.ecr.aws/docker/library/python:3.12-slim
ARG PYTHON_IMAGE=python:3.12-slim

FROM ${PYTHON_IMAGE} AS build

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 PYTHONDONTWRITEBYTECODE=1
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

WORKDIR /src
# Dependencies first, in their own layer: it is rebuilt only when the lock changes, not on every source edit.
# The lock is bind-mounted (never copied into a layer) and pip's download cache is a cache mount (kept between
# builds, never in the image).
RUN --mount=type=bind,source=requirements.lock,target=requirements.lock \
    --mount=type=cache,target=/root/.cache/pip \
    --mount=type=secret,id=ca,required=false \
    if [ -f /run/secrets/ca ]; then export PIP_CERT=/run/secrets/ca SSL_CERT_FILE=/run/secrets/ca; fi; \
    pip install --require-hashes -r requirements.lock

COPY pyproject.toml README.md ./
COPY src ./src
RUN --mount=type=cache,target=/root/.cache/pip \
    --mount=type=secret,id=ca,required=false \
    if [ -f /run/secrets/ca ]; then export PIP_CERT=/run/secrets/ca SSL_CERT_FILE=/run/secrets/ca; fi; \
    pip install --no-deps .


FROM ${PYTHON_IMAGE} AS runtime

LABEL org.opencontainers.image.title="blockpulse" \
      org.opencontainers.image.description="BlockPulse CLI and API" \
      org.opencontainers.image.source="https://github.com/thaitri2005/BlockPulse"

ENV PATH="/opt/venv/bin:$PATH" PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
# An unprivileged user with a fixed numeric id, so Kubernetes can check runAsNonRoot and set volume ownership.
RUN groupadd --system --gid 10001 blockpulse && \
    useradd --system --uid 10001 --gid 10001 --create-home blockpulse
COPY --link --from=build /opt/venv /opt/venv

WORKDIR /home/blockpulse
USER 10001:10001
EXPOSE 8000 9108
ENTRYPOINT ["python", "-m", "blockpulse"]
CMD ["--help"]
