# Stage 1: build the wheel
FROM registry.access.redhat.com/ubi9/python-312-minimal@sha256:b99faaadaf10de740d60c713a047088df2f46ebdd766e9c6a0957896f44d0ea1 as builder

USER 0
WORKDIR /build

COPY pyproject.toml .
COPY src/ src/

RUN chown -R 1001:0 /build
USER 1001

RUN pip install --no-cache-dir build \
 && python -m build --wheel --outdir /build/dist

# Stage 2: minimal runtime image
FROM registry.access.redhat.com/ubi9/python-312-minimal@sha256:b99faaadaf10de740d60c713a047088df2f46ebdd766e9c6a0957896f44d0ea1

WORKDIR /opt/import-orchestrator

# git is required at runtime by the python-sdist ecosystem's canonical-source
# resolver (import_orchestrator.ecosystems.python_sdist.canonical_source), which
# shallow-clones the calunga-python build-config repo for every trigger.
USER 0
RUN microdnf install -y git-core \
 && microdnf clean all
USER 1001

COPY --from=builder /build/dist/*.whl /tmp/

RUN pip install --no-cache-dir /tmp/*.whl \
 && rm -rf /tmp/*.whl

COPY tekton/ tekton/

# Git source identity of the embedded pipeline definition. The wheel has no
# .git to query at runtime, so CI bakes these in from the built commit.
ARG PIPELINE_GIT_URL
ARG PIPELINE_GIT_REVISION

ENV TEKTON_PIPELINE_DIR=/opt/import-orchestrator/tekton \
    PIPELINE_GIT_URL=${PIPELINE_GIT_URL} \
    PIPELINE_GIT_REVISION=${PIPELINE_GIT_REVISION}

ENTRYPOINT ["orchestrator"]
