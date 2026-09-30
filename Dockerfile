ARG LLM_WIKI_IMAGE=dotdrelle/llm-wiki
ARG LLM_WIKI_TAG=latest
FROM ${LLM_WIKI_IMAGE}:${LLM_WIKI_TAG}

USER root
WORKDIR /app-production

# pip is build-time only: the server runs straight from dist-packages, so the
# python3-pip package is purged once the dependencies are in — it is neither
# shipped nor reported by image scanners. The base image already went through
# `apt-get upgrade`; this layer upgrades again because it refreshes the index.
RUN apt-get update && \
    apt-get install -y --no-install-recommends python3 python3-pip rsync && \
    apt-get upgrade -y && \
    pip3 install --no-cache-dir --break-system-packages \
      "mcp>=1.9.4,<2" \
      starlette \
      uvicorn && \
    apt-get purge -y python3-pip && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/*
COPY --chmod=644 production_mcp_server.py .

ENV MCP_HOST=0.0.0.0
ENV MCP_PORT=8080
ENV WIKI_WORKSPACE_PATH=/workspace
ENV WORKSPACE_NAME=workspace
ENV PRODUCTION_ALLOWED_STEPS=doctor,doctor_apply,copy,ingest,ingest_plan,ingest_apply,ingest_rebuild,build,export,polish,restore,pipeline,lint
ENV PRODUCTION_REQUIRE_CONFIRMATION=true
ENV PRODUCTION_JOBS_DIR=/workspace/.wiki/production-jobs
ENV PRODUCTION_LOCKS_DIR=/workspace/.wiki/production-jobs/locks

EXPOSE 8080

WORKDIR /workspace
# Back to the base image's unprivileged user: `USER root` above is for the
# package installation only, and would otherwise leak into the runtime.
USER node
ENTRYPOINT ["python3", "/app-production/production_mcp_server.py"]
