ARG LLM_WIKI_IMAGE=dotdrelle/llm-wiki
ARG LLM_WIKI_TAG=latest
FROM ${LLM_WIKI_IMAGE}:${LLM_WIKI_TAG}

USER root
WORKDIR /app-production

RUN apt-get update && \
    apt-get install -y --no-install-recommends python3 python3-pip rsync && \
    rm -rf /var/lib/apt/lists/* && \
    pip3 install --no-cache-dir --break-system-packages \
      "mcp>=1.9.4" \
      starlette \
      uvicorn

COPY production_mcp_server.py .

ENV MCP_HOST=0.0.0.0
ENV MCP_PORT=8080
ENV WIKI_WORKSPACE_PATH=/workspace
ENV WORKSPACE_NAME=workspace
ENV PRODUCTION_ALLOWED_STEPS=doctor,copy,ingest,ingest_plan,ingest_apply,build,export,polish,pipeline
ENV PRODUCTION_REQUIRE_CONFIRMATION=false
ENV PRODUCTION_JOBS_DIR=/workspace/.wiki/production-jobs
ENV PRODUCTION_LOCKS_DIR=/workspace/.wiki/production-jobs/locks

EXPOSE 8080

WORKDIR /workspace
ENTRYPOINT ["python3", "/app-production/production_mcp_server.py"]
