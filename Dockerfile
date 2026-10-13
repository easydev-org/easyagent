# Multi-stage build of the standalone agent server.
ARG REGISTRY=docker.io/library
FROM ${REGISTRY}/golang:1.27-alpine AS build
ARG GOPROXY=http://artifact.worker.svc.cluster.local/artifacts/go,https://proxy.golang.org
ARG GOSUMDB=off
ARG HTTP_PROXY
ARG HTTPS_PROXY
ARG NO_PROXY=localhost,127.0.0.1,.svc,.svc.cluster.local,artifact.worker.svc.cluster.local
ENV GOPROXY=${GOPROXY} GOSUMDB=${GOSUMDB} CGO_ENABLED=0 \
    HTTP_PROXY=${HTTP_PROXY} HTTPS_PROXY=${HTTPS_PROXY} NO_PROXY=${NO_PROXY}
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/root/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/root/go/pkg/mod go build -trimpath -ldflags="-s -w" -o /out/easyagent ./cmd/easyagent

FROM ${REGISTRY}/alpine:3.24
RUN apk add --no-cache ca-certificates && mkdir -p /data
COPY --from=build /out/easyagent /usr/local/bin/easyagent
ENV PORT=8080 \
    AGENT_HEALTH_ADDR=:8081 \
    DB_BACKEND=sqlite \
    DATABASE_URL=/data/agent.db
EXPOSE 8080 8081
ENTRYPOINT ["/usr/local/bin/easyagent"]
