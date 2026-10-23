# Multi-stage build of the standalone agent server.
#
# PORTABILITY: nothing environment-specific lives here — no internal artifact
# mirror, no proxy, no regional package mirror, no *.svc.cluster.local. A CI
# runner and a developer laptop build the same image; whatever a given
# environment needs (a proxy, a corporate module mirror) is passed as a
# build-arg by whoever builds it, and never baked in.
# Pinned to the version go.mod declares. `1.27` does not exist on Docker Hub, and an
# unpinned tag would let the toolchain drift away from what the module requires — the
# build would then accept syntax the declared minimum rejects.
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/easyagent ./cmd/easyagent

FROM alpine:3.24
RUN apk add --no-cache ca-certificates && mkdir -p /data
COPY --from=build /out/easyagent /usr/local/bin/easyagent
ENV PORT=8080 \
    AGENT_HEALTH_ADDR=:8081 \
    DB_BACKEND=sqlite \
    DATABASE_URL=/data/agent.db
EXPOSE 8080 8081
ENTRYPOINT ["/usr/local/bin/easyagent"]
