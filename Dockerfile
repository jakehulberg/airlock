# One Dockerfile for both components: --build-arg CMD=orchestrator or CMD=web.
# Cross-compiles on the build host, so multi-arch builds need no emulation.
FROM --platform=$BUILDPLATFORM golang:1.27.2 AS build
ARG CMD
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags="-s -w" -o /out/airlock ./cmd/${CMD:?set --build-arg CMD}

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/airlock /airlock
USER 65532:65532
ENTRYPOINT ["/airlock"]
