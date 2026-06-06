ARG GO_VERSION
FROM golang:${GO_VERSION}-alpine AS build
ARG VERSION=dev
ARG REVISION=dev
WORKDIR /src
# ca-certificates so the scratch stage can verify TLS to ghcr.io,
# helm-chart repos, OCI registries, etc.
RUN apk add --no-cache ca-certificates upx
COPY go.mod go.sum ./
RUN go mod download
COPY . ./
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w -X main.version=${VERSION} -X main.commit=${REVISION}" -o /out/flate ./cmd/flate
RUN upx --best --lzma /out/flate

FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/flate /flate
ENTRYPOINT ["/flate"]
