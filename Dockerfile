ARG GO_VERSION
FROM golang:${GO_VERSION}-alpine AS build
ARG VERSION=dev
ARG REVISION=dev
WORKDIR /src
RUN apk add --no-cache upx
COPY go.mod go.sum ./
RUN go mod download
COPY . ./
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w -X main.version=${VERSION} -X main.commit=${REVISION}" -o /out/flate ./cmd/flate
RUN upx --best --lzma /out/flate

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/flate /flate
ENTRYPOINT ["/flate"]
