# Builds the worker into a small static image.
FROM golang:1.27.3 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /worker ./cmd/worker

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /worker /worker
USER nonroot:nonroot
ENTRYPOINT ["/worker"]
