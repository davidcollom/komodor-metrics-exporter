FROM golang:1.27 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY main.go ./
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -ldflags "-s -w" -o /komodor-metrics-exporter .

FROM gcr.io/distroless/static:nonroot
COPY --from=build /komodor-metrics-exporter /komodor-metrics-exporter
EXPOSE 9090
ENTRYPOINT ["/komodor-metrics-exporter"]
