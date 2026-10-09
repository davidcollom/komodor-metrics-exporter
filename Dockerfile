FROM gcr.io/distroless/static:nonroot
ARG TARGETPLATFORM
COPY $TARGETPLATFORM/komodor-metrics-exporter /komodor-metrics-exporter
EXPOSE 9090
ENTRYPOINT ["/komodor-metrics-exporter"]
