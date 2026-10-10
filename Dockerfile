FROM --platform=$BUILDPLATFORM golang:1.27.2-alpine@sha256:f92b6ef800e499660581efdabdf25d9d817a9d124eaf900924f0504e7e27e12d AS build
ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags "-s -w -X main.version=$VERSION" -o /accessrelay ./cmd/accessrelay
ADD --checksum=sha256:3aef5f6d5061decc6fc4946339b3a61b170bd256f80b4e861194b095df83ec86 https://tar.goaccess.io/goaccess-1.12.tar.gz /goaccess-source.tar.gz
RUN tar -xOf /goaccess-source.tar.gz goaccess-1.12/COPYING > /GoAccess-MIT.txt

FROM docker.io/allinurl/goaccess:1.12@sha256:9e6dfd4abce94bd7c2907c1840b7b35f666587d56b24ffbb930b4782a87ea172
COPY --from=build /accessrelay /accessrelay
COPY --from=build /goaccess-source.tar.gz /usr/share/accessrelay/goaccess/source-1.12.tar.gz
COPY --from=build /GoAccess-MIT.txt /usr/share/accessrelay/goaccess/LICENSE
COPY LICENSE NOTICE LICENSES /usr/share/accessrelay/licenses/
USER 1000:1000
ENV TZ=UTC
EXPOSE 8080
ENTRYPOINT ["/accessrelay"]
CMD ["--config", "/config/accessrelay.json"]
LABEL org.opencontainers.image.source="https://github.com/zekihan/accessrelay" \
      org.opencontainers.image.licenses="AGPL-3.0-or-later AND MIT" \
      org.opencontainers.image.title="accessrelay" \
      org.opencontainers.image.description="Durable VictoriaLogs collection, GoAccess replay and report serving"
