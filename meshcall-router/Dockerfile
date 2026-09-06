FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /meshcall-router ./cmd/meshcall-router

FROM scratch
COPY --from=build /meshcall-router /meshcall-router
USER 65532:65532
EXPOSE 8765
ENTRYPOINT ["/meshcall-router"]
CMD ["--host", "0.0.0.0", "--port", "8765"]
