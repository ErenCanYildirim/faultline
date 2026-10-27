# One Dockerfile for every service
# Pick with --build-arg CMD=<name>, with name as the folder under cmd/

FROM golang:1.24-alpine AS build 
WORKDIR /src 

COPY go.mod go.sum* ./
RUN go mod download 

COPY . .
ARG CMD 
RUN test -n "$CMD" || (echo "build arg CMD is required" && exit 1)
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/app ./cmd/${CMD}
 
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/app /app
USER nonroot:nonroot
ENTRYPOINT ["/app"]
