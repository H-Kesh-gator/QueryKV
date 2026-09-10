# QueryKV has zero third-party dependencies (no protobuf, no boost,
# no json library — see README) so this build stage is just a
# compiler and make. That keeps the image small and the build
# reproducible without a package-lock of any kind.

FROM ubuntu:24.04 AS build
RUN apt-get update && apt-get install -y --no-install-recommends g++ make \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY Makefile ./Makefile
COPY include ./include
COPY src ./src
COPY client ./client
RUN make

FROM ubuntu:24.04
RUN useradd -m -u 10001 querykv
WORKDIR /src
COPY --from=build /src/bin/querykv_node /app/querykv_node
COPY --from=build /src/bin/querykv_cli /app/querykv_cli
RUN mkdir -p /data && chown querykv:querykv /data
USER querykv
ENTRYPOINT ["/app/querykv_node"]
