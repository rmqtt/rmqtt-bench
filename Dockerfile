FROM pingcap/alpine-glibc
LABEL maintainer="rmqtt <rmqttd@126.com>"
COPY target/x86_64-unknown-linux-musl/release/rmqtt-bench /
ENTRYPOINT ["/rmqtt-bench"]
# CMD [""]
