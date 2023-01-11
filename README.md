# RMQTT Benchmark Tool

Benchmark for MQTT

## Installation

```rust
git clone https://github.com/rmqtt/rmqtt-bench.git
cd rmqtt-bench
cargo install --path .
```

## Usage

### Help
```rust
$ rmqtt-bench v3 --help

USAGE:
    rmqtt-bench.exe v3 [FLAGS] [OPTIONS]

FLAGS:
        --help       Prints help information
    -S, --sub        Subscribe switch, default value: false
    -P, --pub        Publish switch, default value: false
    -C, --clean      Clean session, default value: false
    -r, --retain     Retain message, default value: false
    -T, --control    Control enable, default value: false
    -V, --version    Prints version information

OPTIONS:
        --addrs <addrs>...                           MQTT broker endpoint list, "host1:port host2:port host3:port" [default: localhost:1883]
    -c, --conns <conns>                              The number of connections [default: 1000]
    -i, --interval <interval>                        Interval of connecting to the broker, millisecond [default: 0]
    -E, --id-pattern <id-pattern>                    Client id pattern, {no} - Connection Serial Number, {random} - The random number [default: {no}]
    -u, --username <username>                        Username
    -p, --password <password>                        Password
    -h, --handshake-timeout <handshake-timeout>      Handshake timeout, Seconds [default: 30]
    -k, --keepalive <keepalive>                      Keepalive, Seconds [default: 60]
    -t, --topic <topic>                              Subscription or Publish Topic pattern, {cid} - Client id, {no} - Connection Serial Number [default: {cid}]
    -q, --qos <qos>                                  QoS, Currently, only 0 and 1 are supported [default: 1]
    -a, --reconn-interval <reconn-interval>          Auto reconnect interval, millisecond [default: 5000]
    -I, --pub-interval <pub-interval>                Publish message interval, millisecond [default: 1000]
    -s, --size <size>                                Publish message length [default: 256]
    -m, --message <message>                          Set the message content for publish
    -l, --max-limit <max-limit>                      Total number of published messages, 0 will not be limited [default: 0]
    -R, --topic-no-range <topic-no-range>...         Publish topic serial number range, format: -R 0 10000

        --lw-msg <lw-message>
        --lw-qos <lw-qos>
        --lw-retain <lw-retain>
        --lw-topic <lw-topic>

    -o, --output-interval <output-interval>          Console output interval, Seconds [default: 5]

    -D, --ctrl-disconn-ratio <ctrl-disconn-ratio>    Disconnected and Reconnection ratio [default: 0.4]
    -L, --ctrl-interval <ctrl-interval>              Control interval, millisecond [default: 1000]

        --ifaddrs <ifaddrs>...                       Local ipaddress, "127.0.0.1 127.0.0.2 127.0.0.3"
```

### Connect Benchmark

For example, create 25K concurrent connections concurrently
```rust
$ rmqtt-bench v3 -c 25000 
```

### Sub Benchmark

For example, create 25K concurrent connections concurrently and subscribe
```rust
$ rmqtt-bench v3 -c 25000 -S -t iot/{no}
```

### Pub Benchmark

For example, create 100 concurrent connections and publish messages concurrently
```rust
$ rmqtt-bench v3 -c 100 -S -t iot/{no} -P
```

For example, 100 concurrent connections are created and 1000 messages are published before exiting
```rust
$ rmqtt-bench v3 -c 100 -S -t iot/{no} -P -l 1000
```

### Sub and Pub Benchmark

For example, create 10K concurrent connections concurrently and subscribe. 
Then create 100 concurrent connections and publish messages to 10K connections
```rust
$ rmqtt-bench v3 -c 10000 -S -t iot/{no}
```
```rust
$ rmqtt-bench v3 -c 100 --id-pattern pub-{no} -P -t iot/{no} -R 0 10000 -I 10
```

### Control Benchmark

Simple control, simulate real scenes.
For example, 1000 concurrent connections are created at the same time, 
and then 20% of the connections are disconnected or reconnected every second
```rust
$ rmqtt-bench v3 -c 1000 -T -L 1000 -D 0.2
```

### Script Benchmark

Simulate real scenes through scripts

```rust

* Planned, please look forward to

```

## Notice


Make sure to increase resource usage limits and expand the port range like following on Linux.

```rust
ulimit -n 500000
sudo sysctl -w net.ipv4.ip_local_port_range="1025 65534"
```

## Author

RMQTT Team.


