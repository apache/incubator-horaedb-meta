# Ceresmeta

![License](https://img.shields.io/badge/license-Apache--2.0-green.svg)

CeresMeta is the meta service for managing the CeresDB cluster.

## Status
The project is in a very early stage.

## Run
A simple cluster with three nodes can be run in the single machine.

Prepare the data directories:
```bash
mkdir /tmp/ceresmeta0 /tmp/ceresmeta1 /tmp/ceresmeta2
```

Run the three commands in three terminals:
```bash
./ceresmeta -etcd-start-timeout-ms 1000000 -peer-urls "http://127.0.0.1:2380" -advertise-client-urls "http://127.0.0.1:2379" -advertise-peer-urls "http://127.0.0.1:2380" -client-urls "http://127.0.0.1:2379" -wal-dir /tmp/ceresmeta0/wal -data-dir /tmp/ceresmeta0/data -node-name "meta0"  -etcd-log-file /tmp/ceresmeta0/etcd.log -initial-cluster "meta0=http://127.0.0.1:2380,meta1=http://127.0.0.1:12380,meta2=http://127.0.0.1:22380

./ceresmeta -etcd-start-timeout-ms 1000000 -peer-urls "http://127.0.0.1:12380" -advertise-client-urls "http://127.0.0.1:12379" -advertise-peer-urls "http://127.0.0.1:12380" -client-urls "http://127.0.0.1:12379" -wal-dir /tmp/ceresmeta1/wal -data-dir /tmp/ceresmeta1/data -node-name "meta1" -etcd-log-file /tmp/ceresmeta1/etcd.log -initial-cluster "meta0=http://127.0.0.1:2380,meta1=http://127.0.0.1:12380,meta2=http://127.0.0.1:22380

./ceresmeta -etcd-start-timeout-ms 1000000 -peer-urls "http://127.0.0.1:22380" -advertise-client-urls "http://127.0.0.1:22379" -advertise-peer-urls "http://127.0.0.1:22380" -client-urls "http://127.0.0.1:22379" -wal-dir /tmp/ceresmeta2/wal -data-dir /tmp/ceresmeta2/data -node-name "meta2" -etcd-log-file /tmp/ceresmeta2/etcd.log -initial-cluster "meta0=http://127.0.0.1:2380,meta1=http://127.0.0.1:12380,meta2=http://127.0.0.1:22380
```

## Acknowledgment
CeresMeta refers to the excellent project [pd](https://github.com/tikv/pd) in design and some module and codes are forked from [pd](https://github.com/tikv/pd), thanks to the TiKV team.

## Contributing
The project is under rapid development so that any contribution is welcome.
Check our [Contributing Guide](https://github.com/CeresDB/ceresmeta/blob/main/CONTRIBUTING.md) and make your first contribution!

## License
CeresMeta is under [Apache License 2.0](./LICENSE).
