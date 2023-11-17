# Horaemeta

[![codecov](https://codecov.io/gh/CeresDB/horaemeta/branch/main/graph/badge.svg?token=VTYXEAB2WU)](https://codecov.io/gh/CeresDB/horaemeta)
![License](https://img.shields.io/badge/license-Apache--2.0-green.svg)

Horaemeta is the meta service for managing the HoraeDB cluster.

## Status
The project is in a very early stage.

## Quick Start
### Build Horaemeta binary
```bash
make build
```

### Standalone Mode
Although Horaemeta is designed to deployed as a cluster with three or more instances, it can also be started standalone:
```bash
# Horaemeta0
mkdir /tmp/horaemeta0
./bin/horaemeta-server --config ./config/example-standalone.toml
```

### Cluster mode
Here is an example for starting Horaemeta in cluster mode (three instances) on single machine by using different ports:
```bash
# Create directories.
mkdir /tmp/horaemeta0
mkdir /tmp/horaemeta1
mkdir /tmp/horaemeta2

# Horaemeta0
./bin/horaemeta-server --config ./config/exampl-cluster0.toml

# Horaemeta1
./bin/horaemeta-server --config ./config/exampl-cluster1.toml

# Horaemeta2
./bin/horaemeta-server --config ./config/exampl-cluster2.toml
```

## Acknowledgment
Horaemeta refers to the excellent project [pd](https://github.com/tikv/pd) in design and some module and codes are forked from [pd](https://github.com/tikv/pd), thanks to the TiKV team.

## Contributing
The project is under rapid development so that any contribution is welcome.
Check our [Contributing Guide](https://github.com/CeresDB/horaemeta/blob/main/CONTRIBUTING.md) and make your first contribution!

## License
Horaemeta is under [Apache License 2.0](./LICENSE).
