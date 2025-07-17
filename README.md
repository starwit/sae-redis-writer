# SAE Redis Writer

This component is part of the Starwit Awareness Engine (SAE). See umbrella repo here: https://github.com/starwit/vision-pipeline-k8s

The intention of this stage is to write received messages to a different redis/[valkey](https://valkey.io/) instance. This means data created by SAE can be transfered to a backend.
It transparently forwards all messages it receives, regardless of their type, to the configured Redis instance.
If the configuration option `remove_frame_data` is set and the message sucessfully parses as a `SaeMessage`, all image data will be removed from the frame before forwarding the message.

The following features are planned:
- Aggregate all messages into a single output stream, therefore leaving it to the receiver to filter (this should be feasible, because messages without frame data are magnitudes smaller, see below)

## How to Build

See [dev readme](doc/DEV_README.md) for build instructions.

## TLS
If TLS is enabled in the settings the Redis client will perform mutual TLS with the Redis server. \
You can find a helper script to generate a working set of CA / client / server certs for testing here: \
https://github.com/starwit/starwit-awareness-engine/tree/main/tools/gen-test-certs.sh \

For the setup to work correctly, the following files need be in place:
| File                 | Description                                                 |
| -------------------- | ----------------------------------------------------------- |
| `./certs/client.crt` | The client certificate, signed with `ca.crt`                |
| `./certs/client.key` | The client private key matching `client.crt`                |
| `./certs/ca.crt`     | Certificate Authority that was used to sign the server cert |

## Tests
Run tests by executing `poetry run pytest`. The integration tests require Docker to be available.\
If you use Docker Rootless, you have to set the following variables for testcontainers to use the correct Docker socket:
```bash
export DOCKER_HOST=unix:///run/user/$(id -u)/docker.sock
export TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/run/user/$(id -u)/docker.sock
```

If you want to run the tests through VSCode you have to run the action `Python: Configure Tests` and select `pytest`. The Variables above can be supplied by putting an `.env` file into the project root directory.

## Changelog
### 1.5.0
- Now transparently forwards all messages verbatim, except if `SaeMessage` is detected for a given stream. No configuration change necessary for existing installations.