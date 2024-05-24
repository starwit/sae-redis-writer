# SAE-redis-writer
The intention of this stage is to write received SAE messages to a different redis instance.
The following features are planned:
- Aggregate all messages into a single output stream, therefore leaving it to the receiver to filter (this should be feasible, because messages without frame data are magnitudes smaller, see below)
- Remove all frame data for privacy and volume reasons

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