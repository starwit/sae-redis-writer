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