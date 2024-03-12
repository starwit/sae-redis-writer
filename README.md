# SAE-redis-writer
The intention of this stage is to write received SAE messages to a different redis instance.
The following features are planned:
- Aggregate all messages into a single output stream, therefore leaving it to the receiver to filter (this should be feasible, because messages without frame data are magnitudes smaller, see below)
- Remove all frame data for privacy and volume reasons