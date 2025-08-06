#!/bin/bash

docker build -t starwitorg/sae-redis-writer:$(git rev-parse --short HEAD) .