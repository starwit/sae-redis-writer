#!/bin/bash

docker build -t starwitorg/sae-redis-writer:$(poetry version --short) .