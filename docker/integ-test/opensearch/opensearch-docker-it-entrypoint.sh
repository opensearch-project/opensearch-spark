#!/bin/bash

# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0

./docker-command-runner.sh &
echo $! > /var/run/docker-command-runner.pid

su opensearch ./opensearch-docker-entrypoint.sh "$@"

kill -TERM `cat /var/run/docker-command-runner.pid`
