#!/bin/bash

# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0

function process_files {
  for cmd_file in `ls -1`; do
    echo "$cmd_file" | grep -q 'cmd$'
    if [ "$?" -eq "0" ]; then
      stdout_filename=$(echo $cmd_file | sed -e 's/cmd$/stdout/')
      stderr_filename=$(echo $cmd_file | sed -e 's/cmd$/stderr/')
      exit_code_filename=$(echo $cmd_file | sed -e 's/cmd/exitCode/')

      /usr/bin/docker $(cat $cmd_file) > $stdout_filename 2> $stderr_filename
      echo "$?" > $exit_code_filename

      rm $cmd_file
    fi
  done
}

if [ ! -d '/tmp/docker' ]; then
  mkdir /tmp/docker
  chown opensearch:opensearch /tmp/docker
fi

cd /tmp/docker
while true; do
  process_files
  sleep 1
done

