#!/usr/bin/env bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/.."
ID=$1
$DIR/bin/node -config $DIR/conf.yaml -self_id=$ID "$@" &