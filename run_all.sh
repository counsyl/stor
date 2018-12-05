#!/usr/bin/env bash

CMD=$1
shift
DIRS=$*
for directory in $DIRS; do
    cd $directory; ${CMD}; cd ..
done