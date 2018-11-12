#!/usr/bin/env bash

CMD=$1
for directory in stor stor_dx stor_swift stor_s3; do
    cd $directory; ${CMD}; cd ..
done