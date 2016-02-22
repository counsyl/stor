#!/bin/bash

if [ -n SWIFT_TEST_USERNAME ]; then
    echo "Installing hpc smoketest for integration test"
    pip install --no-deps hpc-smoketest==1.0.0
    echo "Running integration test"
    test_out=`test-swift`
    echo $test_out
    exit_code=`echo $test_out | head -n 1 | cut -f1 -d' '`
    exit $exit_code
else
    echo "Ignoring swift integration tests. To run the tests, set the SWIFT_TEST_USERNAME and SWIFT_TEST_PASSWORD environment variables"
fi
