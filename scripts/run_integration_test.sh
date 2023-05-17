#!/usr/bin/env bash

set -exo

mkdir -p $INTEGRATION_TEST_PATH

# Copy Meta Binary
cp ./ceresmeta $INTEGRATION_TEST_PATH

# Download CeresDB Code
cd $INTEGRATION_TEST_PATH
git clone --depth 1 https://github.com/ceresdb/ceresdb.git --branch main

# Replace Build Meta Script
echo > $INTEGRATION_TEST_PATH/ceresdb/integration_tests/build_meta.sh

# Copy Meta Binary to CeresDB Execution Directory
cd ./ceresdb/integration_tests
mkdir ceresmeta
cp ../../ceresmeta ./ceresmeta/ceresmeta

# Run integration_tests
make run-cluster
