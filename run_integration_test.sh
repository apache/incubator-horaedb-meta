#!/usr/bin/env bash

set -exo

integration_test_path="/tmp/integration_tests"
# Clean old cache
rm -rf $integration_test_path
mkdir $integration_test_path

# Copy Build Meta Script
cp -f ./integration_test_build_meta.sh $integration_test_path
cp ./ceresmeta $integration_test_path

# Download CeresDB Code
cd $integration_test_path
git clone --depth 1 https://github.com/ceresdb/ceresdb.git --branch main

# Replace Build Meta Script
cp -f ./integration_test_build_meta.sh $integration_test_path/ceresdb/integration_tests/build_meta.sh

# Run integration_tests
cd ./ceresdb/integration_tests
make run-cluster
