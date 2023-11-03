#!/bin/bash

DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )/../../

### AUX BUILD DIGESTS ###
# Source files hash:
SOURCE_DIGEST=$(find $DIR/src $DIR/contrib/*-cmake $DIR/cmake $DIR/base $DIR/programs $DIR/packages  -type f  | grep -vE '*.md$' | xargs md5sum | awk '{ print $1 }' | sort | md5sum | awk '{ print $1 }')
# Modules hash:
#   git submodule status cmd works regardeles wether modules are cloned or not, drop possible +/- sign as it shows the status (cloned/ not cloned/ changed files) and may vary
MODULES_DIGEST=$(cd $DIR; git submodule status | awk '{ print $1 }' | sed 's/^[ +-]//' | md5sum | awk '{ print $1 }')
# clickhouse/binary-builder
DOCKER_BUILDER_DIGEST=$(find $DIR/docker/packager/ $DIR/docker/test/util $DIR/docker/test/base -type f | grep -vE '*.md$' | xargs md5sum | awk '{ print $1 }' | sort | md5sum | awk '{ print $1 }') # | cut -c 1-8


### AUX TESTS DIGESTS ###
STATELESS_TEST_SRC_DIGEST=$(find $DIR/tests/queries/0_stateless/ -type f | grep -vE '*.md$' | xargs md5sum | awk '{ print $1 }' | sort | md5sum | awk '{ print $1 }')
STATEFUL_TEST_SRC_DIGEST=$(find $DIR/tests/queries/1_stateful/ -type f | grep -vE '*.md$' | xargs md5sum | awk '{ print $1 }' | sort | md5sum | awk '{ print $1 }')
STATELESS_TEST_DOCKER_DIGEST=$(find $DIR/docker/test/stateless $DIR/docker/test/base -type f | grep -vE '*.md$' | xargs md5sum | awk '{ print $1 }' | sort | md5sum | awk '{ print $1 }')
STATEFUL_TEST_DOCKER_DIGEST=$(find $DIR/docker/test/stateful $DIR/docker/test/stateless $DIR/docker/test/base -type f | grep -vE '*.md$' | xargs md5sum | awk '{ print $1 }' | sort | md5sum | awk '{ print $1 }')
STRESS_TEST_DOCKER_DIGEST=$(find $DIR/docker/test/stress $DIR/docker/test/stateful $DIR/docker/test/stateless $DIR/docker/test/base -type f | grep -vE '*.md$' | xargs md5sum | awk '{ print $1 }' | sort | md5sum | awk '{ print $1 }')
FAST_TEST_DOCKER_DIGEST=$(find $DIR/docker/test/fasttest $DIR/docker/test/util -type f | grep -vE '*.md$' | xargs md5sum | awk '{ print $1 }' | sort | md5sum | awk '{ print $1 }')

INTEGRATION_TEST_SRC_DIGEST=$(find $DIR/tests/integration/ -type f | grep -vE '*.md$' | xargs md5sum | awk '{ print $1 }' | sort | md5sum | awk '{ print $1 }')

### DOCKER DIGESTS ###
# all docker code digest
# DOCKER_DIGEST=$(find $DIR/docker/ -type f | grep -vE '*.md$' | xargs md5sum | awk '{ print $1 }' | sort | md5sum | awk '{ print $1 }' | cut -c 1-8)
# echo "DOCKER_DIGEST=${DOCKER_DIGEST}"
# export DOCKER_DIGEST=$DOCKER_DIGEST
# # clickhouse/test-util
# DOCKER_TEST_UTIL_DIGEST=$(find $DIR/docker/test/util -type f | grep -vE '*.md$' | xargs md5sum | awk '{ print $1 }' | sort | md5sum | awk '{ print $1 }' | cut -c 1-8)
# echo "DOCKER_TEST_UTIL_DIGEST=${DOCKER_TEST_UTIL_DIGEST}"
# export DOCKER_TEST_UTIL_DIGEST=$DOCKER_TEST_UTIL_DIGEST
# # clickhouse/test-base
# DOCKER_TEST_BASE_DIGEST=$(find $DIR/docker/test/base -type f | grep -vE '*.md$' | xargs md5sum | awk '{ print $1 }' | sort | md5sum | awk '{ print $1 }' | cut -c 1-8)
# echo "DOCKER_TEST_BASE_DIGEST=${DOCKER_TEST_BASE_DIGEST}"
# export DOCKER_TEST_BASE_DIGEST=$DOCKER_TEST_BASE_DIGEST

### sanity check
[ -z $SOURCE_DIGEST ] || [ -z $MODULES_DIGEST ] || [ -z $DOCKER_BUILDER_DIGEST ] && echo "ERROR" && exit 1

### FINAL BUILD DIGEST
BUILD_DIGEST=$(echo $SOURCE_DIGEST-$MODULES_DIGEST-$DOCKER_BUILDER_DIGEST | md5sum | awk '{ print $1 }' | cut -c 1-12)
echo "BUILD_DIGEST=${BUILD_DIGEST}"
export BUILD_DIGEST=$BUILD_DIGEST

### ALL DIGEST
ALL_DIGEST=$(find $DIR/  -type f -not -path $DIR/.git | xargs md5sum | awk '{ print $1 }' | sort | md5sum | awk '{ print $1 }' | cut -c 1-12)
echo "ALL_DIGEST=${ALL_DIGEST}"
export ALL_DIGEST=$ALL_DIGEST

### FINAL TEST DIGESTs
STATELESS_TEST_DIGEST=$(echo $STATELESS_TEST_SRC_DIGEST-$STATELESS_TEST_DOCKER_DIGEST | md5sum | awk '{ print $1 }' | cut -c 1-12)
STATEFUL_TEST_DIGEST=$(echo $STATEFUL_TEST_SRC_DIGEST-$STATEFUL_TEST_DOCKER_DIGEST | md5sum | awk '{ print $1 }' | cut -c 1-12)
echo STATELESS_TEST_DIGEST=$STATELESS_TEST_DIGEST
echo STATEFUL_TEST_DIGEST=$STATEFUL_TEST_DIGEST
FAST_TEST_DIGEST=$(echo $STATELESS_TEST_DIGEST-$FAST_TEST_DOCKER_DIGEST | md5sum | awk '{ print $1 }' | cut -c 1-12)
echo FAST_TEST_DIGEST=$FAST_TEST_DIGEST
INTEGRATION_TEST_DIGEST=$(echo $INTEGRATION_TEST_SRC_DIGEST-$DOCKER_DIGEST | md5sum | awk '{ print $1 }' | cut -c 1-12)
echo INTEGRATION_TEST_DIGEST=$INTEGRATION_TEST_DIGEST
STRESS_TEST_DIGEST=$(echo $STATELESS_TEST_DIGEST-$STATEFUL_TEST_DIGEST-$STRESS_TEST_DOCKER_DIGEST | md5sum | awk '{ print $1 }' | cut -c 1-12)
echo STRESS_TEST_DIGEST=$STRESS_TEST_DIGEST
