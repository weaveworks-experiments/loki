#!/bin/sh

set -eu

SRC_PATH=$GOPATH/src/github.com/weaveworks-experiments/loki

# If we run make directly, any files created on the bind mount
# will have awkward ownership.  So we switch to a user with the
# same user and group IDs as source directory.  We have to set a
# few things up so that sudo works without complaining later on.
uid=$(stat --format="%u" $SRC_PATH)
gid=$(stat --format="%g" $SRC_PATH)
echo "loki:x:$uid:$gid::$SRC_PATH:/bin/sh" >>/etc/passwd
echo "loki:*:::::::" >>/etc/shadow
echo "loki	ALL=(ALL)	NOPASSWD: ALL" >>/etc/sudoers

su loki -c "PATH=$PATH make -C $SRC_PATH BUILD_IN_CONTAINER=false $*"
