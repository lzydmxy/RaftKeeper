#!/usr/bin/env bash

set -x -e

# Special dpkg-deb (https://github.com/ClickHouse-Extras/dpkg) version which is able
# to compress files using pigz (https://zlib.net/pigz/) instead of gzip.
# Significantly increase deb packaging speed and compatible with old systems

counter=0
until curl -O https://clickhouse-builds.s3.yandex.net/utils/1/dpkg-deb
do
    sleep 0.5
    counter=$(($counter + 1))
    echo "Cannot fetch better dpgk, retry $counter"
    if [ "$counter" -gt 120 ]
    then
        echo "Cannot fetch busybox image all retries exceeded"
        exit 1
    fi
done

chmod +x dpkg-deb && cp dpkg-deb /usr/bin

ccache --show-stats ||:
ccache --zero-stats ||:
build/release --no-pbuilder $ALIEN_PKGS | ts '%Y-%m-%d %H:%M:%S'
mv /*.deb /output
mv *.changes /output
mv *.buildinfo /output
mv /*.rpm /output ||: # if exists
mv /*.tgz /output ||: # if exists

if [ -n "$BINARY_OUTPUT" ] && { [ "$BINARY_OUTPUT" = "programs" ] || [ "$BINARY_OUTPUT" = "tests" ] ;}
then
  echo Place $BINARY_OUTPUT to output
  mkdir /output/binary ||: # if exists
  mv /build/obj-*/programs/clickhouse* /output/binary
  if [ "$BINARY_OUTPUT" = "tests" ]
  then
    mv /build/obj-*/src/unit_tests_dbms /output/binary
  fi
fi
ccache --show-stats ||:
ln -s /usr/lib/x86_64-linux-gnu/libOpenCL.so.1.0.0 /usr/lib/libOpenCL.so ||:
