#!/bin/sh
set -eu
libs="vips libheif libjpeg libwebpdemux libwebp libjxl libavif"
version=$(pkg-config --modversion $libs | tr '\n' '/')
stamp=$(cat image.c source.h encode.h hdr.h io.h jpeg.cc dependencies.sh hdr.sh build.sh | sha256sum | cut -c1-16)
install -d /opt/mokyu/bin
cc -O2 -std=c11 -D_GNU_SOURCE -DIMAGE_VERSION="\"$stamp/$version\"" \
    -Ijpegli -Iwebp -Iuhdr $(pkg-config --cflags $libs) -c image.c webp/extras/quality_estimate.c
c++ -O2 -std=c++17 -Ijpegli -Ijpegli/build/lib/include/jpegli $(pkg-config --cflags vips) -c jpeg.cc
c++ image.o jpeg.o quality_estimate.o uhdr/libimage-hdr.a jpegli/build/lib/libjpegli-static.a \
    jpegli/build/third_party/highway/libhwy.a $(pkg-config --libs $libs) -lm -lpthread \
    -o /opt/mokyu/bin/mokyu-image
install -Dm644 jpegli/third_party/highway/LICENSE /opt/mokyu/licenses/highway
install -Dm644 jpegli/third_party/skcms/LICENSE /opt/mokyu/licenses/skcms
install -Dm644 jpegli/third_party/libjpeg-turbo/LICENSE.md /opt/mokyu/licenses/libjpeg-turbo
