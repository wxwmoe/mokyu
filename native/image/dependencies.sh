#!/bin/sh
set -eu
fetch() {
    git init -q "$1"
    git -C "$1" remote add origin "$2"
    git -C "$1" fetch -q --depth=1 origin "$3"
    git -C "$1" checkout -q --detach FETCH_HEAD
    test "$(git -C "$1" rev-parse HEAD)" = "$3"
}
fetch jpegli https://github.com/google/jpegli.git 031a0077f5799a6041004267fc12b956c1f52a20
git -C jpegli submodule update --init --depth=1 third_party/highway third_party/skcms third_party/libjpeg-turbo
cmake -S jpegli -B jpegli/build -G Ninja -DCMAKE_BUILD_TYPE=Release \
    -DBUILD_TESTING=OFF -DBUILD_SHARED_LIBS=OFF -DJPEGLI_ENABLE_SJPEG=OFF \
    -DJPEGLI_ENABLE_DEVTOOLS=OFF -DJPEGLI_ENABLE_BENCHMARK=OFF -DJPEGLI_ENABLE_JNI=OFF
cmake --build jpegli/build --target jpegli-static -j4
install -Dm644 jpegli/LICENSE /opt/mokyu/licenses/jpegli
fetch webp https://github.com/webmproject/libwebp.git 44257cb82623ceedec4c945413b67b7e1d3ff904
install -Dm644 webp/COPYING /opt/mokyu/licenses/webp
