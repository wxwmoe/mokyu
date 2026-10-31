#!/bin/sh
set -eu
commit=e5f5a022fe96fc4dc2ee35c19f733a50df807abe
git init -q uhdr
git -C uhdr remote add origin https://github.com/google/libultrahdr.git
git -C uhdr fetch -q --depth=1 origin "$commit"
git -C uhdr checkout -q --detach FETCH_HEAD
test "$(git -C uhdr rev-parse HEAD)" = "$commit"
cmake -S uhdr -B uhdr/build -G Ninja -DCMAKE_BUILD_TYPE=Release -DBUILD_SHARED_LIBS=OFF \
    -DUHDR_BUILD_DEPS=OFF -DUHDR_BUILD_TESTS=OFF -DUHDR_BUILD_EXAMPLES=OFF -DUHDR_ENABLE_GLES=OFF \
    -DUHDR_ENABLE_HEIF=OFF -DUHDR_ENABLE_SMPTE2094_50=OFF \
    -DJPEG_LIBRARY="$PWD/jpegli/build/lib/libjpegli-static.a" \
    -DJPEG_INCLUDE_DIR="$PWD/jpegli/build/lib/include/jpegli" \
    -DCMAKE_CXX_FLAGS="-I$PWD/jpegli/build/lib/include/jpegli"
cmake --build uhdr/build --target uhdr -j4
# Keep the gain-map codec on Jpegli's ABI without replacing the system JPEG decoder.
nm -u uhdr/build/libuhdr.a | awk '$1=="U" && $2~/^jpeg_/ { s=$2; sub(/^jpeg_/,"jpegli_",s);
    if ($2=="jpeg_start_compress") s="image_hdr_start"; print $2,s }' | sort -u > uhdr/symbols
test -s uhdr/symbols
objcopy --redefine-syms=uhdr/symbols uhdr/build/libuhdr.a uhdr/libimage-hdr.a
for file in LICENSE LICENSE-APACHE LICENSE-MIT adobe-hdr-gain-map-license/NOTICE third_party/image_io/LICENSE; do
    install -Dm644 "uhdr/$file" "/opt/mokyu/licenses/ultrahdr/$file"
done
