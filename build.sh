#!/bin/sh
set -eu
cd -- "$(dirname -- "$0")"
version=0.0.3
file=Dockerfile
tag=latest
no_cache=
for arg in "$@"; do
  case "$arg" in
    --alpine) file=Dockerfile.alpine; tag=alpine ;;
    --no-cache) no_cache=--no-cache ;;
    *) printf 'Usage: %s [--alpine] [--no-cache]\n' "$0" >&2; exit 2 ;;
  esac
done
if [ "$tag" = alpine ]; then version="$version-alpine"; fi
docker build ${no_cache:+--no-cache} --file "$file" \
  --tag "wxwmoe/media-gateway:$tag" \
  --tag "wxwmoe/media-gateway:$version" .
docker image inspect "wxwmoe/media-gateway:$version" --format '{{.Id}} {{.Size}} bytes'
