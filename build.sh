#!/bin/sh
set -eu
cd -- "$(dirname -- "$0")"
version=0.0.3
file=Dockerfile
tag=latest
no_cache=
web_ui=true
for arg in "$@"; do
  case "$arg" in
    --alpine) file=Dockerfile.alpine; tag=alpine ;;
    --no-cache) no_cache=--no-cache ;;
    --api-only) web_ui=false ;;
    *) printf 'Usage: %s [--alpine] [--api-only] [--no-cache]\n' "$0" >&2; exit 2 ;;
  esac
done
if [ "$tag" = alpine ]; then version="$version-alpine"; fi
if [ "$web_ui" = false ]; then
  if [ "$tag" = latest ]; then tag=api; else tag="$tag-api"; fi
  version="$version-api"
fi
docker build ${no_cache:+--no-cache} --file "$file" \
  --build-arg "WEB_UI=$web_ui" \
  --tag "wxwmoe/mokyu:$tag" \
  --tag "wxwmoe/mokyu:$version" .
docker image inspect "wxwmoe/mokyu:$version" --format '{{.Id}} {{.Size}} bytes'
