#!/bin/sh
set -eu
: "${MOKYU_TEST_IMAGE:?}" "${MOKYU_TEST_WORKDIR:?}"
test "${MOKYU_TEST_ALLOW_STATE_CHANGES:-}" = isolated-only
cd "$MOKYU_TEST_WORKDIR"
test -z "$(ls -A)"
image=$MOKYU_TEST_IMAGE
container=
trap 'if [ -n "$container" ]; then docker rm -f "$container" >/dev/null; fi' EXIT

run() {
    docker run --network none --memory 512m --cpus 2 "$@"
}

reject() {
    pattern=$1
    shift
    container=$(docker create --network none --memory 512m --cpus 2 "$@")
    docker start "$container" >/dev/null
    status=$(timeout 15 docker wait "$container")
    test "$status" -ne 0
    docker logs "$container" 2>&1 | grep -q "$pattern"
    docker rm "$container" >/dev/null
    container=
}

run --rm --user 0:0 -v ./:/fixture --entrypoint sh "$image" -ec '
    umask 022
    mkdir /fixture/config /fixture/foreign
    mkdir -m 700 /fixture/private
    printf "%s\n" "[database]" "type = \"sqlite\"" "[instance]" \
        "deployment_key = \"$(mokyu tool keygen)\"" > /fixture/config/config.toml
    printf keep > /fixture/foreign/.marker
'

for mode in single testing private; do
    case $mode in
        single) set -- -v ./config:/config:ro "$image" ;;
        *) set -- "$image" testing ;;
    esac
    if [ "$mode" != private ]; then test ! -e "$mode"; fi
    container=$(run -d -v "./$mode:/data" "$@")
    for attempt in $(seq 1 30); do
        if docker exec "$container" mokyu cli status --ready >/dev/null 2>&1; then break; fi
        [ "$(docker inspect -f '{{.State.Running}}' "$container")" = true ] || break
        sleep 1
    done
    docker exec "$container" mokyu cli status --ready >/dev/null || { docker logs "$container"; exit 1; }
    docker exec "$container" sh -ec '
        test -w /data
        test -s /data/mokyu.db
        test "$(id -u)" = 10001
        grep -Eq "^CapEff:[[:space:]]+0+$" /proc/1/status
    '
    docker stop -t 45 "$container" >/dev/null
    docker rm "$container" >/dev/null
    container=
done

reject 'cannot read configuration' -v ./empty-config:/config:ro -v ./unconfigured:/data "$image"
reject 'cannot write data directory' -v ./foreign:/data "$image" testing
reject 'cannot write data directory' -v ./unconfigured:/data:ro "$image" testing
run --rm --user 0:0 -v ./:/fixture:ro --entrypoint sh "$image" -ec '
    test -z "$(ls -A /fixture/empty-config)"
    test -z "$(ls -A /fixture/unconfigured)"
    test "$(ls -A /fixture/foreign)" = .marker
    test "$(cat /fixture/foreign/.marker)" = keep
    test "$(stat -c "%u:%g:%a" /fixture/foreign)" = 0:0:755
    test "$(stat -c "%u:%g:%a" /fixture/config/config.toml)" = 0:0:644
    test "$(stat -c "%u:%g:%a" /fixture/private)" = 10001:10001:700
'
