#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
image=${ACCESSRELAY_IMAGE:-accessrelay:test}
if [ "${ACCESSRELAY_SKIP_BUILD:-false}" != true ]; then
  docker build --build-arg VERSION="$(cat VERSION)" -t "$image" .
fi
docker run --rm --read-only --cap-drop ALL --network none "$image" --version
docker run --rm --read-only --cap-drop ALL --network none --entrypoint goaccess "$image" --version
ACCESSRELAY_IMAGE="$image" python3 tests/integration/runtime.py
