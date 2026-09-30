#!/bin/sh
set -e

# Seed the mounted /app/config with any default file that is not there.
#
# 🔴 This used to ask `if [ ! -f /app/config/config.yaml ]` and seed only then.
#    That is the wrong question: the program needs THREE files (config.yaml,
#    categories.yaml, categories.de.yaml), and an installation whose config
#    directory holds one but not the others — because it comes from a version
#    that did not ship it yet, or because somebody tidied up — was left with
#    what it had. The container then died at start-up with a Python traceback
#    and never came back, which looks to its owner exactly like „the update
#    broke it". Found by the pre-delivery gate, not by a user.
#
# 🔑 `cp -n` is already per-file no-clobber, so asking first bought nothing:
#    a config the user owns is never overwritten either way. Every default file
#    that is missing is put there, and nothing else is touched.
mkdir -p /app/config
cp -n /app/config-default/*.yaml /app/config/ 2>/dev/null || true

exec "$@"
