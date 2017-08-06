#!/usr/bin/env bash -e
t="$(mktemp)"; f() {
  rm "$t"
}
trap f EXIT; cat > "$t"; sudo sysdig -N -s0 -pc -c httplog
