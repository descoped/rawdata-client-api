#!/usr/bin/env bash
# JPMS smoke test: an application module that requires only io.descoped.rawdata.api finds every provider through the
# service loader and writes and reads a topic through filesystem, PostgreSQL 18, a fake GCS and S3Mock, in two
# layouts:
#
#   modules     everything on the module path
#   class path  everything on the class path
#
# Also checks the module declarations with jdeps. Needs Docker; CI runs it after the build.
# Usage: src/it/jpms/run.sh   (from anywhere; builds the reactor without tests first)
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
WORK="$ROOT/target/jpms-smoke"
cd "$ROOT"

echo "== build"
./mvnw --batch-mode --no-transfer-progress --quiet package -DskipTests \
  dependency:build-classpath -DincludeScope=runtime -Dmdep.outputFile=target/runtime.classpath \
  -pl rawdata-client-api,rawdata-client-provider-avro,rawdata-client-provider-postgres,rawdata-client-provider-gcs,rawdata-client-provider-s3 -am
rm -rf "$WORK" && mkdir -p "$WORK/app"

version="$(./mvnw --batch-mode --no-transfer-progress --quiet help:evaluate -Dexpression=project.version -DforceStdout)"
jar_of() { echo "$ROOT/$1/target/$1-$version.jar"; }
entries() { tr ':' '\n' < "$ROOT/$1/target/runtime.classpath"; echo; }
# One version of each library, the newest, as Maven resolves for an application (modules can resolve different ones).
newest() { grep -v '^$' | awk -F/ '{ key = $0; sub("/[^/]*/[^/]*$", "", key); print key "\t" $(NF-1) "\t" $0 }' \
  | sort -t "$(printf '\t')" -k1,1 -k2,2V | awk -F '\t' '{ last[$1] = $3 } END { for (k in last) print last[k] }' | sort; }

# Every provider and its runtime dependencies, one version of each library.
{
  jar_of rawdata-client-api; entries rawdata-client-api
  jar_of rawdata-client-provider-avro; entries rawdata-client-provider-avro
  jar_of rawdata-client-provider-postgres; entries rawdata-client-provider-postgres
  jar_of rawdata-client-provider-gcs; entries rawdata-client-provider-gcs
  jar_of rawdata-client-provider-s3; entries rawdata-client-provider-s3
} | grep -v '/rawdata-client-[a-z-]*/target/classes$' | newest > "$WORK/modules.txt"
MODULES="$(paste -sd: "$WORK/modules.txt")"
echo "   $(wc -l < "$WORK/modules.txt" | tr -d ' ') jars"

echo "== jdeps"
jdeps --multi-release 17 --module-path "$MODULES" --check io.descoped.rawdata.api,io.descoped.rawdata.provider.avro,io.descoped.rawdata.provider.postgres,io.descoped.rawdata.provider.gcs,io.descoped.rawdata.provider.s3 \
  > "$WORK/jdeps.txt" 2>&1 || { cat "$WORK/jdeps.txt"; exit 1; }
if grep -q -E 'unused|missing' "$WORK/jdeps.txt"; then
  cat "$WORK/jdeps.txt"
  echo "jdeps: the module declarations do not match the code"
  exit 1
fi

echo "== stores"
containers=()
cleanup() { [ ${#containers[@]} -eq 0 ] || docker rm -f "${containers[@]}" > /dev/null; }
trap cleanup EXIT
postgres="$(docker run -d -p 127.0.0.1::5432 -e POSTGRES_USER=rdc -e POSTGRES_PASSWORD=rdc -e POSTGRES_DB=rdc postgres:18-alpine)"
containers+=("$postgres")
gcs="$(docker run -d -p 127.0.0.1::4443 fsouza/fake-gcs-server:1.56.1 -scheme http -port 4443 -backend memory)"
containers+=("$gcs")
s3="$(docker run -d -p 127.0.0.1::9090 adobe/s3mock:5.2.3)"
containers+=("$s3")
postgres_port="$(docker port "$postgres" 5432/tcp | head -1 | sed 's/.*://')"
gcs_endpoint="http://localhost:$(docker port "$gcs" 4443/tcp | head -1 | sed 's/.*://')"
s3_endpoint="http://localhost:$(docker port "$s3" 9090/tcp | head -1 | sed 's/.*://')"
# Waits up to 60 s for a store; if it never comes up, shows its log and fails here rather than later in the app.
await() {
  local name="$1" container="$2"; shift 2
  for _ in $(seq 1 60); do
    "$@" > /dev/null 2>&1 && return 0
    sleep 1
  done
  echo "$name did not come up within 60 s:"
  docker logs "$container" 2>&1 | tail -50
  exit 1
}
await PostgreSQL "$postgres" docker exec "$postgres" pg_isready -U rdc -d rdc -h localhost
await "the fake GCS" "$gcs" curl -sf -X POST -H 'Content-Type: application/json' -d '{"name":"rawdata"}' \
  "$gcs_endpoint/storage/v1/b?project=smoke"
await S3Mock "$s3" curl -sf -X PUT "$s3_endpoint/rawdata"
STORES=(-Dsmoke.postgres.port="$postgres_port" -Dsmoke.gcs.endpoint="$gcs_endpoint" -Dsmoke.s3.endpoint="$s3_endpoint")

echo "== compile"
javac --release 17 -d "$WORK/app" --module-path "$MODULES" $(find src/it/jpms/app -name '*.java')

echo "== modules"
java "${STORES[@]}" --module-path "$WORK/app:$MODULES" --module io.descoped.rawdata.smoke/io.descoped.rawdata.smoke.Smoke modules

echo "== class path"
java "${STORES[@]}" --class-path "$WORK/app:$MODULES" io.descoped.rawdata.smoke.Smoke classpath

echo "== JPMS smoke test passed"
