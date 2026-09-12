#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -euo pipefail
root="${1:?dependency directory required}"
mkdir -p "$root"
root="$(cd "$root" && pwd)"
celeborn_commit=edb413ee3d5e77fbecf43afa7b1a33d6054ab569
iceberg_commit=e76d63584d7f83b102026749e1ae0f91813cb78e
reuse_seed() {
  local name="$1" revision="$2" artifact="$3" archive_digest="$4"
  local seed="${NATIVE_DEPENDENCY_SEED:-$root/no-seed}"
  if [[ -f "$seed/.verified" && -f "$seed/$name.commit" && -f "$seed/$artifact" ]] &&
      [[ "$(cat "$seed/.verified")" == "$archive_digest" ]] &&
      [[ "$(cat "$seed/$name.commit")" == "$revision" ]]; then
    cp "$seed/$artifact" "$seed/$name.commit" "$root/"
    cp "$seed/$name-"*.log "$root/"
    printf '%s\n' 'verified artifact from Actions run 34486966131' > "$root/$name.origin"
    (cd "$root" && sha256sum "$artifact" > "$name.sha256")
    return 0
  fi
  return 1
}
fetch_source() {
  local name="$1" url="$2" revision="$3"
  git init -q "$root/$name"
  git -C "$root/$name" remote add origin "$url"
  git -C "$root/$name" fetch --depth=1 origin "$revision"
  git -C "$root/$name" checkout -q --detach FETCH_HEAD
  git -C "$root/$name" rev-parse HEAD > "$root/$name.commit"
}
if [[ "${2:-all}" != iceberg ]] && ! reuse_seed celeborn "$celeborn_commit" \
    celeborn-dist.tgz 7fea87e72bea23a2a46febd40907e2569132cc29d09be510bc7e17ab61e18a31; then
  fetch_source celeborn https://github.com/unikdahal/celeborn.git "$celeborn_commit"
  (
    cd "$root/celeborn"
    bash build/make-distribution.sh -Pspark-4.2
  ) 2>&1 | tee "$root/celeborn-build.log"
  (
    cd "$root/celeborn"
    build/mvn -Pspark-4.2 -pl client -am test \
      '-Dtest=RetainedShuffle*' \
      -DwildcardSuites=org.apache.celeborn.client.RetainedShuffle
  ) 2>&1 | tee "$root/celeborn-tests.log"
  grep -F 'RetainedShuffleLeasesSuite' "$root/celeborn-tests.log"
  grep -F 'RetainedShuffleDescriptorSuite' "$root/celeborn-tests.log"
  tar -C "$root/celeborn" -czf "$root/celeborn-dist.tgz" dist
  (cd "$root" && sha256sum celeborn-dist.tgz > celeborn.sha256)
fi
if [[ "${2:-all}" != celeborn ]] && ! reuse_seed iceberg "$iceberg_commit" \
    iceberg-runtime.jar 1f27a2e3a2cb29a9d63d7baa5e3827c3f0d1e666dda9c849324ec3728c4a0131; then
  fetch_source iceberg https://github.com/apache/iceberg.git "$iceberg_commit"
  (
    cd "$root/iceberg"
    ./gradlew --no-daemon -DsparkVersions=4.2 \
      :iceberg-spark:iceberg-spark-runtime-4.2_2.13:shadowJar
  ) 2>&1 | tee "$root/iceberg-build.log"
  mapfile -t jars < <(find "$root/iceberg/spark/v4.2/spark-runtime/build/libs" \
    -maxdepth 1 -name 'iceberg-spark-runtime-4.2_2.13-*.jar' \
    ! -name '*-sources.jar' ! -name '*-javadoc.jar')
  [[ ${#jars[@]} == 1 ]]
  cp "${jars[0]}" "$root/iceberg-runtime.jar"
  (cd "$root" && sha256sum iceberg-runtime.jar > iceberg.sha256)
fi
