#!/usr/bin/env bash

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

set -euo pipefail

repo_root=$(cd "$(dirname "$0")/.." && pwd)
java_tool_dir=""
if [[ -n "${JAVA_HOME:-}" ]]; then
  java_tool_dir="$JAVA_HOME/bin/"
fi
output=${1:-/tmp/spark-recovery-api.jar}
case "$output" in
  /*) ;;
  *) output="$(pwd)/$output" ;;
esac

catalyst_classes="$repo_root/sql/catalyst/target/scala-2.13/classes"
sql_classes="$repo_root/sql/core/target/scala-2.13/classes"
class_allowlist="$repo_root/dev/recovery-api-public-classes.txt"
stage=$(mktemp -d)
trap 'rm -rf "$stage"' EXIT

if [[ ! -d "$catalyst_classes" || ! -d "$sql_classes" ]]; then
  echo "Catalyst and SQL must be compiled before creating the recovery API artifact." >&2
  exit 1
fi

while IFS= read -r binary_name; do
  [[ -z "$binary_name" || "$binary_name" == \#* ]] && continue
  relative=${binary_name//./\/}
  matches=0
  for root in "$catalyst_classes" "$sql_classes"; do
    class_file="$root/$relative.class"
    if [[ -f "$class_file" ]]; then
      matches=$((matches + 1))
      mkdir -p "$stage/$(dirname "$relative")"
      cp "$class_file" "$stage/$relative.class"
    fi
  done
  if [[ "$matches" -ne 1 ]]; then
    echo "Expected one compiled recovery API class, found $matches: $binary_name" >&2
    exit 1
  fi
done < "$class_allowlist"

bytecode_extras=(
  'org/apache/spark/sql/catalyst/analysis/SourceRecoveryInfo$.class'
  'org/apache/spark/sql/catalyst/analysis/WriteRecoveryInfo$.class'
  'org/apache/spark/sql/connector/recovery/RecoveryTaskCommitStore$Capabilities.class'
  'org/apache/spark/sql/connector/recovery/RecoveryTaskCommitStore$FailureReason.class'
  'org/apache/spark/sql/connector/recovery/RecoveryTaskCommitStore$StoreException.class'
  'org/apache/spark/sql/execution/adaptive/RecoveredShuffleStage$.class'
  'org/apache/spark/sql/execution/adaptive/ShuffleStageRecovery$.class'
  'org/apache/spark/sql/execution/adaptive/ShuffleStageRecoveryInfo$.class'
)
for relative in "${bytecode_extras[@]}"; do
  matches=0
  for root in "$catalyst_classes" "$sql_classes"; do
    if [[ -f "$root/$relative" ]]; then
      matches=$((matches + 1))
      mkdir -p "$stage/$(dirname "$relative")"
      cp "$root/$relative" "$stage/$relative"
    fi
  done
  if [[ "$matches" -ne 1 ]]; then
    echo "Expected one compiled recovery API companion, found $matches: $relative" >&2
    exit 1
  fi
done

mkdir -p "$(dirname "$output")"
(
  cd "$stage"
  find . -type f -name '*.class' -printf '%P\n' | LC_ALL=C sort > "$output.classes"
  "${java_tool_dir}jar" --create --file "$output" --no-manifest \
    --date=2020-01-01T00:00:00Z @"$output.classes"
)

: > "$output.signatures"
while IFS= read -r binary_name; do
  [[ -z "$binary_name" || "$binary_name" == \#* ]] && continue
  "${java_tool_dir}javap" -public -s -constants \
    -classpath "$catalyst_classes:$sql_classes" "$binary_name" \
    >> "$output.signatures"
done < "$class_allowlist"

sha256sum "$output" > "$output.sha256"
sha256sum "$output.classes" > "$output.classes.sha256"
sha256sum "$output.signatures" > "$output.signatures.sha256"
worktree_sha256=$({
  git -C "$repo_root" diff --binary HEAD
  git -C "$repo_root" ls-files --others --exclude-standard -z | LC_ALL=C sort -z |
    while IFS= read -r -d '' path; do
      printf 'untracked %s\n' "$path"
      sha256sum "$repo_root/$path"
    done
} | sha256sum | awk '{print $1}')
{
  echo "head=$(git -C "$repo_root" rev-parse HEAD)"
  echo "worktree_sha256=$worktree_sha256"
  echo "scala_binary_version=2.13"
  echo "java=$("${java_tool_dir}java" -version 2>&1 | sed -n '/version/{p;q;}')"
  echo "jar_date=2020-01-01T00:00:00Z"
  echo "usage=compile-time overlay; not a standalone or runtime replacement Spark jar"
  git -C "$repo_root" status --porcelain=v1
} > "$output.provenance"

echo "$output"
cat "$output.sha256"
