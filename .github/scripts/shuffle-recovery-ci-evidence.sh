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

command="${1:-}"
shift $(( $# > 0 ? 1 : 0 ))

case "${command}" in
  resolve-mode)
    event_name="${1:-}"
    ref_name="${2:-}"
    actual="${3:-}"
    case "${event_name}" in
      pull_request)
        mode=focused
        requested="${actual}"
        ;;
      push)
        if [[ "${ref_name}" == spip/poc-owned-end-to-end ]]; then
          mode=focused
          requested="${actual}"
        elif [[ "${ref_name}" =~ ^shuffle-recovery-ci/(full|evidence)/([0-9a-fA-F]{40})$ ]]; then
          mode="${BASH_REMATCH[1]}"
          requested="${BASH_REMATCH[2]}"
        else
          echo "unsupported shuffle recovery tag: ${ref_name:-<empty>}" >&2
          exit 1
        fi
        ;;
      *)
        echo "unsupported shuffle recovery event: ${event_name:-<empty>}" >&2
        exit 1
        ;;
    esac
    bash "${BASH_SOURCE[0]}" validate-candidate "${mode}" "${requested}" "${actual}"
    echo "validation_mode=${mode}"
    echo "requested_sha=${requested}"
    ;;

  validate-candidate)
    mode="${1:-}"
    requested="${2:-}"
    actual="${3:-}"
    case "${mode}" in focused|full|evidence) ;; *) exit 2 ;; esac
    if [[ "${mode}" == full || "${mode}" == evidence ]]; then
      if [[ ! "${requested}" =~ ^[0-9a-fA-F]{40}$ ]]; then
        echo "full/evidence mode requires a full 40-character candidate SHA" >&2
        exit 1
      fi
    elif [[ -z "${requested}" ]]; then
      exit 0
    elif [[ ! "${requested}" =~ ^[0-9a-fA-F]{40}$ ]]; then
      echo "candidate SHA must be a full 40-character SHA when supplied" >&2
      exit 1
    fi
    if [[ "${requested,,}" != "${actual,,}" ]]; then
      echo "checked out ${actual}, expected ${requested}" >&2
      exit 1
    fi
    ;;

  require-suite)
    module="${1:?module is required}"
    suite="${2:?suite is required}"
    short="${suite##*.}"
    match="$(find "${module}/src/test" -type f -name "${short}.scala" -print -quit 2>/dev/null || true)"
    if [[ -z "${match}" ]]; then
      echo "required suite ${suite} is missing from ${module}" >&2
      exit 1
    fi
    printf '%s\n' "${match}"
    ;;

  locate-report)
    module="${1:?module is required}"
    suite="${2:?suite is required}"
    fresh_after="${3:-}"
    expected="TEST-${suite}.xml"
    mapfile -d '' reports < <(
      find "${module}" -type f -path '*/test-reports/*' -name "${expected}" -print0 2>/dev/null | sort -z)
    if [[ ${#reports[@]} -ne 1 ]]; then
      echo "required suite ${suite} produced ${#reports[@]} exact reports; expected 1" >&2
      exit 1
    fi
    report="${reports[0]}"
    if [[ ! -s "${report}" ]]; then
      echo "required suite ${suite} produced an empty test report: ${report}" >&2
      exit 1
    fi
    if [[ -n "${fresh_after}" ]]; then
      if [[ ! -f "${fresh_after}" ]]; then
        echo "freshness marker does not exist: ${fresh_after}" >&2
        exit 1
      fi
      if [[ ! "${report}" -nt "${fresh_after}" ]]; then
        echo "required suite ${suite} report is stale: ${report}" >&2
        exit 1
      fi
    fi
    printf '%s\n' "${report}"
    ;;

  validate-report)
    report="${1:?report is required}"
    suite="${2:?suite is required}"
    python3 - "${report}" "${suite}" <<'PY'
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

report = Path(sys.argv[1])
expected_suite = sys.argv[2]
# This is the one pre-existing ignored case observed in the frozen/current Spark suite evidence.
# Keep the allowance tied to the exact testcase so a newly ignored test cannot silently pass.
allowed_skipped_tests = {
    "org.apache.spark.MapOutputTrackerSuite": {
        "SPARK-32210: serialize and deserialize over 2GB compressed mapStatuses"
    }
}


def fail(message: str) -> None:
    raise SystemExit(f"{report}: {message}")


def local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def integer_attr(root, name: str) -> int:
    value = root.attrib.get(name)
    if value is None:
        fail(f"missing testsuite {name} attribute")
    try:
        parsed = int(value)
    except ValueError:
        fail(f"invalid testsuite {name}={value!r}")
    if parsed < 0:
        fail(f"negative testsuite {name}={parsed}")
    return parsed

try:
    tree = ET.parse(report)
except (ET.ParseError, OSError) as exc:
    fail(f"invalid XML: {exc}")
root = tree.getroot()
if local_name(root.tag) != "testsuite":
    fail(f"expected testsuite root, found {local_name(root.tag)!r}")
if root.attrib.get("name") != expected_suite:
    fail(f"suite identity {root.attrib.get('name')!r} does not match {expected_suite!r}")

tests = integer_attr(root, "tests")
errors = integer_attr(root, "errors")
failures = integer_attr(root, "failures")
skipped = integer_attr(root, "skipped")
testcases = [child for child in root if local_name(child.tag) == "testcase"]
if len(testcases) != tests:
    fail(f"tests={tests} but XML contains {len(testcases)} testcase elements")
actual_skipped = 0
actual_failures = 0
actual_errors = 0
skipped_names = set()
for testcase in testcases:
    if testcase.attrib.get("classname") != expected_suite:
        fail(
            f"testcase classname {testcase.attrib.get('classname')!r} does not match "
            f"{expected_suite!r}")
    child_kinds = [local_name(child.tag) for child in testcase]
    skipped_children = child_kinds.count("skipped")
    actual_skipped += skipped_children
    if skipped_children:
        skipped_names.add(testcase.attrib.get("name", ""))
    actual_failures += child_kinds.count("failure")
    actual_errors += child_kinds.count("error")
if actual_skipped != skipped:
    fail(f"skipped={skipped} but XML contains {actual_skipped} skipped testcases")
if actual_failures != failures:
    fail(f"failures={failures} but XML contains {actual_failures} failure elements")
if actual_errors != errors:
    fail(f"errors={errors} but XML contains {actual_errors} error elements")
if failures != 0 or errors != 0:
    fail(f"suite has failures={failures}, errors={errors}")
allowed = allowed_skipped_tests.get(expected_suite, set())
if skipped_names - allowed:
    fail(f"suite skipped unapproved tests: {sorted(skipped_names - allowed)!r}")
if skipped != len(skipped_names):
    fail("multiple skipped elements or duplicate skipped testcase names are not permitted")
executed = tests - skipped
if executed <= 0:
    fail(f"suite executed no tests: tests={tests}, skipped={skipped}")
print(f"suite={expected_suite}")
print(f"tests={tests}")
print(f"executed={executed}")
print(f"skipped={skipped}")
print("failures=0")
print("errors=0")
PY
    ;;

  require-report)
    module="${1:?module is required}"
    suite="${2:?suite is required}"
    fresh_after="${3:-}"
    report="$(bash "${BASH_SOURCE[0]}" locate-report "${module}" "${suite}" "${fresh_after}")"
    bash "${BASH_SOURCE[0]}" validate-report "${report}" "${suite}" >/dev/null
    printf '%s\n' "${report}"
    ;;

  validate-cold-process)
    evidence_root="${1:?cold-process evidence root is required}"
    candidate="${2:?candidate SHA is required}"
    python3 - "${evidence_root}" "${candidate}" <<'PY'
import csv
import re
import sys
from collections import Counter
from pathlib import Path

root = Path(sys.argv[1])
candidate = sys.argv[2].lower()
if not re.fullmatch(r"[0-9a-f]{40}", candidate):
    raise SystemExit(f"candidate must be a full lowercase-normalized SHA: {candidate!r}")
if not root.is_dir():
    raise SystemExit(f"cold-process evidence directory is missing: {root}")

header = [
    "role", "scenario", "control", "sparkBaseline", "testedCommit",
    "sparkCompatibility", "providerCompatibility", "aqeEnabled", "group", "generation",
    "publishingGeneration", "originShuffleId", "currentShuffleId", "targetStageIds",
    "mapTaskCount", "adopted", "incarnation", "providerBlockReads",
    "providerNonEmptyReads", "providerEmptyReads", "providerBytesRead", "emptyBlocks",
    "nonEmptyBlocks", "physicalBytes", "maxBlockBytes", "rowCount", "resultDigest",
    "elapsedMillis", "note"]
scenarios = ["sparse", "empty", "adjacent", "skewed", "small", "large", "wide"]
controls = [
    "disabled", "group-diff", "generation-not-later", "source-token", "provider-compat",
    "manifest-absent", "digest-collision", "artifact-missing", "claim-unavailable",
    "reservation-stale"]


def fail(message: str) -> None:
    raise SystemExit(message)


def single_dir(prefix: str) -> Path:
    matches = sorted(path for path in root.iterdir() if path.is_dir() and path.name.startswith(prefix))
    if len(matches) != 1:
        fail(f"expected exactly one {prefix} directory under {root}, found {len(matches)}")
    return matches[0]


def read_table(path: Path, one_row: bool = False):
    try:
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle, delimiter="\t")
            if reader.fieldnames != header:
                fail(f"{path}: unexpected header {reader.fieldnames!r}")
            rows = list(reader)
    except (OSError, UnicodeError, csv.Error) as exc:
        fail(f"{path}: invalid TSV: {exc}")
    if one_row and len(rows) != 1:
        fail(f"{path}: expected exactly one data row, found {len(rows)}")
    for row in rows:
        if None in row:
            fail(f"{path}: malformed row with extra columns")
        if row["testedCommit"].lower() != candidate:
            fail(
                f"{path}: testedCommit={row['testedCommit']!r} does not match candidate {candidate}")
    return rows


def read_kv(path: Path):
    values = {}
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        fail(f"{path}: invalid evidence: {exc}")
    for line in lines:
        if line.count("\t") != 1:
            fail(f"{path}: invalid key/value line {line!r}")
        key, value = line.split("\t", 1)
        if not key or key in values:
            fail(f"{path}: invalid or duplicate key {key!r}")
        values[key] = value
    if values.get("testedCommit", "").lower() != candidate:
        fail(
            f"{path}: testedCommit={values.get('testedCommit')!r} does not match candidate {candidate}")
    return values


def integer(value: str, description: str) -> int:
    try:
        return int(value)
    except ValueError:
        fail(f"{description} is not an integer: {value!r}")

cold_dir = single_dir("run-")
expected = []
for scenario in scenarios:
    expected.extend(
        f"happy-{scenario}-{role}.tsv" for role in ("baseline", "producer", "replacement"))
for prefix in ("repeat-sparse", "abrupt-sparse"):
    expected.extend(f"{prefix}-{role}.tsv" for role in ("baseline", "producer", "replacement"))
expected.extend(["negative-baseline.tsv", "negative-producer.tsv"])
expected.extend(f"negative-{control}.tsv" for control in controls)
actual = sorted(path.name for path in cold_dir.glob("*.tsv"))
if actual != sorted(expected):
    fail(
        f"{cold_dir}: cold child record set mismatch; missing={sorted(set(expected)-set(actual))}, "
        f"extra={sorted(set(actual)-set(expected))}")

child_rows = []
for name in expected:
    path = cold_dir / name
    log = path.with_name(path.name + ".log")
    if not log.is_file():
        fail(f"{path}: missing preserved child log {log.name}")
    row = read_table(path, one_row=True)[0]
    child_rows.append(row)

for scenario in scenarios:
    family = {}
    for role in ("baseline", "producer", "replacement"):
        path = cold_dir / f"happy-{scenario}-{role}.tsv"
        row = read_table(path, one_row=True)[0]
        if row["role"] != role or row["scenario"] != scenario or row["control"] != "none":
            fail(f"{path}: unexpected role/scenario/control")
        family[role] = row
    if len({family[role]["resultDigest"] for role in family}) != 1:
        fail(f"happy-{scenario}: result digests differ across cold processes")
    replacement = family["replacement"]
    if replacement["adopted"] != "true" or integer(replacement["mapTaskCount"], str(path)) != 0:
        fail(f"happy-{scenario}: replacement did not adopt with zero map tasks")
    if replacement["providerEmptyReads"] != "0":
        fail(f"happy-{scenario}: replacement fetched a known-empty provider block")
    if replacement["currentShuffleId"] == replacement["originShuffleId"]:
        fail(f"happy-{scenario}: replacement reused the producer shuffle id")

for prefix in ("repeat-sparse", "abrupt-sparse"):
    rows = {}
    for role in ("baseline", "producer", "replacement"):
        path = cold_dir / f"{prefix}-{role}.tsv"
        row = read_table(path, one_row=True)[0]
        if row["role"] != role or row["scenario"] != "sparse" or row["control"] != "none":
            fail(f"{path}: unexpected role/scenario/control")
        rows[role] = row
    if len({rows[role]["resultDigest"] for role in rows}) != 1:
        fail(f"{prefix}: result digests differ across cold processes")
    replacement = rows["replacement"]
    if replacement["adopted"] != "true" or integer(replacement["mapTaskCount"], prefix) != 0:
        fail(f"{prefix}: replacement did not adopt with zero map tasks")
    if replacement["currentShuffleId"] == replacement["originShuffleId"]:
        fail(f"{prefix}: replacement reused the producer shuffle id")

negative_reference = {}
for role in ("baseline", "producer"):
    path = cold_dir / f"negative-{role}.tsv"
    row = read_table(path, one_row=True)[0]
    if row["role"] != role or row["scenario"] != "negative" or row["control"] != "none":
        fail(f"{path}: unexpected role/scenario/control")
    negative_reference[role] = row
if negative_reference["baseline"]["resultDigest"] != negative_reference["producer"]["resultDigest"]:
    fail("negative control baseline and producer result digests differ")
for control in controls:
    path = cold_dir / f"negative-{control}.tsv"
    row = read_table(path, one_row=True)[0]
    if row["role"] != "replacement" or row["scenario"] != "negative" or row["control"] != control:
        fail(f"{path}: unexpected role/scenario/control")
    if row["adopted"] != "false" or integer(row["mapTaskCount"], str(path)) <= 0:
        fail(f"{path}: negative control did not recompute")
    if row["resultDigest"] != negative_reference["baseline"]["resultDigest"]:
        fail(f"{path}: negative control result differs from baseline")

aggregate_path = root / "cold-process-evidence.tsv"
aggregate = read_table(aggregate_path)
if len(aggregate) != len(child_rows):
    fail(
        f"{aggregate_path}: aggregate row count {len(aggregate)} does not match "
        f"{len(child_rows)} child records")
def row_tuple(row):
    return tuple(row[name] for name in header)
if Counter(map(row_tuple, aggregate)) != Counter(map(row_tuple, child_rows)):
    fail(f"{aggregate_path}: aggregate rows do not exactly match child records")

healing_dir = single_dir("adopted-failure-run-")
healing_names = ["producer.tsv", "failure.tsv", "healed.tsv"]
actual_healing = sorted(path.name for path in healing_dir.glob("*.tsv"))
if actual_healing != sorted(healing_names):
    fail(
        f"{healing_dir}: healing child record set mismatch; "
        f"missing={sorted(set(healing_names)-set(actual_healing))}, "
        f"extra={sorted(set(actual_healing)-set(healing_names))}")
healing = {}
for name in healing_names:
    path = healing_dir / name
    log = path.with_name(path.name + ".log")
    if not log.is_file():
        fail(f"{path}: missing preserved healing child log {log.name}")
    healing[name] = read_kv(path)
producer = healing["producer.tsv"]
failure = healing["failure.tsv"]
healed = healing["healed.tsv"]
if producer.get("role") != "producer" or producer.get("publishedGeneration") != "1":
    fail("healing producer record does not advertise generation 1")
if failure.get("role") != "failure" or failure.get("adopted") != "false":
    fail("healing failure record does not advertise all-fresh fallback")
if failure.get("retiredA") != "true" or failure.get("publishedGeneration") != "2":
    fail("healing failure record does not advertise exact-A retirement and successor publication")
if failure.get("mapTaskPartitions") != "0,1,2,3" or integer(
        failure.get("mapTaskCount", ""), "healing failure mapTaskCount") <= 0:
    fail("healing failure record does not prove complete mapper recomputation")
if healed.get("role") != "healed" or healed.get("adopted") != "true":
    fail("healed record does not advertise successor adoption")
if integer(healed.get("mapTaskCount", ""), "healed mapTaskCount") != 0:
    fail("healed successor launched map tasks")
if healed.get("publishingGeneration") != "2":
    fail("healed record did not select generation 2")
if len({producer.get("resultDigest"), failure.get("resultDigest"), healed.get("resultDigest")}) != 1:
    fail("healing process result digests differ")
if len({producer.get("rowCount"), failure.get("rowCount"), healed.get("rowCount")}) != 1:
    fail("healing process row counts differ")

print(f"candidate_sha={candidate}")
print(f"cold_child_records={len(child_rows)}")
print(f"cold_aggregate_rows={len(aggregate)}")
print("happy_replacements=9")
print(f"negative_controls={len(controls)}")
print(f"healing_child_records={len(healing_names)}")
print("process_evidence=validated")
PY
    ;;

  checksum-tree)
    directory="${1:?directory is required}"
    if [[ ! -d "${directory}" ]]; then
      echo "evidence directory does not exist: ${directory}" >&2
      exit 1
    fi
    (
      cd "${directory}"
      mapfile -d '' files < <(find . -type f ! -name checksums.sha256 -print0 | sort -z)
      if [[ ${#files[@]} -eq 0 ]]; then
        echo "no evidence files available for checksum" >&2
        exit 1
      fi
      : > checksums.sha256
      for file in "${files[@]}"; do
        sha256sum "${file#./}" >> checksums.sha256
      done
      test -s checksums.sha256
    )
    ;;

  *)
    echo "unsupported evidence command: ${command:-<empty>}" >&2
    exit 2
    ;;
esac
