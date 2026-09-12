#!/usr/bin/env python3
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

"""Validate the prospective completed-shuffle recovery economics ledger."""

from decimal import Decimal
import json
from pathlib import Path
import sys


REPO_ROOT = Path(__file__).resolve().parents[2]
CONTRACT = (
    REPO_ROOT
    / "docs"
    / "shuffle-recovery"
    / "experimental"
    / "value-study"
    / "value-economics-v2.json"
)

REQUIRED_CASES = {
    "zero_retries",
    "zero_publication_copies",
    "positive_retry_saving_negative_net",
    "all_miss_overhead",
    "existing_provider_comparison",
    "greenfield_comparison",
}
REQUIRED_TERMS = {"B", "H", "S", "T_initial", "P_initial", "T_retry", "P_retry"}
RETRY_TERMS = ["B", "T_retry", "P_retry"]
INITIAL_TERMS = ["H", "S", "T_initial", "P_initial"]
GREENFIELD_FIXED_CAPACITY_TERMS = {"S", "T_initial", "P_initial", "T_retry", "P_retry"}


def decimal(record, key):
    return Decimal(str(record[key]))


def ledger_value(record):
    retry_value = decimal(record, "B") - decimal(record, "T_retry") - decimal(record, "P_retry")
    return (
        decimal(record, "p") * retry_value
        - decimal(record, "H")
        - decimal(record, "S")
        - decimal(record, "T_initial")
        - decimal(record, "P_initial")
        - decimal(record, "F")
    )


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def validate(contract):
    require(
        contract["status"] == "prospective-preregistered-not-executed",
        "economics contract must remain prospective",
    )
    history = contract["measurementHistory"]
    require(history["noCostMeasurementsInferred"], "revision must not infer cost measurements")
    require(
        not history["primaryRestartMeasurementsObservedBeforeThisRevision"]
        and not history["primaryOverheadMeasurementsObservedBeforeThisRevision"]
        and not history["realSourceProviderValueCampaignExecutedBeforeThisRevision"]
        and not history["newBenchmarkRunForThisRevision"],
        "measurement history no longer supports a prospective accounting repair",
    )

    population_rules = contract["populationRules"]
    require(
        population_rules["retryProbabilityAppliesOnlyTo"] == RETRY_TERMS,
        "p must apply only to retry-conditional terms",
    )
    require(
        population_rules["initialSubmissionCharges"] == INITIAL_TERMS,
        "initial-submission charge set changed",
    )
    require(
        contract["terms"]["H"]["mustRemainChargedWhenOverheadGatePasses"],
        "H cannot disappear after an overhead-gate pass",
    )

    byte_accounting = contract["byteAccounting"]
    require(
        "incremental" in byte_accounting["economicCharge"].lower()
        and "same-provider" in byte_accounting["economicCharge"].lower(),
        "economic bytes must be incremental versus the same-provider control",
    )
    require(
        byte_accounting["ordinaryControlIoChargedAgain"] is False,
        "ordinary control I/O cannot be charged twice",
    )

    uncertainty = contract["uncertainty"]
    measured_terms = set(uncertainty["requiredMeasuredTerms"])
    require(measured_terms == REQUIRED_TERMS, "economic CI does not include the complete measured ledger")
    procedure = " ".join(uncertainty["replicateProcedure"])
    for term in REQUIRED_TERMS:
        require(term in procedure, f"{term} is absent from the replicate procedure")
    require(
        "complete net-value replicate distribution" in uncertainty["finalInterval"],
        "economic CI must be formed from complete net replicates",
    )
    require(
        "covariance" in uncertainty["jointResamplingRule"],
        "joint resampling must preserve covariance",
    )

    cases = {record["id"]: record for record in contract["workedLedgers"]}
    require(REQUIRED_CASES.issubset(cases), "worked ledger is missing an acceptance case")

    for case_id in REQUIRED_CASES:
        record = cases[case_id]
        actual = ledger_value(record)
        expected = decimal(record, "expectedNet")
        require(actual == expected, f"{case_id}: expected {expected}, recomputed {actual}")

    zero_retries = cases["zero_retries"]
    require(decimal(zero_retries, "p") == 0, "zero-retries case must use p=0")
    require(decimal(zero_retries, "H") > 0, "zero-retries case must still charge H")

    zero_publication = cases["zero_publication_copies"]
    require(
        Decimal(str(zero_publication["publicationCopyBytes"])) == 0,
        "zero-publication-copy case must use zero copied bytes",
    )
    require(decimal(zero_publication, "H") > 0, "zero publication copies cannot erase H")

    negative_net = cases["positive_retry_saving_negative_net"]
    require(decimal(negative_net, "B") > 0, "negative-net case must have positive retry saving")
    require(negative_net["overheadGatePasses"] is True, "negative-net case must pass the overhead gate")
    require(decimal(negative_net, "H") > 0, "overhead gate pass cannot erase H")
    require(decimal(negative_net, "expectedNet") < 0, "positive retry saving must still yield negative net")

    all_miss = cases["all_miss_overhead"]
    require(int(all_miss["recoveryHits"]) == 0, "all-miss case must have zero recovery hits")
    require(decimal(all_miss, "H") > 0, "all-miss case must charge first-attempt overhead")

    existing = cases["existing_provider_comparison"]
    greenfield = cases["greenfield_comparison"]
    require(
        existing["comparisonGroup"] == greenfield["comparisonGroup"],
        "deployment-lens cases must share one comparison group",
    )
    require(decimal(greenfield, "F") > 0, "greenfield comparison must charge fixed capacity")
    require(decimal(existing, "F") == 0, "existing-provider comparison must not charge greenfield capacity")
    require(
        set(greenfield["marginalTermsCoveredByFixedCapacity"]).issubset(
            GREENFIELD_FIXED_CAPACITY_TERMS
        ),
        "greenfield fixed-capacity exclusions may cover only provider/storage/traffic marginal terms",
    )
    require(
        decimal(greenfield, "expectedNet") < decimal(existing, "expectedNet"),
        "greenfield fixed capacity should remain distinct from the existing-provider lens",
    )


def main():
    with CONTRACT.open(encoding="utf-8") as handle:
        contract = json.load(handle)
    validate(contract)
    print(f"validated {len(contract['workedLedgers'])} completed-shuffle economics ledgers")


if __name__ == "__main__":
    try:
        main()
    except (AssertionError, KeyError, ValueError, json.JSONDecodeError) as error:
        print(f"economics contract validation failed: {error}", file=sys.stderr)
        sys.exit(1)
