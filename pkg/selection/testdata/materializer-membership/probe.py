"""Count duplicate membership operations in the production collectors via Go overlays."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile


def digest(data):
    return "sha256:" + hashlib.sha256(data).hexdigest()


def instrument(source, mode):
    inventory = []
    for index, route in enumerate(("Indexed", "Prepared", "Object")):
        name = "collect" + route + "Materializers"
        functions = list(re.finditer(r"^func \(c \*admissionClosure\) " + name + r"\([\s\S]*?(?=^func |\Z)", source, re.M))
        if len(functions) != 1:
            raise ValueError(f"{name}: expected one function, found {len(functions)}")
        match = functions[0]
        original = body = match.group()

        def replace(site, before, after):
            nonlocal body
            count = body.count(before)
            if count != 1:
                raise ValueError(f"{name}/{site}: expected one site, found {count}")
            inventory.append({
                "collector": route.lower(), "site": site, "matches": count,
                "line": source[:match.start()].count("\n") + original[:original.index(before)].count("\n") + 1,
                "hash": digest(before.encode()), "source": before,
            })
            body = body.replace(before, after, 1)

        counter = f"membershipCounts[{index}]"
        if mode == "scan":
            if body.count("slices.ContainsFunc(") != 1 or "var seen " in body:
                raise ValueError(f"{name}: expected exactly one duplicate scan")
            if route == "Indexed":
                predicate = "return existing.Declaration == occurrence.Declaration && existing.Identity == occurrence.Identity &&\n\t\t\t\texisting.Witness == occurrence.Witness"
            else:
                operand = "child.Obj" if route == "Prepared" else "object"
                predicate = f"return existing.Object == {operand}"
            replace("comparison", predicate, counter + ".Comparisons++; " + predicate)
        else:
            if body.count("seen[") != 3 or "slices.ContainsFunc(" in body:
                raise ValueError(f"{name}: expected one seed, probe, and accepted insertion")
            slot = 1 if route == "Object" else 0
            key = "candidateKey" if route == "Indexed" else "manifest.BaseManifest"
            initialization = f"seen = make(map[{key}]struct{{}}, len(candidates[{slot}]))"
            seed_key = "candidateKey{existing.Declaration, existing.Identity, existing.Witness}" if route == "Indexed" else "existing.Object"
            seed = f"seen[{seed_key}] = struct{{}}{{}}"
            probe_key = {"Indexed": "key", "Prepared": "child.Obj", "Object": "object"}[route]
            probe = f"if _, exists := seen[{probe_key}]; exists {{"
            insert_key = "key" if route == "Indexed" else "occurrence.Object"
            insertion = f"seen[{insert_key}] = struct{{}}{{}}"
            replace("declaration", f"var seen map[{key}]struct{{}}", f"var seen map[{key}]struct{{}}")
            replace("initialization", initialization, counter + ".Initializations++; " + initialization)
            replace("seed", seed, counter + ".Seeds++; " + seed)
            replace("probe", probe, counter + ".Probes++; " + probe)
            replace("insertion", insertion, counter + ".Insertions++; " + insertion)
        source = source[:match.start()] + body + source[match.end():]
    source += """
type membershipCounter struct {
    Comparisons, Probes, Insertions, Seeds, Initializations int
}
var membershipCounts [3]membershipCounter
func (c membershipCounter) operations() int {
    return c.Comparisons + c.Probes + c.Insertions + c.Seeds
}
"""
    return source, inventory


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worktree", required=True, type=Path)
    parser.add_argument("--artifact-root", required=True, type=Path)
    parser.add_argument("--mode", required=True, choices=("scan", "keyed"))
    args = parser.parse_args()
    support = Path(__file__).resolve().parent
    worktree, artifacts = args.worktree.resolve(), args.artifact_root.resolve()
    os.chdir(worktree)
    if Path.cwd().resolve() != worktree:
        raise ValueError("working directory differs from --worktree")
    artifacts.mkdir(parents=True, exist_ok=True)
    source_path = worktree / "pkg/selection/admission_materializers.go"
    source = source_path.read_text()
    test = (support / "membership_test.go").read_bytes()
    instrumented, sites = instrument(source, args.mode)
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    status = subprocess.check_output(["git", "status", "--porcelain"], text=True)
    inventory = {
        "mode": args.mode, "revision": revision, "checkout_status": status,
        "source": "pkg/selection/admission_materializers.go", "source_hash": digest(source.encode()),
        "test_hash": digest(test), "runner_hash": digest(Path(__file__).read_bytes()), "sites": sites,
    }
    (artifacts / "inventory.json").write_text(json.dumps(inventory, indent=2) + "\n")
    generated = artifacts / "generated"
    generated.mkdir(exist_ok=True)
    try:
        (generated / "admission_materializers.go").write_text(instrumented)
        (generated / "membership_test.go").write_bytes(test)
        subprocess.run(["gofmt", "-w", str(generated / "admission_materializers.go"), str(generated / "membership_test.go")], check=True)
        overlay = generated / "overlay.json"
        overlay.write_text(json.dumps({"Replace": {
            str(source_path): str(generated / "admission_materializers.go"),
            str(worktree / "pkg/selection/admission_membership_count_test.go"): str(generated / "membership_test.go"),
        }}, indent=2) + "\n")
        command = ["go", "test", "-race", "-count=1", "-overlay=" + str(overlay),
                   "-run=^TestMaterializerMembership_Count$", "-json", "./pkg/selection"]
        with tempfile.TemporaryDirectory(prefix="fl-", dir="/var/tmp") as temporary:
            environment = dict(os.environ, CGO_ENABLED="1", TMPDIR=temporary, GOTMPDIR=temporary)
            run = subprocess.run(command, env=environment, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        rows, failures, diagnostics = [], [], []
        passed = 0
        for line in run.stdout.splitlines():
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                diagnostics.append(line)
                continue
            if event.get("Action") == "fail" and "Test" in event:
                failures.append(event["Test"])
            if event.get("Action") == "pass" and "Test" in event:
                passed += 1
            output = event.get("Output", "")
            if "membership-count " in output:
                row = json.loads(output.split("membership-count ", 1)[1])
                row["test"] = event["Test"]
                rows.append(row)
            elif "operations =" in output or "did not execute" in output or "zero control" in output or "panic:" in output:
                diagnostics.append(output.strip())
        controls = [row for row in rows if row["Collector"] == "object" and re.search(r"/object/(100|200|400)/first$", row["test"])]
        expected_control = args.mode == "scan" and run.returncode != 0 and len(controls) == 3
        expected_control = expected_control and [row["Counts"]["Comparisons"] for row in controls] == [4950, 19900, 79800]
        expected_control = expected_control and all(row["test"] in failures for row in controls)
        complete = len(rows) == 108
        witnesses = {}
        for collector in ("indexed", "prepared", "object"):
            measured = [row for row in rows if row["Collector"] == collector]
            fields = ("Comparisons",) if args.mode == "scan" else ("Probes", "Insertions", "Seeds", "Initializations")
            witnesses[collector] = {field: any(row["Counts"][field] > 0 for row in measured) for field in fields}
        witnesses_ok = all(all(fields.values()) for fields in witnesses.values())
        exact_counts = args.mode != "keyed" or all(row["Counts"] == {
            "Comparisons": 0, "Probes": row["P"], "Insertions": row["A"],
            "Seeds": row["R"], "Initializations": int(row["P"] > 0),
        } for row in rows)
        result = {
            **{key: value for key, value in inventory.items() if key != "sites"},
            "command": command, "environment": {"CGO_ENABLED": "1", "TMPDIR": "/var/tmp/fl-<temporary>", "GOTMPDIR": "/var/tmp/fl-<temporary>"},
            "go_version": subprocess.check_output(["go", "version"], text=True).strip(),
            "exit_code": run.returncode, "output_hash": digest(run.stdout.encode()),
            "rows_complete": complete, "expected_base_control": expected_control,
            "positive_witnesses": witnesses, "exact_candidate_counts": exact_counts,
            "passed_tests": passed, "failed_tests": failures, "diagnostics": diagnostics, "rows": rows,
        }
        (artifacts / "result.json").write_text(json.dumps(result, indent=2) + "\n")
        print(json.dumps({"result": str(artifacts / "result.json"), "exit_code": run.returncode,
                          "rows": len(rows), "failed_tests": len(failures), "expected_base_control": expected_control}))
        if not complete or not witnesses_ok or not exact_counts or (run.returncode and not expected_control):
            print(run.stdout)
        return run.returncode if complete and witnesses_ok and exact_counts else 1
    finally:
        shutil.rmtree(generated)


if __name__ == "__main__":
    raise SystemExit(main())
