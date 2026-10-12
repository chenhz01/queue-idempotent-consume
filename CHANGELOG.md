# Changelog

All notable changes to this project are documented here.
Format based on Keep a Changelog; versioning: SemVer.

## [0.1.1] - 2026-10-13

### Fixed / Revised (round-2 iteration)
- exactly_once now additionally requires zero keyless deliveries (keyless messages are unprovable - found by A12)
- benign-replay test fixtures use identical payloads so Q002/Q003 are distinguishable (A15)
- self_test msg() helper default-key bug fixed (crashed before any test ran)

### Verification
- self_test re-run green after revisions: SUMMARY: PASS 67/67 (65 cases + 2 meta; >=60 gate met)
- CLI error contract re-verified (exit 0/1/2); four scaffold-v2 defenses inherited.

## [0.1.0] - 2026-10-13

### Added
- Core module `queue_idempotent_consume.py`: simulate (replay JSONL with --dup-every/--dup-count) / lint (consumer declaration), pure standard library.
- argparse CLI with `--json` machine output and 0/1/2 exit-code contract.
- `self_test.py` inheriting scaffold-v2 defenses: SystemExit capture, unicode-safe
  regexes, `newline=""` fixtures, and the `SUMMARY: PASS n/n` last-line contract.
- README (Performance section with [Measured] numbers and in-repo repro commands),
  MIT LICENSE, ALIGNMENT.md (16-item rubric).
