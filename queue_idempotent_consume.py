#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""queue-idempotent-consume — dedup-key declaration linter + duplicate-delivery simulator.

Checks that a consumer declares a dedup key / store / TTL, and replays a
delivery log with injected duplicates to verify exactly-once effects.
Pure stdlib.
"""
import argparse
import json
import sys
import time


class QueueError(Exception):
    """Recoverable input error surfaced as exit 2."""


# ------------------------------------------------------------- key extraction

def extract_key(msg, path="key"):
    """Resolve a dotted path like 'payload.order_id' inside a dict message."""
    if not isinstance(msg, dict):
        return None
    cur = msg
    for part in str(path).split("."):
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            return None
    return cur


def key_to_str(key):
    if key is None:
        return None
    if isinstance(key, bool):
        return "bool:%s" % key
    if isinstance(key, (int, float)):
        return "num:%s" % key
    return "str:%s" % key


# ------------------------------------------------------------------- consumer

class MemoryStore:
    """Minimal dedup store with optional TTL and effect counters."""

    def __init__(self, ttl=None):
        self.ttl = ttl
        self._seen = {}
        self.evictions = 0

    def contains(self, key):
        entry = self._seen.get(key)
        if entry is None:
            return False
        if self.ttl is not None and time.monotonic() - entry[1] > self.ttl:
            del self._seen[key]
            self.evictions += 1
            return False
        return True

    def mark(self, key):
        self._seen[key] = (True, time.monotonic())

    def __len__(self):
        return len(self._seen)


class IdempotentConsumer:
    """Delivers messages at-most-once per dedup key."""

    def __init__(self, store=None, key_path="key"):
        self.store = store if store is not None else MemoryStore()
        self.key_path = key_path
        self.processed = 0
        self.duplicates = 0
        self.no_key = 0
        self.effects = []

    def deliver(self, msg):
        raw = extract_key(msg, self.key_path)
        if raw is None:
            self.no_key += 1
            return ("no_key", None)
        key = key_to_str(raw)
        if self.store.contains(key):
            self.duplicates += 1
            return ("duplicate", key)
        self.store.mark(key)
        self.processed += 1
        self.effects.append(key)
        return ("processed", key)


# ----------------------------------------------------------------- simulation

def simulate(entries, key_path="key", dup_every=0, dup_count=1, ttl=None):
    """Replay a delivery log; after every Nth processed message re-deliver it
    dup_count times to emulate broker at-least-once redelivery."""
    if dup_every < 0 or dup_count < 0:
        raise QueueError("dup_every/dup_count must be >= 0")
    consumer = IdempotentConsumer(store=MemoryStore(ttl=ttl), key_path=key_path)
    since_processed = 0
    delivered = 0
    for i, msg in enumerate(entries):
        if not isinstance(msg, dict):
            raise QueueError("entry %d is not an object" % i)
        delivered += 1
        consumer.deliver(msg)
        if consumer.processed > since_processed:
            since_processed = consumer.processed
            if dup_every and since_processed % dup_every == 0:
                for _ in range(dup_count):
                    delivered += 1
                    consumer.deliver(msg)
    unique_keyed = len(set(
        key_to_str(extract_key(m, key_path)) for m in entries
        if extract_key(m, key_path) is not None))
    exactly_once = (consumer.no_key == 0
                    and consumer.processed == unique_keyed)
    return {
        "delivered": delivered,
        "processed": consumer.processed,
        "duplicates_suppressed": consumer.duplicates,
        "no_key": consumer.no_key,
        "unique_keyed": unique_keyed,
        "exactly_once": exactly_once,
    }


def check_log(entries, key_path="key"):
    """Static findings over a delivery log."""
    findings = []
    seen = {}
    last_seq = None
    for i, msg in enumerate(entries):
        if not isinstance(msg, dict):
            findings.append(dict(line=i + 1, code="Q098", severity="error",
                                 message="entry %d is not an object" % (i + 1)))
            continue
        raw = extract_key(msg, key_path)
        if raw is None:
            findings.append(dict(line=i + 1, code="Q001", severity="error",
                                 message="entry %d missing dedup key %r" % (i + 1, key_path)))
            continue
        ks = key_to_str(raw)
        if ks in seen:
            prev = seen[ks]
            if json.dumps(prev.get("payload"), sort_keys=True) != json.dumps(msg.get("payload"), sort_keys=True):
                findings.append(dict(
                    line=i + 1, code="Q002", severity="error",
                    message="duplicate key %r with different payload (entry %d vs %d)"
                            % (raw, prev["line"], i + 1)))
            else:
                findings.append(dict(line=i + 1, code="Q003", severity="info",
                                     message="duplicate key %r (benign replay)" % raw))
        else:
            seen[ks] = {"line": i + 1, "payload": msg.get("payload")}
        seq = msg.get("seq")
        if seq is not None:
            if last_seq is not None and seq < last_seq:
                findings.append(dict(line=i + 1, code="Q004", severity="warn",
                                     message="non-monotonic seq %s after %s" % (seq, last_seq)))
            last_seq = seq
    return findings


def lint_spec(spec):
    """Lint a consumer declaration {dedup_key, store, ttl_seconds, at_least_once}."""
    findings = []
    if not isinstance(spec, dict):
        raise QueueError("spec must be a JSON object")
    if not spec.get("dedup_key"):
        findings.append(dict(code="S001", severity="error",
                             message="consumer does not declare a dedup_key"))
    store = spec.get("store", "none")
    if store in (None, "", "none"):
        findings.append(dict(code="S002", severity="error",
                             message="no dedup store declared; at-least-once delivery will double-apply"))
    ttl = spec.get("ttl_seconds")
    if ttl is None:
        findings.append(dict(code="S003", severity="warn",
                             message="dedup TTL not declared; unbounded store growth risk"))
    elif not isinstance(ttl, (int, float)) or isinstance(ttl, bool) or ttl <= 0:
        findings.append(dict(code="S004", severity="error",
                             message="ttl_seconds must be a positive number, got %r" % (ttl,)))
    if spec.get("at_least_once") is False:
        findings.append(dict(code="S005", severity="info",
                             message="at_least_once=false: simulator redundant but harmless"))
    return findings


# ------------------------------------------------------------------------ CLI

def _load_jsonl(path):
    entries = []
    with open(path, "r", encoding="utf-8", newline="") as fh:
        for n, line in enumerate(fh, 1):
            line = line.strip()
            if not line:
                continue
            try:
                entries.append(json.loads(line))
            except json.JSONDecodeError as exc:
                raise QueueError("line %d: invalid JSON: %s" % (n, exc))
    return entries


def _cmd_simulate(args):
    entries = _load_jsonl(args.log)
    stats = simulate(entries, key_path=args.key, dup_every=args.dup_every,
                     dup_count=args.dup_count, ttl=args.ttl)
    findings = check_log(entries, key_path=args.key)
    stats["findings"] = findings
    if args.json:
        print(json.dumps(stats, ensure_ascii=False, indent=2))
    else:
        print("delivered=%d processed=%d duplicates_suppressed=%d no_key=%d exactly_once=%s"
              % (stats["delivered"], stats["processed"], stats["duplicates_suppressed"],
                 stats["no_key"], stats["exactly_once"]))
        for f in findings:
            print("  %s [%s] entry %s: %s" % (f["code"], f["severity"],
                                              f.get("line", "-"), f["message"]))
    blocking = any(f["severity"] in ("error",) for f in findings)
    if blocking or not stats["exactly_once"]:
        return 1
    return 0


def _cmd_lint(args):
    with open(args.spec, "r", encoding="utf-8") as fh:
        spec = json.load(fh)
    findings = lint_spec(spec)
    if args.json:
        print(json.dumps({"findings": findings}, ensure_ascii=False, indent=2))
    else:
        for f in findings:
            print("%s [%s] %s" % (f["code"], f["severity"], f["message"]))
        print("summary: %d finding(s)" % len(findings))
    return 1 if any(f["severity"] == "error" for f in findings) else 0


def main(argv=None):
    parser = argparse.ArgumentParser(
        prog="queue-idempotent-consume",
        description="Lint queue consumer dedup declarations and verify exactly-once "
                    "behaviour under duplicate redelivery.")
    sub = parser.add_subparsers(dest="cmd")
    p_sim = sub.add_parser("simulate", help="replay a JSONL delivery log with injected duplicates")
    p_sim.add_argument("--log", required=True, help="JSONL file of message objects")
    p_sim.add_argument("--key", default="key", help="dotted path of the dedup key (default 'key')")
    p_sim.add_argument("--dup-every", type=int, default=0,
                       help="re-deliver every Nth processed message (0 = off)")
    p_sim.add_argument("--dup-count", type=int, default=1)
    p_sim.add_argument("--ttl", type=float, default=None, help="dedup store TTL seconds")
    p_sim.add_argument("--json", action="store_true")
    p_lint = sub.add_parser("lint", help="lint a consumer declaration JSON")
    p_lint.add_argument("--spec", required=True)
    p_lint.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    if args.cmd not in ("simulate", "lint"):
        parser.print_help()
        return 2
    try:
        return _cmd_simulate(args) if args.cmd == "simulate" else _cmd_lint(args)
    except (QueueError, OSError, json.JSONDecodeError) as exc:
        print("error: %s" % exc, file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
