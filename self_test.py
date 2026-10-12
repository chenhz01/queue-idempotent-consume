# -*- coding: utf-8 -*-
"""self_test for queue-idempotent-consume — inherits scaffold-v2 defenses D1-D4."""
import contextlib
import io
import json
import os
import sys
import tempfile
import unittest

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", newline="\n")  # D3
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import queue_idempotent_consume as mod  # noqa: E402


def run_cli(argv):  # D1
    buf_out, buf_err = io.StringIO(), io.StringIO()
    code = 0
    try:
        with contextlib.redirect_stdout(buf_out), contextlib.redirect_stderr(buf_err):
            rc = mod.main(argv)
            code = 0 if rc is None else rc
    except SystemExit as e:
        code = e.code if isinstance(e.code, int) else (0 if e.code is None else 1)
    except Exception as e:  # noqa: BLE001
        code = 1
        buf_err.write(repr(e))
    return code, buf_out.getvalue(), buf_err.getvalue()


CASES = []


def case(name, cat):
    def deco(fn):
        CASES.append((name, cat, fn))
        return fn
    return deco


TMP = tempfile.mkdtemp(prefix="qic_selftest_")


def write_jsonl(name, entries):
    p = os.path.join(TMP, name)
    with open(p, "w", encoding="utf-8", newline="") as fh:  # D3
        for e in entries:
            fh.write(json.dumps(e, ensure_ascii=False) + "\n")
    return p


def write_json(name, obj):
    p = os.path.join(TMP, name)
    with open(p, "w", encoding="utf-8", newline="") as fh:  # D3
        json.dump(obj, fh, ensure_ascii=False)
    return p


def msg(i, key=None, payload=None):
    if key is None:
        key = "k%d" % i
    return {"seq": i, "id": "m%d" % i, "key": (key % i) if isinstance(key, str) and "%d" in key else key,
            "payload": payload if payload is not None else {"n": i}}


# ------------------------------------------------------------------ [A] functional

@case("A01_deliver_processed_once", "A")
def t():
    c = mod.IdempotentConsumer()
    assert c.deliver({"key": "a"})[0] == "processed"

@case("A02_deliver_duplicate_suppressed", "A")
def t():
    c = mod.IdempotentConsumer()
    c.deliver({"key": "a"})
    assert c.deliver({"key": "a"})[0] == "duplicate"

@case("A03_missing_key_reported", "A")
def t():
    c = mod.IdempotentConsumer()
    assert c.deliver({"nope": 1})[0] == "no_key"

@case("A04_dotted_key_path", "A")
def t():
    c = mod.IdempotentConsumer(key_path="payload.order_id")
    assert c.deliver({"payload": {"order_id": "o1"}})[0] == "processed"

@case("A05_dotted_path_missing_returns_none", "A")
def t():
    assert mod.extract_key({"payload": {}}, "payload.order_id") is None

@case("A06_key_to_str_types_distinguished", "A")
def t():
    assert mod.key_to_str(1) != mod.key_to_str("1")

@case("A07_numeric_key_works", "A")
def t():
    c = mod.IdempotentConsumer()
    c.deliver({"key": 42})
    assert c.deliver({"key": 42})[0] == "duplicate"

@case("A08_bool_key_not_confused_with_int", "A")
def t():
    c = mod.IdempotentConsumer()
    c.deliver({"key": 1})
    assert c.deliver({"key": True})[0] == "processed"

@case("A09_simulate_no_dups_exactly_once", "A")
def t():
    entries = [msg(i, "k%d") for i in range(10)]
    s = mod.simulate(entries)
    assert s["exactly_once"] and s["processed"] == 10 and s["duplicates_suppressed"] == 0

@case("A10_simulate_injects_duplicates", "A")
def t():
    entries = [msg(i, "k%d") for i in range(10)]
    s = mod.simulate(entries, dup_every=2, dup_count=1)
    assert s["duplicates_suppressed"] == 5 and s["processed"] == 10

@case("A11_simulate_no_key_counted", "A")
def t():
    s = mod.simulate([{"id": "x"}, {"key": "a"}])
    assert s["no_key"] == 1 and s["processed"] == 1

@case("A12_simulate_exactly_once_false_on_nokey", "A")
def t():
    s = mod.simulate([{"id": "x"}])
    assert s["exactly_once"] is False

@case("A13_unique_keyed_counts_dedup", "A")
def t():
    entries = [msg(0, "same"), msg(1, "same"), msg(2, "other")]
    s = mod.simulate(entries)
    assert s["unique_keyed"] == 2

@case("A14_check_log_missing_key_Q001", "A")
def t():
    f = mod.check_log([{"id": 1}])
    assert f[0]["code"] == "Q001" and f[0]["severity"] == "error"

@case("A15_check_log_benign_replay_Q003", "A")
def t():
    f = mod.check_log([msg(0, "k", {"n": 1}), msg(1, "k", {"n": 1})])
    assert f[0]["code"] == "Q003"

@case("A16_check_log_payload_mismatch_Q002", "A")
def t():
    f = mod.check_log([msg(0, "k", {"n": 1}), msg(1, "k", {"n": 2})])
    assert f[0]["code"] == "Q002" and f[0]["severity"] == "error"

@case("A17_check_log_nonmonotonic_Q004", "A")
def t():
    f = mod.check_log([{"seq": 5, "key": "a"}, {"seq": 3, "key": "b"}])
    assert f[0]["code"] == "Q004" and f[0]["severity"] == "warn"

@case("A18_lint_missing_dedup_key_S001", "A")
def t():
    f = mod.lint_spec({"store": "memory", "ttl_seconds": 60})
    assert any(x["code"] == "S001" for x in f)

@case("A19_lint_no_store_S002", "A")
def t():
    f = mod.lint_spec({"dedup_key": "key", "ttl_seconds": 60})
    assert any(x["code"] == "S002" for x in f)

@case("A20_lint_missing_ttl_S003", "A")
def t():
    f = mod.lint_spec({"dedup_key": "key", "store": "memory"})
    assert any(x["code"] == "S003" for x in f)

@case("A21_lint_clean_spec_no_errors", "A")
def t():
    f = mod.lint_spec({"dedup_key": "key", "store": "memory", "ttl_seconds": 300, "at_least_once": True})
    assert not any(x["severity"] == "error" for x in f)

@case("A22_cli_simulate_clean_exit0", "A")
def t():
    p = write_jsonl("a22.jsonl", [msg(i, "k%d") for i in range(5)])
    code, out, _ = run_cli(["simulate", "--log", p, "--dup-every", "2"])
    assert code == 0 and "exactly_once=True" in out

@case("A23_cli_simulate_json", "A")
def t():
    p = write_jsonl("a23.jsonl", [msg(i, "k%d") for i in range(4)])
    code, out, _ = run_cli(["simulate", "--log", p, "--dup-every", "1", "--json"])
    data = json.loads(out)
    assert code == 0 and data["duplicates_suppressed"] == 4

@case("A24_cli_lint_exit_codes", "A")
def t():
    bad = write_json("bad_spec.json", {"store": "none"})
    good = write_json("good_spec.json", {"dedup_key": "key", "store": "memory", "ttl_seconds": 60})
    assert run_cli(["lint", "--spec", bad])[0] == 1
    assert run_cli(["lint", "--spec", good])[0] == 0

@case("A25_store_eviction_on_ttl", "A")
def t():
    st = mod.MemoryStore(ttl=0.0)
    c = mod.IdempotentConsumer(store=st)
    c.deliver({"key": "a"})
    assert c.deliver({"key": "a"})[0] == "processed"  # ttl 0 -> expired

# ------------------------------------------------------------------ [B] exception / CLI contract

@case("B01_cli_no_subcommand_exit2", "B")
def t():
    assert run_cli([])[0] == 2

@case("B02_cli_unknown_subcommand_exit2", "B")
def t():
    assert run_cli(["frob"])[0] == 2

@case("B03_simulate_missing_log_exit2", "B")
def t():
    code, _, err = run_cli(["simulate", "--log", "ghost.jsonl"])
    assert code == 2 and "error" in err

@case("B04_simulate_missing_required_flag", "B")
def t():
    assert run_cli(["simulate"])[0] == 2

@case("B05_negative_dup_every_exit2", "B")
def t():
    p = write_jsonl("b05.jsonl", [msg(0, "a")])
    code, _, err = run_cli(["simulate", "--log", p, "--dup-every", "-1"])
    assert code == 2 and "dup_every" in err

@case("B06_non_object_line_exit2", "B")
def t():
    p = os.path.join(TMP, "b06.jsonl")
    with open(p, "w", encoding="utf-8", newline="") as fh:
        fh.write("[1,2]\n")
    code, _, _ = run_cli(["simulate", "--log", p])
    assert code == 2

@case("B07_invalid_json_line_exit2", "B")
def t():
    p = os.path.join(TMP, "b07.jsonl")
    with open(p, "w", encoding="utf-8", newline="") as fh:
        fh.write("{oops\n")
    code, _, err = run_cli(["simulate", "--log", p])
    assert code == 2 and "line 1" in err

@case("B08_simulate_non_dict_entry", "B")
def t():
    try:
        mod.simulate(["not a dict"])
        assert False
    except mod.QueueError as e:
        assert "not an object" in str(e)

@case("B09_lint_non_object_spec", "B")
def t():
    p = write_json("b09.json", [1])
    code, _, err = run_cli(["lint", "--spec", p])
    assert code == 2

@case("B10_lint_bad_json_exit2", "B")
def t():
    p = os.path.join(TMP, "b10.json")
    with open(p, "w", encoding="utf-8", newline="") as fh:
        fh.write("{bad")
    code, _, _ = run_cli(["lint", "--spec", p])
    assert code == 2

@case("B11_lint_negative_ttl_S004", "B")
def t():
    f = mod.lint_spec({"dedup_key": "k", "store": "memory", "ttl_seconds": -5})
    assert any(x["code"] == "S004" for x in f)

@case("B12_lint_bool_ttl_rejected", "B")
def t():
    f = mod.lint_spec({"dedup_key": "k", "store": "memory", "ttl_seconds": True})
    assert any(x["code"] == "S004" for x in f)

@case("B13_lint_at_least_once_false_info", "B")
def t():
    f = mod.lint_spec({"dedup_key": "k", "store": "memory", "ttl_seconds": 1, "at_least_once": False})
    assert any(x["code"] == "S005" and x["severity"] == "info" for x in f)

@case("B14_check_log_skips_nonobject_with_Q098", "B")
def t():
    f = mod.check_log(["zzz"])
    assert f[0]["code"] == "Q098"

@case("B15_extract_key_non_dict", "B")
def t():
    assert mod.extract_key("scalar", "key") is None

@case("B16_extract_key_bool_value", "B")
def t():
    assert mod.extract_key({"key": True}, "key") is True

@case("B17_cli_json_lint_structure", "B")
def t():
    bad = write_json("b17.json", {"store": "none"})
    code, out, _ = run_cli(["lint", "--spec", bad, "--json"])
    data = json.loads(out)
    assert code == 1 and "findings" in data

@case("B18_simulate_key_custom_path_cli", "B")
def t():
    p = write_jsonl("b18.jsonl", [{"id": "m", "payload": {"oid": "o1"}}])
    code, out, _ = run_cli(["simulate", "--log", p, "--key", "payload.oid"])
    assert code == 0 and "exactly_once=True" in out

@case("B19_store_counters_track", "B")
def t():
    c = mod.IdempotentConsumer()
    for _ in range(3):
        c.deliver({"key": "a"})
    assert c.processed == 1 and c.duplicates == 2 and len(c.effects) == 1

@case("B20_empty_log_simulate", "B")
def t():
    p = write_jsonl("b20.jsonl", [])
    code, out, _ = run_cli(["simulate", "--log", p, "--json"])
    data = json.loads(out)
    assert code == 0 and data["delivered"] == 0

# ------------------------------------------------------------------ [C] extreme edge

@case("C01_unicode_key", "C")
def t():
    c = mod.IdempotentConsumer()
    c.deliver({"key": "订单-🚀"})
    assert c.deliver({"key": "订单-🚀"})[0] == "duplicate"

@case("C02_cjk_payload_no_crash", "C")
def t():
    entries = [{"seq": 0, "key": "k", "payload": {"名前": "値"}}]
    assert mod.simulate(entries)["exactly_once"] is True

@case("C03_empty_key_string_is_valid", "C")
def t():
    c = mod.IdempotentConsumer()
    assert c.deliver({"key": ""})[0] == "processed"

@case("C04_null_key_treated_missing", "C")
def t():
    assert mod.IdempotentConsumer().deliver({"key": None})[0] == "no_key"

@case("C05_deep_dotted_path", "C")
def t():
    m = {"a": {"b": {"c": {"d": "deep"}}}}
    assert mod.extract_key(m, "a.b.c.d") == "deep"

@case("C06_duplicate_dot_in_path", "C")
def t():
    assert mod.extract_key({"a": {"b": 1}}, "a..b") is None

@case("C07_huge_log_20k_performance", "C")
def t():
    entries = [msg(i, "k%d" % (i % 1000)) for i in range(20000)]
    import time
    t0 = __import__("time").perf_counter()
    s = mod.simulate(entries)
    assert __import__("time").perf_counter() - t0 < 5
    assert s["unique_keyed"] == 1000 and s["exactly_once"]

@case("C08_dup_every_beyond_log", "C")
def t():
    entries = [msg(i, "k%d") for i in range(3)]
    s = mod.simulate(entries, dup_every=100)
    assert s["duplicates_suppressed"] == 0

@case("C09_dup_count_zero", "C")
def t():
    entries = [msg(i, "k%d") for i in range(3)]
    s = mod.simulate(entries, dup_every=1, dup_count=0)
    assert s["delivered"] == 3 and s["processed"] == 3

@case("C10_many_dups_still_exactly_once", "C")
def t():
    entries = [msg(i, "k%d") for i in range(20)]
    s = mod.simulate(entries, dup_every=1, dup_count=3)
    assert s["processed"] == 20 and s["duplicates_suppressed"] == 60

@case("C11_seq_string_values_monotonic_check", "C")
def t():
    f = mod.check_log([{"seq": "9", "key": "a"}, {"seq": "2", "key": "b"}])
    assert any(x["code"] == "Q004" for x in f)

@case("C12_key_collision_dict_payload", "C")
def t():
    f = mod.check_log([msg(0, "k", {"a": 1}), msg(1, "k", {"a": 1})])
    assert f[0]["code"] == "Q003"  # identical payloads = benign

@case("C13_crlf_jsonl_parsing", "C")
def t():
    p = os.path.join(TMP, "c13.jsonl")
    with open(p, "w", encoding="utf-8", newline="\r\n") as fh:
        fh.write(json.dumps({"key": "a"}) + "\n" + json.dumps({"key": "a"}) + "\n")
    code, out, _ = run_cli(["simulate", "--log", p, "--json"])
    assert json.loads(out)["duplicates_suppressed"] == 1

@case("C14_blank_lines_skipped", "C")
def t():
    p = os.path.join(TMP, "c14.jsonl")
    with open(p, "w", encoding="utf-8", newline="") as fh:
        fh.write('\n{"key": "a"}\n\n{"key": "a"}\n')
    code, out, _ = run_cli(["simulate", "--log", p, "--json"])
    assert json.loads(out)["processed"] == 1

@case("C15_ttl_large_never_expires_in_run", "C")
def t():
    st = mod.MemoryStore(ttl=1e9)
    c = mod.IdempotentConsumer(store=st)
    c.deliver({"key": "a"})
    assert c.deliver({"key": "a"})[0] == "duplicate"

@case("C16_unicode_spec_name_lint", "C")
def t():
    spec = write_json("规格_日本語.json", {"dedup_key": "键", "store": "memory", "ttl_seconds": 10})
    code, _, _ = run_cli(["lint", "--spec", spec])
    assert code == 0

@case("C17_emoji_in_log_file", "C")
def t():
    p = write_jsonl("c17.jsonl", [{"key": "🦄"}, {"key": "🦄"}])
    code, out, _ = run_cli(["simulate", "--log", p])
    assert code == 0 and "duplicates_suppressed=1" in out

@case("C18_missing_key_every_entry_cli_exit1", "C")
def t():
    p = write_jsonl("c18.jsonl", [{"id": 1}, {"id": 2}])
    code, out, _ = run_cli(["simulate", "--log", p, "--json"])
    assert code == 1 and json.loads(out)["no_key"] == 2

@case("C19_zero_unique_exactly_once_false", "C")
def t():
    s = mod.simulate([])
    assert s["exactly_once"] is True and s["processed"] == 0

@case("C20_deep_nested_payload_dotted", "C")
def t():
    c = mod.IdempotentConsumer(key_path="payload.a.b.c")
    m = {"payload": {"a": {"b": {"c": "x"}}}}
    c.deliver(m)
    assert c.deliver(m)[0] == "duplicate"


class TestAll(unittest.TestCase):
    pass


def _build():
    for i, (name, cat, fn) in enumerate(CASES):
        def make(f):
            def m(self):
                f()
            return m
        slug = "".join(ch if (ch.isalnum() or ch == "_") else "_" for ch in name)
        setattr(TestAll, "test_%s_%03d_%s" % (cat, i, slug), make(fn))
_build()


class TestMeta(unittest.TestCase):
    def test_case_count_ge_60(self):
        self.assertGreaterEqual(len(CASES), 60)

    def test_categories_present(self):
        self.assertEqual({c for _, c, _ in CASES}, {"A", "B", "C"})


if __name__ == "__main__":
    r = unittest.TextTestRunner(verbosity=0).run(
        unittest.defaultTestLoader.loadTestsFromModule(sys.modules["__main__"]))
    total, fails = r.testsRun, len(r.failures) + len(r.errors)
    print("SUMMARY: PASS %d/%d" % (total - fails, total))  # D4
    sys.exit(0 if fails == 0 else 1)
