"""
config_admin.py - read and change the configuration tables. Every change is a NEW version row (who, when, why),
so history and rollback are free. Works the same from the VM, Cloud Shell or a Cloud Run job.

    python config_admin.py seed --by NAME                       # first time: copy the values now in config.py (never overwrites)
    python config_admin.py show [SOURCE] [--effective]            # current values (--effective: what a run really uses)
    python config_admin.py set SOURCE key=value [key=value ...] --by NAME [--note TEXT] [--dry-run]
    python config_admin.py set-global key=value ...  --by NAME
    python config_admin.py history SOURCE [--limit 10]
    python config_admin.py rollback SOURCE --as-of 2026-10-10T08:00:00Z --by NAME
    python config_admin.py enable SOURCE --by NAME   |   disable SOURCE --by NAME     # pause a source, scheduler untouched
    python config_admin.py recon-put RECON_ID definition.json --by NAME [--disabled]
    python config_admin.py recon-show [RECON_ID]

Examples
    python config_admin.py set PGW conflict_policy=supersede lookup_window_days=45 --by rishi --note "corrections expected"
    python config_admin.py set PGW df_max_workers=20 --by rishi
    python config_admin.py set PGW ignore_suffixes=none --by rishi      # explicit: no suffixes ignored (columns cannot be blank)
"""
import argparse
import dataclasses
import json
import sys
from datetime import datetime, timezone

import config_store as cs
from config import CTL_DATASET, KMS_KEY, LOCATION, PROJECT, SOURCES, BATCH_FILES, LEASE_MIN, MAX_ATTEMPTS, MAX_JOB_WAIT_MIN


def store() -> cs.ConfigStore:
    return cs.ConfigStore(cs.BqHelper(PROJECT, LOCATION, KMS_KEY), PROJECT, CTL_DATASET)


def code_defaults() -> dict:
    return {"MAX_ATTEMPTS": MAX_ATTEMPTS, "BATCH_FILES": BATCH_FILES, "LEASE_MIN": LEASE_MIN, "MAX_JOB_WAIT_MIN": MAX_JOB_WAIT_MIN}


def parse_changes(pairs, spec) -> dict:
    out = {}
    for p in pairs:
        if "=" not in p:
            raise cs.ConfigError(f"{p!r}: use key=value")
        k, v = p.split("=", 1)
        out[k.strip()] = cs.coerce(k.strip(), v, spec)
    return out


def show_row(row: dict):
    for k, v in row.items():
        if not k.startswith("_"):
            print(f"  {k:26} {v!r}")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="PRISM configuration tables")
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("seed"); s.add_argument("--by", required=True)
    s = sub.add_parser("show"); s.add_argument("source", nargs="?")
    s.add_argument("--effective", action="store_true", help="also print the values a run would actually use (NULL columns resolved)")
    s = sub.add_parser("set"); s.add_argument("source"); s.add_argument("pairs", nargs="+")
    s.add_argument("--by", required=True); s.add_argument("--note"); s.add_argument("--dry-run", action="store_true")
    s = sub.add_parser("set-global"); s.add_argument("pairs", nargs="+")
    s.add_argument("--by", required=True); s.add_argument("--note"); s.add_argument("--dry-run", action="store_true")
    s = sub.add_parser("history"); s.add_argument("source"); s.add_argument("--limit", type=int, default=10)
    s = sub.add_parser("rollback"); s.add_argument("source"); s.add_argument("--as-of", required=True); s.add_argument("--by", required=True)
    for name in ("enable", "disable"):
        s = sub.add_parser(name); s.add_argument("source"); s.add_argument("--by", required=True)
    s = sub.add_parser("recon-put"); s.add_argument("recon_id"); s.add_argument("file")
    s.add_argument("--by", required=True); s.add_argument("--note"); s.add_argument("--disabled", action="store_true")
    s = sub.add_parser("recon-show"); s.add_argument("recon_id", nargs="?")
    a = ap.parse_args(argv)

    st = store()
    try:
        if a.cmd == "seed":
            done = st.seed(SOURCES, code_defaults(), a.by)
            print("written:", done or "nothing (configuration already exists)")
        elif a.cmd == "show":
            g = st.latest_global()
            print("GLOBAL (changed %s by %s)" % (g and g["changed_at"], g and g["changed_by"])); show_row(g or {})
            for name in ([a.source] if a.source else st.sources()):
                r = st.latest_source(name)
                print(f"\nSOURCE {name} (changed {r and r['changed_at']} by {r and r['changed_by']})"); show_row(r or {})
                if a.effective and r:
                    res = st.resolve(name, KMS_KEY)
                    print(f"  --- effective (what a run uses; enabled={res.enabled}, version {res.version}) ---")
                    print(json.dumps({k: v for k, v in dataclasses.asdict(res.settings).items()}, indent=2, default=str))
        elif a.cmd in ("set", "set-global"):
            spec = cs.SOURCE_SPEC if a.cmd == "set" else cs.GLOBAL_SPEC
            changes = parse_changes(a.pairs, spec)
            if a.dry_run:                                   # validate against the current version, write nothing
                cur = st.latest_source(a.source) if a.cmd == "set" else st.latest_global()
                new = {k: v for k, v in (cur or {}).items() if k in spec}; new.update(changes)
                cs.validate_row(new, spec, "configuration after the change")
                d = cs.diff(cur, new)
            else:
                d = st.set_source(a.source, changes, a.by, a.note) if a.cmd == "set" else st.set_global(changes, a.by, a.note)
            for k, old, new in d:
                print(f"  {k}: {old!r} -> {new!r}")
            print(("dry run, nothing written" if a.dry_run else "new version written; used from the next run") if d else "no change")
        elif a.cmd == "history":
            for r in st.history(cs.T_SOURCE, "source_type", a.source, a.limit):
                print(f"{r['changed_at']}  {r['changed_by']:12} changed: {', '.join(r['_changed']) or '-':50} {r.get('note') or ''}")
        elif a.cmd == "rollback":
            as_of = datetime.fromisoformat(a.as_of.replace("Z", "+00:00"))
            old = st.rollback_source(a.source, as_of, a.by)
            print(f"restored the version of {old['changed_at']} as the newest version")
        elif a.cmd in ("enable", "disable"):
            st.set_source(a.source, {"enabled": a.cmd == "enable"}, a.by, a.cmd)
            print(f"{a.source} {a.cmd}d; takes effect on the next run")
        elif a.cmd == "recon-put":
            st.put_recon(a.recon_id, open(a.file, encoding="utf-8").read(), a.by, a.note, not a.disabled)
            print("recon definition stored (validated)")
        elif a.cmd == "recon-show":
            defs = st.latest_recons()
            for rid in ([a.recon_id] if a.recon_id else sorted(defs)):
                print(rid, "enabled" if defs[rid].get("enabled", True) else "DISABLED")
                if a.recon_id:
                    print(json.dumps(defs[rid], indent=2))
    except cs.ConfigError as e:
        print("ERROR:", e, file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
