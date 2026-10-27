"""
config_store.py - ALL settings live in BigQuery tables (VPDB_ADM), read at the start of every run.
The VM test mode and Cloud Run therefore run with the SAME settings; a change is one command and takes
effect on the next run - no image, no file upload.

Tables (append-only: a change inserts a NEW row, the newest row wins, older rows are the history)
  prism_config_global      one row per version   : max_attempts, batch_files, lease_min, max_job_wait_min
  prism_config_source      one row per source+version : paths, window, conflict policy, tables, Dataflow options, enabled
  prism_recon_definition   one row per recon+version  : the recon definition as JSON

Stays in code (needed BEFORE the tables can be read): project, location, control dataset, encryption key
(config.py / prism_settings.py). launch_mode is NOT a setting: it is how the program is started
(`--launch-mode vm` for the VM test, `flex` - the default - for Cloud Run).

NO DEFAULT VALUES. Every column must hold a value; a run stops with a message naming each empty column
instead of silently using something from the code. The tables themselves also reject blanks (NOT NULL in
ddl_config.sql). The one exception are the df_* Dataflow options (OPTIONAL below): empty means "do not pass
that option", so Dataflow's own default applies. `config_admin.py seed` copies the values currently in config.py ONCE, as a starting point to
review - after that the tables are the only source.
"""
import dataclasses
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from google.cloud import bigquery

S = bigquery.ScalarQueryParameter
T_GLOBAL, T_SOURCE, T_RECON = "prism_config_global", "prism_config_source", "prism_recon_definition"
BQ_TYPE = {str: "STRING", int: "INT64", float: "FLOAT64", bool: "BOOL"}

# column -> (python type, code variable in config.py / run_prism_batch.py)
GLOBAL_SPEC = {"max_attempts": (int, "MAX_ATTEMPTS"), "batch_files": (int, "BATCH_FILES"),
               "lease_min": (int, "LEASE_MIN"), "max_job_wait_min": (int, "MAX_JOB_WAIT_MIN")}

# column -> (python type, section, attribute). sections: src=SourceConfig  target=TargetConfig
#                                                          settings=SourceSettings  env=Dataflow environment
SOURCE_SPEC = {
    "enabled": (bool, None, None),
    "input_prefix": (str, "src", "input_prefix"), "processed_prefix": (str, "src", "processed_prefix"),
    "duplicate_prefix": (str, "src", "duplicate_prefix"), "quarantine_prefix": (str, "src", "quarantine_prefix"),
    "name_glob": (str, "src", "name_glob"), "conflict_policy": (str, "src", "conflict_policy"),
    "lookup_window_days": (int, "src", "lookup_window_days"), "min_age_seconds": (int, "src", "min_age_seconds"),
    "ignore_suffixes": (str, "src", "ignore_suffixes"),                       # comma separated, e.g. ".tmp,.part"
    "detail_table": (str, "target", "detail_table"), "staging_table": (str, "target", "staging_table"),
    "reject_table": (str, "settings", "reject_table"), "read_counts_table": (str, "settings", "read_counts_table"),
    "flex_template_gcs_path": (str, "settings", "flex_template_gcs_path"),
    "manifest_prefix": (str, "settings", "manifest_prefix"),
    "max_reject_ratio": (float, "settings", "max_reject_ratio"), "allow_empty_files": (bool, "settings", "allow_empty_files"),
    "skip_if_running": (bool, "settings", "skip_if_running"), "orphan_after_min": (int, "settings", "orphan_after_min"),
    "df_temp_location": (str, "env", "tempLocation"), "df_staging_location": (str, "env", "stagingLocation"),
    "df_service_account": (str, "env", "serviceAccountEmail"), "df_network": (str, "env", "network"),
    "df_subnetwork": (str, "env", "subnetwork"), "df_max_workers": (int, "env", "maxWorkers"),
    "df_machine_type": (str, "env", "machineType"),
}
META = ("changed_at", "changed_by", "note")
# The ONLY columns that may be empty: the Dataflow options. Empty = the option is not passed and DATAFLOW's own
# default applies (for example no worker limit, the default network, the default service account).
OPTIONAL = {col for col in SOURCE_SPEC if col.startswith("df_")}


class ConfigError(Exception):
    pass


# ------------------------------------------------------------------ validation / coercion
def _positive(v): return None if v >= 1 else "must be at least 1"
def _prefix(v): return None if v.endswith("/") and not v.startswith("/") else "must end with / and not start with /"
def _gs(v): return None if v.startswith("gs://") else "must start with gs://"
def _table(v): return None if len(v.split(".")) == 3 and all(v.split(".")) else "must be project.dataset.table"


RULES = {
    "max_attempts": _positive, "batch_files": _positive, "lease_min": _positive, "max_job_wait_min": _positive,
    "lookup_window_days": _positive, "orphan_after_min": _positive, "df_max_workers": _positive,
    "min_age_seconds": lambda v: None if v >= 0 else "must be 0 or more",
    "max_reject_ratio": lambda v: None if 0 <= v <= 1 else "must be between 0 and 1",
    "conflict_policy": lambda v: None if v in ("quarantine", "supersede") else "must be quarantine or supersede",
    "input_prefix": _prefix, "processed_prefix": _prefix, "duplicate_prefix": _prefix,
    "quarantine_prefix": _prefix, "manifest_prefix": _prefix,
    "name_glob": lambda v: None if v.strip() else "must not be empty",
    "flex_template_gcs_path": lambda v: None if v.startswith("gs://") and v.endswith(".json") else "must be a gs://....json path",
    "df_temp_location": _gs, "df_staging_location": _gs,
    "detail_table": _table, "staging_table": _table, "reject_table": _table, "read_counts_table": _table,
    "ignore_suffixes": lambda v: None if all(x.strip().startswith(".") for x in v.split(",") if x.strip()) else "each suffix must start with a dot",
}


def _type_ok(v, typ) -> bool:
    if typ is bool: return isinstance(v, bool)
    if typ is int: return isinstance(v, int) and not isinstance(v, bool)
    if typ is float: return isinstance(v, (int, float)) and not isinstance(v, bool)
    return isinstance(v, typ)


def validate_row(row: dict, spec: dict, label: str, require_all: bool = False) -> None:
    """Raises ConfigError listing EVERY problem. require_all=True: no column may be empty (except OPTIONAL)."""
    errs = []
    if require_all:
        empty = [c for c in spec if row.get(c) is None and c not in OPTIONAL]
        if empty:
            errs.append("empty (NULL) columns: " + ", ".join(empty) + ". No default values are used - give each a "
                        "value, e.g.  python config_admin.py set <SOURCE> <column>=<value> --by <you>")
    for col, v in row.items():
        if col in META or col in ("source_type", "recon_id"):
            continue
        if col not in spec:
            errs.append(f"unknown column {col!r}")
        elif v is None:
            continue
        elif not _type_ok(v, spec[col][0]):
            errs.append(f"{col}: expected {spec[col][0].__name__}, got {type(v).__name__} ({v!r})")
        elif col in RULES and RULES[col](v):
            errs.append(f"{col}={v!r}: {RULES[col](v)}")
    if errs:
        raise ConfigError(f"invalid {label}:\n  - " + "\n  - ".join(errs))


def coerce(col: str, text: str, spec: dict):
    """Command-line text -> typed value ('' or 'null' -> None)."""
    if col not in spec:
        raise ConfigError(f"unknown column {col!r}; valid: {sorted(spec)}")
    if col == "ignore_suffixes" and text is not None and text.strip().lower() in ("none", "[]"):
        return ""                                          # explicit "ignore no suffixes" (an empty value, not NULL)
    if text is None or text.strip().lower() in ("", "null", "none"):
        return None
    typ = spec[col][0]
    try:
        if typ is bool:
            low = text.strip().lower()
            if low in ("true", "yes", "1"): return True
            if low in ("false", "no", "0"): return False
            raise ValueError
        return typ(text)
    except ValueError:
        raise ConfigError(f"{col}: {text!r} is not a valid {typ.__name__}") from None


def diff(old: Optional[dict], new: dict) -> List[Tuple[str, object, object]]:
    old = old or {}
    return [(k, old.get(k), v) for k, v in new.items() if k not in META and old.get(k) != v]


# ------------------------------------------------------------------ rows <-> settings objects
def settings_from_row(source_type: str, row: dict, kms_key_name: str, launch_mode: str = "flex"):
    """Builds the SourceSettings of a source from ITS ROW ALONE. Nothing is taken from config.py SOURCES:
    the table decides which sources exist and what every setting is. Only the encryption key (needed to read
    the table at all) comes from code, and launch_mode from the command line."""
    from config import SourceSettings                     # imported here: these only define the object shapes
    from file_tracker import SourceConfig, TargetConfig
    kw = {"src": {}, "target": {}, "settings": {}}
    env = {}
    for col, (_, sec, attr) in SOURCE_SPEC.items():
        if sec is None:
            continue
        v = row.get(col)
        if sec == "env":
            if v is not None:                             # empty df_* option = not passed, Dataflow's default applies
                env[attr] = v
            continue
        if col == "ignore_suffixes":
            v = tuple(x.strip() for x in v.split(",") if x.strip())
        kw[sec][attr] = v
    return SourceSettings(src=SourceConfig(source_type=source_type, **kw["src"]), target=TargetConfig(**kw["target"]),
                          dataflow_env=env, launch_mode=launch_mode, kms_key_name=kms_key_name, **kw["settings"])


def settings_to_row(settings, enabled: bool = True) -> dict:
    """Inverse of build_settings: used by `seed` to write the code defaults into the table."""
    obj = {"src": settings.src, "target": settings.target, "settings": settings}
    row = {"enabled": enabled}
    for col, (_, sec, attr) in SOURCE_SPEC.items():
        if sec is None:
            continue
        if sec == "env":
            row[col] = settings.dataflow_env.get(attr)
            continue
        v = getattr(obj[sec], attr)
        row[col] = ",".join(v) if col == "ignore_suffixes" else v
    return row


# ------------------------------------------------------------------ BigQuery access
class BqHelper:
    """Small BigQuery wrapper that attaches the CMEK key to every job (org policy)."""
    def __init__(self, project: str, location: str, kms_key: Optional[str]):
        self.client = bigquery.Client(project=project, location=location)
        self.enc = bigquery.EncryptionConfiguration(kms_key_name=kms_key) if kms_key else None

    def run(self, sql, params=None):
        cfg = bigquery.QueryJobConfig(query_parameters=params or [])
        if self.enc is not None:
            cfg.destination_encryption_configuration = self.enc
        job = self.client.query(sql, job_config=cfg)
        job.result()
        return job

    def rows(self, sql, params=None):
        return list(self.run(sql, params).result())


@dataclass
class Resolved:
    globals: Dict[str, int]       # {"MAX_ATTEMPTS": .., "BATCH_FILES": .., ...}
    settings: object              # SourceSettings
    version: str                  # recorded in prism_runs.config_version
    enabled: bool


def _ts(v) -> str:
    return v.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%S") if isinstance(v, datetime) else str(v)


class ConfigStore:
    def __init__(self, bq, project: str, ctl_dataset: str):
        self.bq = bq
        self.where = f"{project}.{ctl_dataset}"
        self.t = lambda name: f"`{project}.{ctl_dataset}.{name}`"        # noqa: E731

    # ---------------- reads
    def sources(self) -> List[str]:
        """Every source that has configuration in the table."""
        return sorted(r.source_type for r in self.bq.rows(f"SELECT DISTINCT source_type FROM {self.t(T_SOURCE)}"))

    def latest_global(self) -> Optional[dict]:
        r = self.bq.rows(f"SELECT * FROM {self.t(T_GLOBAL)} WHERE TRUE QUALIFY ROW_NUMBER() OVER (ORDER BY changed_at DESC) = 1")
        return dict(r[0].items()) if r else None

    def latest_source(self, source_type: str) -> Optional[dict]:
        r = self.bq.rows(f"SELECT * FROM {self.t(T_SOURCE)} WHERE source_type = @s "
                         "QUALIFY ROW_NUMBER() OVER (ORDER BY changed_at DESC) = 1", [S("s", "STRING", source_type)])
        return dict(r[0].items()) if r else None

    def resolve(self, source_type: str, kms_key_name: str) -> Resolved:
        """Everything comes from the tables; kms_key_name (code) is only attached to the settings object."""
        g, s = self.latest_global(), self.latest_source(source_type)
        if g is None or s is None:
            missing = ", ".join(n for n, v in ((T_GLOBAL, g), (f"{T_SOURCE} row for {source_type!r}", s)) if v is None)
            known = self.sources() if g is not None else []
            hint = (f" Sources configured in the table: {known} (the name is case-sensitive)." if known else "")
            raise ConfigError(f"no configuration found ({missing}) in {self.where}.{hint} "
                              "If the tables are new, run:  python config_admin.py seed --by <you>")
        validate_row(g, GLOBAL_SPEC, "global configuration", require_all=True)
        validate_row(s, SOURCE_SPEC, f"configuration of source {source_type}", require_all=True)
        globs = {var: g[col] for col, (_, var) in GLOBAL_SPEC.items()}
        return Resolved(globs, settings_from_row(source_type, s, kms_key_name), f"global@{_ts(g['changed_at'])}|{source_type}@{_ts(s['changed_at'])}",
                        bool(s["enabled"]))

    def history(self, table: str, key_col: Optional[str], key: Optional[str], limit: int = 10) -> List[dict]:
        where, params = (f"WHERE {key_col} = @k", [S("k", "STRING", key)]) if key_col else ("WHERE TRUE", [])
        rows = self.bq.rows(f"SELECT * FROM {self.t(table)} {where} ORDER BY changed_at DESC LIMIT {int(limit) + 1}", params)
        rows = [dict(r.items()) for r in rows]
        for i, r in enumerate(rows):                       # what changed compared with the previous version
            prev = rows[i + 1] if i + 1 < len(rows) else None
            r["_changed"] = [k for k, _, _ in diff(prev, {k: v for k, v in r.items() if not k.startswith("_")})]
        return rows[:limit]

    # ---------------- writes (always INSERT a new version)
    def _insert(self, table: str, spec: dict, key_col: Optional[str], key: Optional[str], row: dict, by: str, note: Optional[str]):
        cols = ([key_col] if key_col else []) + list(spec)
        params = [S(c, "STRING", key) if c == key_col else S(c, BQ_TYPE[spec[c][0]], row.get(c)) for c in cols]
        params += [S("changed_by", "STRING", by), S("note", "STRING", note)]
        self.bq.run(f"INSERT INTO {self.t(table)} ({', '.join(cols)}, changed_at, changed_by, note) "
                    f"VALUES ({', '.join('@' + c for c in cols)}, CURRENT_TIMESTAMP(), @changed_by, @note)", params)

    def set_source(self, source_type: str, changes: dict, by: str, note: Optional[str] = None):
        cur = self.latest_source(source_type)
        if cur is None:
            raise ConfigError(f"source {source_type} has no configuration yet; run `seed` first")
        new = {k: v for k, v in cur.items() if k in SOURCE_SPEC}
        new.update(changes)
        validate_row(new, SOURCE_SPEC, f"configuration of source {source_type} after the change", require_all=True)
        d = diff(cur, new)
        if not d:
            return []
        self._insert(T_SOURCE, SOURCE_SPEC, "source_type", source_type, new, by, note)
        return d

    def set_global(self, changes: dict, by: str, note: Optional[str] = None):
        cur = self.latest_global()
        if cur is None:
            raise ConfigError("no global configuration yet; run `seed` first")
        new = {k: v for k, v in cur.items() if k in GLOBAL_SPEC}
        new.update(changes)
        validate_row(new, GLOBAL_SPEC, "global configuration after the change", require_all=True)
        d = diff(cur, new)
        if d:
            self._insert(T_GLOBAL, GLOBAL_SPEC, None, None, new, by, note)
        return d

    def rollback_source(self, source_type: str, as_of: datetime, by: str):
        """Insert a copy of the version that was current at `as_of` as the NEWEST version."""
        r = self.bq.rows(f"SELECT * FROM {self.t(T_SOURCE)} WHERE source_type = @s AND changed_at <= @t "
                         "QUALIFY ROW_NUMBER() OVER (ORDER BY changed_at DESC) = 1",
                         [S("s", "STRING", source_type), S("t", "TIMESTAMP", as_of)])
        if not r:
            raise ConfigError(f"no version of {source_type} at or before {as_of}")
        old = dict(r[0].items())
        row = {k: v for k, v in old.items() if k in SOURCE_SPEC}
        validate_row(row, SOURCE_SPEC, "the version being restored", require_all=True)
        self._insert(T_SOURCE, SOURCE_SPEC, "source_type", source_type, row, by, f"rollback to version of {old['changed_at']}")
        return old

    def seed(self, sources: dict, default_globals: Dict[str, int], by: str) -> List[str]:
        """Writes the code defaults ONCE; never overwrites existing configuration."""
        done = []
        if self.latest_global() is None:
            g = {col: default_globals[var] for col, (_, var) in GLOBAL_SPEC.items()}
            validate_row(g, GLOBAL_SPEC, "seed: global", require_all=True)
            self._insert(T_GLOBAL, GLOBAL_SPEC, None, None, g, by, "seeded from code defaults")
            done.append("global")
        for name, settings in sources.items():
            if self.latest_source(name) is None:
                row = settings_to_row(settings)
                validate_row(row, SOURCE_SPEC, f"seed: source {name}", require_all=True)
                self._insert(T_SOURCE, SOURCE_SPEC, "source_type", name, row, by, "seeded from code defaults")
                done.append(name)
        return done

    # ---------------- recon definitions
    def latest_recons(self) -> Dict[str, dict]:
        """recon_id -> parsed definition (the table's `enabled` overrides the JSON's)."""
        rows = self.bq.rows(f"SELECT recon_id, enabled, definition FROM {self.t(T_RECON)} WHERE TRUE "
                            "QUALIFY ROW_NUMBER() OVER (PARTITION BY recon_id ORDER BY changed_at DESC) = 1")
        out = {}
        for r in rows:
            d = json.loads(r.definition)
            d["recon_id"] = r.recon_id
            if r.enabled is not None:
                d["enabled"] = bool(r.enabled)
            out[r.recon_id] = d
        return out

    def put_recon(self, recon_id: str, definition_text: str, by: str, note: Optional[str] = None, enabled: bool = True):
        import recon_logic as rl                          # pure module, same checks the runner applies
        d = json.loads(definition_text)
        if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", recon_id):
            raise ConfigError("recon_id must be letters, digits and underscore")
        d["recon_id"] = recon_id
        try:
            rl.validate_definition(d)
        except ValueError as e:
            raise ConfigError(str(e)) from None
        self.bq.run(f"INSERT INTO {self.t(T_RECON)} (recon_id, enabled, definition, changed_at, changed_by, note) "
                    "VALUES (@recon_id, @enabled, @definition, CURRENT_TIMESTAMP(), @changed_by, @note)",
                    [S("recon_id", "STRING", recon_id), S("enabled", "BOOL", enabled),
                     S("definition", "STRING", json.dumps(d)), S("changed_by", "STRING", by), S("note", "STRING", note)])
