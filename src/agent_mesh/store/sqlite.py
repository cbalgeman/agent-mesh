"""SQLite projection store for agent-mesh events."""
from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

MESSAGE_TABLES = (
    "messages",
    "message_refs",
    "message_source_context_refs",
    "message_causal_edges",
    "message_source_selection",
    "events_seen",
    "meta",
)
DECISION_TABLES = (
    "decisions",
    "decision_aliases",
    "decision_globs",
    "decision_checks",
    "decision_verifications",
    "decision_assumptions",
    "decision_evidence",
    "decision_references_in_code",
    "decision_tags",
)
BACKLOG_TABLES = (
    "backlog_items",
    "backlog_item_links",
    "backlog_events",
)
DISPATCH_TABLES = (
    "dispatch_runs",
    "dispatch_leases",
)
ALL_TABLES = MESSAGE_TABLES + DECISION_TABLES + BACKLOG_TABLES + DISPATCH_TABLES


class StoreError(RuntimeError):
    """Raised for projection-store errors."""


@dataclass(frozen=True)
class MessageRow:
    id: str
    kind: str
    thread_id: str
    sender: str
    title: str | None
    summary: str | None
    status: str
    created_utc: str
    body: str


def connect(db_path: str | Path) -> sqlite3.Connection:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


@contextmanager
def transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    with conn:
        yield conn


def initialize_schema(conn: sqlite3.Connection) -> None:
    # Existing projection DBs may already have a pre-source-provenance messages
    # table. Add new columns before CREATE INDEX statements reference them.
    _migrate_messages_source_schema(conn)
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS messages (
          id              TEXT PRIMARY KEY,
          kind            TEXT NOT NULL,
          schema_version  INTEGER NOT NULL,
          thread_id       TEXT NOT NULL,
          request_id      TEXT,
          parent_id       TEXT,
          sender          TEXT NOT NULL,
          recipients_json TEXT,
          feature_id      TEXT,
          title           TEXT,
          summary         TEXT,
          body_preview    TEXT,
          body_sha        TEXT NOT NULL,
          body_path       TEXT,
          body_bytes      INTEGER NOT NULL,
          body_media_type TEXT DEFAULT 'text/markdown',
          body_authority  TEXT NOT NULL DEFAULT 'unknown',
          body_fidelity   TEXT,
          status          TEXT NOT NULL DEFAULT 'open',
          resolution      TEXT,
          resolved_utc    TEXT,
          claimed_by      TEXT,
          claimed_utc     TEXT,
          has_fenced_json INTEGER NOT NULL DEFAULT 0,
          json_packet_type TEXT,
          created_utc     TEXT NOT NULL,
          updated_utc     TEXT NOT NULL,
          event_seq       INTEGER NOT NULL,
          source_file     TEXT,
          source_line_start INTEGER,
          source_line_end   INTEGER,
          import_batch_id TEXT,
          meta_json       TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_messages_status      ON messages(status);
        CREATE INDEX IF NOT EXISTS idx_messages_recipient   ON messages(recipients_json);
        CREATE INDEX IF NOT EXISTS idx_messages_feature     ON messages(feature_id);
        CREATE INDEX IF NOT EXISTS idx_messages_created     ON messages(created_utc);
        CREATE INDEX IF NOT EXISTS idx_messages_request_id  ON messages(request_id);
        CREATE INDEX IF NOT EXISTS idx_messages_thread      ON messages(thread_id);
        CREATE INDEX IF NOT EXISTS idx_messages_kind_status ON messages(kind, status);
        CREATE INDEX IF NOT EXISTS idx_messages_body_authority ON messages(body_authority);
        CREATE INDEX IF NOT EXISTS idx_messages_body_fidelity  ON messages(body_fidelity);

        CREATE TABLE IF NOT EXISTS message_refs (
          message_id TEXT NOT NULL,
          ref_type   TEXT NOT NULL,
          ref_value  TEXT NOT NULL,
          PRIMARY KEY (message_id, ref_type, ref_value)
        );
        CREATE INDEX IF NOT EXISTS idx_refs_value ON message_refs(ref_value);

        CREATE TABLE IF NOT EXISTS message_source_context_refs (
          message_id      TEXT NOT NULL,
          ref_index       INTEGER NOT NULL,
          channel         TEXT,
          source_kind     TEXT,
          source_id       TEXT,
          source_event_id TEXT,
          source_uri      TEXT,
          role            TEXT,
          observed_utc    TEXT,
          confidence      REAL,
          event_seq       INTEGER NOT NULL,
          meta_json       TEXT,
          PRIMARY KEY (message_id, ref_index)
        );
        CREATE INDEX IF NOT EXISTS idx_source_refs_channel
          ON message_source_context_refs(channel);
        CREATE INDEX IF NOT EXISTS idx_source_refs_source_id
          ON message_source_context_refs(source_id);
        CREATE INDEX IF NOT EXISTS idx_source_refs_event_id
          ON message_source_context_refs(source_event_id);

        CREATE TABLE IF NOT EXISTS message_causal_edges (
          message_id  TEXT NOT NULL,
          edge_index  INTEGER NOT NULL,
          relation    TEXT NOT NULL,
          from_ref    TEXT,
          to_ref      TEXT,
          confidence  REAL,
          event_seq   INTEGER NOT NULL,
          meta_json   TEXT,
          PRIMARY KEY (message_id, edge_index)
        );
        CREATE INDEX IF NOT EXISTS idx_causal_edges_relation ON message_causal_edges(relation);
        CREATE INDEX IF NOT EXISTS idx_causal_edges_from ON message_causal_edges(from_ref);
        CREATE INDEX IF NOT EXISTS idx_causal_edges_to ON message_causal_edges(to_ref);

        CREATE TABLE IF NOT EXISTS message_source_selection (
          message_id       TEXT PRIMARY KEY,
          mode             TEXT NOT NULL,
          confidence       REAL NOT NULL,
          selected_by      TEXT NOT NULL,
          requires_review  INTEGER NOT NULL,
          event_seq        INTEGER NOT NULL,
          meta_json        TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_source_selection_mode ON message_source_selection(mode);
        CREATE INDEX IF NOT EXISTS idx_source_selection_review ON message_source_selection(requires_review);

        CREATE TABLE IF NOT EXISTS events_seen (
          last_event_seq INTEGER NOT NULL,
          rebuilt_utc    TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS meta (
          key TEXT PRIMARY KEY,
          value TEXT
        );

        CREATE TABLE IF NOT EXISTS decisions (
          dec_ulid           TEXT PRIMARY KEY,
          human_id           TEXT NOT NULL,
          parent_human_id    TEXT,
          title              TEXT NOT NULL,
          tier               TEXT NOT NULL,
          status             TEXT NOT NULL,
          enforcement_mode   TEXT NOT NULL,
          owner              TEXT,
          body_sha           TEXT NOT NULL,
          body_path          TEXT,
          body_bytes         INTEGER NOT NULL,
          body_media_type    TEXT DEFAULT 'text/markdown',
          superseded_by      TEXT,
          supersedes         TEXT,
          proposed_utc       TEXT NOT NULL,
          accepted_utc       TEXT,
          in_force_utc       TEXT,
          retired_utc        TEXT,
          last_verified_utc  TEXT,
          drift_risk         TEXT,
          event_seq          INTEGER NOT NULL,
          meta_json          TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_decisions_human_id        ON decisions(human_id);
        CREATE INDEX IF NOT EXISTS idx_decisions_parent_human_id ON decisions(parent_human_id);
        CREATE INDEX IF NOT EXISTS idx_decisions_status          ON decisions(status);
        CREATE INDEX IF NOT EXISTS idx_decisions_tier            ON decisions(tier);
        CREATE INDEX IF NOT EXISTS idx_decisions_owner           ON decisions(owner);

        CREATE TABLE IF NOT EXISTS decision_aliases (
          human_id   TEXT NOT NULL,
          dec_ulid   TEXT NOT NULL,
          is_primary INTEGER NOT NULL DEFAULT 0,
          PRIMARY KEY (human_id, dec_ulid)
        );
        CREATE INDEX IF NOT EXISTS idx_decision_aliases_ulid ON decision_aliases(dec_ulid);

        CREATE TABLE IF NOT EXISTS decision_globs (
          dec_ulid   TEXT NOT NULL,
          pattern    TEXT NOT NULL,
          kind       TEXT NOT NULL,
          PRIMARY KEY (dec_ulid, pattern, kind)
        );
        CREATE INDEX IF NOT EXISTS idx_decision_globs_kind ON decision_globs(kind);

        CREATE TABLE IF NOT EXISTS decision_checks (
          dec_ulid   TEXT NOT NULL,
          check_name TEXT NOT NULL,
          PRIMARY KEY (dec_ulid, check_name)
        );

        CREATE TABLE IF NOT EXISTS decision_verifications (
          dec_ulid           TEXT NOT NULL,
          command            TEXT NOT NULL,
          expected_signal    TEXT NOT NULL,
          runtime_cost       TEXT,
          drift_risk         TEXT,
          last_verified_utc  TEXT,
          last_outcome       TEXT,
          last_event_id      TEXT,
          PRIMARY KEY (dec_ulid, command)
        );

        CREATE TABLE IF NOT EXISTS decision_assumptions (
          dec_ulid           TEXT NOT NULL,
          assumption_id      TEXT NOT NULL,
          text               TEXT NOT NULL,
          references_json    TEXT,
          status             TEXT NOT NULL DEFAULT 'active',
          invalidated_event_id TEXT,
          PRIMARY KEY (dec_ulid, assumption_id)
        );

        CREATE TABLE IF NOT EXISTS decision_evidence (
          dec_ulid       TEXT NOT NULL,
          evidence_kind  TEXT NOT NULL,
          ref_value      TEXT NOT NULL,
          PRIMARY KEY (dec_ulid, evidence_kind, ref_value)
        );
        CREATE INDEX IF NOT EXISTS idx_decision_evidence_value ON decision_evidence(ref_value);

        CREATE TABLE IF NOT EXISTS decision_references_in_code (
          dec_ulid       TEXT NOT NULL,
          file_path      TEXT NOT NULL,
          line_start     INTEGER NOT NULL,
          line_end       INTEGER NOT NULL,
          reference_form TEXT NOT NULL,
          commit_sha     TEXT,
          scanner_run_id TEXT NOT NULL,
          PRIMARY KEY (dec_ulid, file_path, line_start, line_end, reference_form)
        );
        CREATE INDEX IF NOT EXISTS idx_dec_refs_file    ON decision_references_in_code(file_path);
        CREATE INDEX IF NOT EXISTS idx_dec_refs_scanner ON decision_references_in_code(scanner_run_id);

        CREATE TABLE IF NOT EXISTS decision_tags (
          dec_ulid TEXT NOT NULL,
          tag      TEXT NOT NULL,
          PRIMARY KEY (dec_ulid, tag)
        );
        CREATE INDEX IF NOT EXISTS idx_decision_tags_tag ON decision_tags(tag);
        CREATE TABLE IF NOT EXISTS backlog_items (
          id                     TEXT PRIMARY KEY,
          title                  TEXT NOT NULL,
          item_type              TEXT,
          summary                TEXT,
          root_cause_summary     TEXT,
          architectural_category TEXT,
          status                 TEXT NOT NULL,
          priority               TEXT,
          launch_scope           TEXT,
          release_phase          TEXT,
          production_state       TEXT,
          disposition            TEXT,
          owner_hint             TEXT,
          lane                   TEXT,
          notes                  TEXT,
          refs_json              TEXT,
          created_utc            TEXT NOT NULL,
          updated_utc            TEXT NOT NULL,
          event_seq              INTEGER NOT NULL,
          meta_json              TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_backlog_items_status ON backlog_items(status);
        CREATE INDEX IF NOT EXISTS idx_backlog_items_lane ON backlog_items(lane);
        CREATE INDEX IF NOT EXISTS idx_backlog_items_priority ON backlog_items(priority);

        CREATE TABLE IF NOT EXISTS backlog_item_links (
          link_event_id TEXT NOT NULL PRIMARY KEY,
          item_id       TEXT NOT NULL,
          ref_type      TEXT NOT NULL,
          ref_value     TEXT NOT NULL,
          created_utc   TEXT NOT NULL,
          event_seq     INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_backlog_links_ref ON backlog_item_links(ref_type, ref_value);

        CREATE TABLE IF NOT EXISTS backlog_events (
          event_id     TEXT PRIMARY KEY,
          item_id      TEXT NOT NULL,
          event_type   TEXT NOT NULL,
          actor        TEXT NOT NULL,
          created_utc  TEXT NOT NULL,
          details_json TEXT,
          event_seq    INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_backlog_events_item ON backlog_events(item_id, event_seq);

        CREATE TABLE IF NOT EXISTS dispatch_runs (
          run_id                    TEXT PRIMARY KEY,
          run_mode                  TEXT NOT NULL,
          input_message_id          TEXT NOT NULL,
          target_agent              TEXT NOT NULL,
          gen_ai_system             TEXT,
          model                     TEXT,
          session_key               TEXT,
          session_key_source        TEXT,
          session_uuid              TEXT,
          wave                      TEXT,
          classification            TEXT,
          gate                      TEXT,
          gate_reason_code          TEXT,
          requires_gate_json        TEXT,
          grounding_complete        INTEGER,
          grounding_digest          TEXT,
          plan_artifact_hash        TEXT,
          adapter_capabilities_json TEXT,
          target_event_seq          INTEGER,
          response_mode             TEXT,
          status                    TEXT NOT NULL,
          planned                   INTEGER NOT NULL DEFAULT 0,
          block_reason_codes_json   TEXT,
          missing_count             INTEGER,
          thread_id                 TEXT,
          output_message_id         TEXT,
          error_class               TEXT,
          input_tokens              INTEGER,
          cache_read_input_tokens   INTEGER,
          cache_creation_input_tokens INTEGER,
          total_input_tokens        INTEGER,
          planned_utc               TEXT,
          started_utc               TEXT,
          completed_utc             TEXT,
          failed_utc                TEXT,
          event_seq                 INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_dispatch_runs_input ON dispatch_runs(input_message_id);
        CREATE INDEX IF NOT EXISTS idx_dispatch_runs_agent_status ON dispatch_runs(target_agent, status);
        CREATE INDEX IF NOT EXISTS idx_dispatch_runs_session ON dispatch_runs(session_key);

        CREATE TABLE IF NOT EXISTS dispatch_leases (
          lease_id             TEXT PRIMARY KEY,
          run_id               TEXT NOT NULL,
          input_message_id     TEXT NOT NULL,
          target_agent         TEXT NOT NULL,
          session_uuid         TEXT,
          ttl_seconds          INTEGER,
          status               TEXT NOT NULL,
          reason               TEXT,
          superseded_by_run_id TEXT,
          created_utc          TEXT,
          released_utc         TEXT,
          event_seq            INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_dispatch_leases_run ON dispatch_leases(run_id);
        CREATE UNIQUE INDEX IF NOT EXISTS uq_dispatch_leases_open
          ON dispatch_leases(input_message_id, target_agent) WHERE status='open';
        """
    )
    _migrate_messages_source_schema(conn)
    _migrate_backlog_item_links_schema(conn)
    conn.execute(
        "INSERT INTO meta(key, value) VALUES ('schema_version', '1') "
        "ON CONFLICT(key) DO NOTHING"
    )
    conn.execute(
        "INSERT INTO meta(key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (
            "domain_versions",
            '{"messages":2,"source_provenance":1,"decisions":1,"backlog":1,"dispatch":1}',
        ),
    )
    if conn.execute("SELECT COUNT(*) FROM events_seen").fetchone()[0] == 0:
        conn.execute(
            "INSERT INTO events_seen(last_event_seq, rebuilt_utc) VALUES (0, '1970-01-01T00:00:00Z')"
        )


def _migrate_backlog_item_links_schema(conn: sqlite3.Connection) -> None:
    columns = [row["name"] for row in conn.execute("PRAGMA table_info(backlog_item_links)")]
    if not columns or "link_event_id" in columns:
        return
    conn.executescript(
        """
        ALTER TABLE backlog_item_links RENAME TO backlog_item_links_legacy;
        CREATE TABLE backlog_item_links (
          link_event_id TEXT NOT NULL PRIMARY KEY,
          item_id       TEXT NOT NULL,
          ref_type      TEXT NOT NULL,
          ref_value     TEXT NOT NULL,
          created_utc   TEXT NOT NULL,
          event_seq     INTEGER NOT NULL
        );
        INSERT INTO backlog_item_links(link_event_id, item_id, ref_type, ref_value, created_utc, event_seq)
        SELECT 'legacy:' || event_seq || ':' || item_id || ':' || ref_type || ':' || ref_value,
               item_id, ref_type, ref_value, created_utc, event_seq
        FROM backlog_item_links_legacy;
        DROP TABLE backlog_item_links_legacy;
        CREATE INDEX IF NOT EXISTS idx_backlog_links_ref ON backlog_item_links(ref_type, ref_value);
        """
    )


def _migrate_messages_source_schema(conn: sqlite3.Connection) -> None:
    columns = [row["name"] for row in conn.execute("PRAGMA table_info(messages)")]
    if not columns:
        return
    if "body_authority" not in columns:
        conn.execute(
            "ALTER TABLE messages ADD COLUMN body_authority TEXT NOT NULL DEFAULT 'unknown'"
        )
    if "body_fidelity" not in columns:
        conn.execute("ALTER TABLE messages ADD COLUMN body_fidelity TEXT")


def reset_schema(conn: sqlite3.Connection) -> None:
    for table in reversed(ALL_TABLES):
        conn.execute(f"DROP TABLE IF EXISTS {table}")
    initialize_schema(conn)


def get_last_event_seq(conn: sqlite3.Connection) -> int:
    initialize_schema(conn)
    row = conn.execute("SELECT last_event_seq FROM events_seen LIMIT 1").fetchone()
    return int(row["last_event_seq"]) if row else 0


def set_last_event_seq(conn: sqlite3.Connection, event_seq: int, rebuilt_utc: str) -> None:
    conn.execute("DELETE FROM events_seen")
    conn.execute(
        "INSERT INTO events_seen(last_event_seq, rebuilt_utc) VALUES (?, ?)",
        (event_seq, rebuilt_utc),
    )


def set_meta(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        "INSERT INTO meta(key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, value),
    )


def get_meta(conn: sqlite3.Connection, key: str) -> str | None:
    row = conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
    return str(row["value"]) if row is not None and row["value"] is not None else None


def json_dumps(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def json_loads(value: str | None, default: Any) -> Any:
    if value is None or value == "":
        return default
    return json.loads(value)


def resolve_decision(conn: sqlite3.Connection, identifier: str) -> str | None:
    if identifier.startswith("dec_"):
        row = conn.execute(
            "SELECT dec_ulid FROM decisions WHERE dec_ulid = ?", (identifier,)
        ).fetchone()
        return str(row["dec_ulid"]) if row else None
    row = conn.execute(
        "SELECT dec_ulid FROM decision_aliases WHERE human_id = ? ORDER BY is_primary DESC",
        (identifier,),
    ).fetchone()
    return str(row["dec_ulid"]) if row else None


def resolve_message(conn: sqlite3.Connection, identifier: str) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM messages WHERE id = ?", (identifier,)).fetchone()


def body_from_message_row(row: sqlite3.Row) -> str:
    meta = json_loads(row["meta_json"], {})
    return str(meta.get("body", ""))


def fetch_message_body(conn: sqlite3.Connection, message_id: str) -> str | None:
    row = resolve_message(conn, message_id)
    if row is None:
        return None
    return body_from_message_row(row)


def canonical_table_dump(conn: sqlite3.Connection, table: str) -> list[dict[str, Any]]:
    rows = conn.execute(f"SELECT * FROM {table}").fetchall()
    natural = _natural_order_key(table)
    return [
        {key: row[key] for key in row.keys()}
        for row in sorted(rows, key=lambda row: tuple(row[key] for key in natural))
    ]


def dump_all_tables(conn: sqlite3.Connection) -> dict[str, list[dict[str, Any]]]:
    return {table: canonical_table_dump(conn, table) for table in ALL_TABLES}


def _natural_order_key(table: str) -> tuple[str, ...]:
    return {
        "messages": ("event_seq", "id"),
        "message_refs": ("message_id", "ref_type", "ref_value"),
        "message_source_context_refs": ("message_id", "ref_index"),
        "message_causal_edges": ("message_id", "edge_index"),
        "message_source_selection": ("message_id",),
        "events_seen": ("last_event_seq",),
        "meta": ("key",),
        "decisions": ("event_seq", "dec_ulid"),
        "decision_aliases": ("human_id", "dec_ulid"),
        "decision_globs": ("dec_ulid", "pattern", "kind"),
        "decision_checks": ("dec_ulid", "check_name"),
        "decision_verifications": ("dec_ulid", "command"),
        "decision_assumptions": ("dec_ulid", "assumption_id"),
        "decision_evidence": ("dec_ulid", "evidence_kind", "ref_value"),
        "decision_references_in_code": (
            "dec_ulid",
            "file_path",
            "line_start",
            "line_end",
            "reference_form",
        ),
        "decision_tags": ("dec_ulid", "tag"),
        "backlog_items": ("event_seq", "id"),
        "backlog_item_links": ("event_seq", "link_event_id"),
        "backlog_events": ("event_seq", "event_id"),
        "dispatch_runs": ("run_id",),
        "dispatch_leases": ("lease_id",),
    }[table]


def resolve_dispatch_run(conn: sqlite3.Connection, identifier: str) -> str | None:
    """Resolve a run identifier to a ``run_id``. Accepts a full ``run_<ulid>`` or a unique suffix."""
    row = conn.execute(
        "SELECT run_id FROM dispatch_runs WHERE run_id = ?", (identifier,)
    ).fetchone()
    if row is not None:
        return str(row["run_id"])
    matches = conn.execute(
        "SELECT run_id FROM dispatch_runs WHERE run_id LIKE ? ORDER BY run_id",
        (f"%{identifier}",),
    ).fetchall()
    if len(matches) == 1:
        return str(matches[0]["run_id"])
    return None
