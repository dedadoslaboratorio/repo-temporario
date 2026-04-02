#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
pg_metrics.py — Coleta completa de metricas do PostgreSQL para o New Relic
Compativel com Python 2.7

Grupos:
  1. Atividade e conexoes
  2. Performance de queries
  3. Cache e I/O
  4. Transacoes
  5. Tuplas
  6. Locks
  7. Tamanhos
  8. Vacuum e manutencao
  9. Background writer e checkpoints
  10. Replicacao

Uso: python pg_metrics.py
"""

import os
import sys
import json
import time
import socket

try:
    import ConfigParser
except ImportError:
    import configparser as ConfigParser

import psycopg2

# ── HTTP helper (Python 2.7 e 3) ─────────────
try:
    import urllib2
    def http_post(url, headers, data):
        req = urllib2.Request(url, data=data)
        for k, v in headers.items():
            req.add_header(k, v)
        try:
            return urllib2.urlopen(req, timeout=15).getcode()
        except urllib2.HTTPError as e:
            return e.code
        except Exception as e:
            print("Erro HTTP: %s" % str(e))
            return 0
except ImportError:
    import urllib.request
    def http_post(url, headers, data):
        req = urllib.request.Request(url, data=data, headers=headers)
        try:
            return urllib.request.urlopen(req, timeout=15).getcode()
        except urllib.error.HTTPError as e:
            return e.code
        except Exception as e:
            print("Erro HTTP: %s" % str(e))
            return 0


# ═════════════════════════════════════════════
# CONFIGURACAO
# ═════════════════════════════════════════════

CONFIG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.ini")

if not os.path.exists(CONFIG_FILE):
    print("Arquivo config.ini nao encontrado: %s" % CONFIG_FILE)
    sys.exit(1)

cfg = ConfigParser.ConfigParser()
cfg.read(CONFIG_FILE)

HOST         = cfg.get("postgresql",      "host")
PORT         = cfg.getint("postgresql",   "port")
DBNAME       = cfg.get("postgresql",      "dbname")
USER         = cfg.get("postgresql",      "user")
PASSWORD     = cfg.get("postgresql",      "password")
THRESHOLD_MS = cfg.getint("slow_queries", "threshold_ms")
LIMIT        = cfg.getint("slow_queries", "limit")
NR_LICENSE   = cfg.get("newrelic",        "license_key")
NR_ENDPOINT  = cfg.get("newrelic",        "api_endpoint")
NR_APP       = cfg.get("newrelic",        "app_name")
HOSTNAME     = socket.gethostname()


# ═════════════════════════════════════════════
# HELPERS
# ═════════════════════════════════════════════

def gauge(name, value, attributes, timestamp):
    if value is None:
        value = 0
    return {
        "name":       name,
        "type":       "gauge",
        "value":      value,
        "timestamp":  timestamp,
        "attributes": attributes,
    }

def base_dims(**extra):
    d = {"app.name": NR_APP, "host": HOSTNAME, "database": DBNAME}
    d.update(extra)
    return d

def run_query(cur, sql, params=None):
    try:
        cur.execute(sql, params)
        return cur.fetchall()
    except Exception as e:
        print("  [WARN] Erro na query: %s" % str(e))
        return []

def safe_float(v):
    try:
        return float(v) if v is not None else 0.0
    except:
        return 0.0

def safe_int(v):
    try:
        return int(v) if v is not None else 0
    except:
        return 0


# ═════════════════════════════════════════════
# 1. ATIVIDADE E CONEXOES
# ═════════════════════════════════════════════

def collect_connections(cur, now_ms):
    print("  [1] Atividade e conexoes...")
    metrics = []

    rows = run_query(cur, """
        SELECT
            COALESCE(state, 'unknown')                                   AS state,
            COUNT(*)                                                     AS total,
            MAX(EXTRACT(EPOCH FROM (now() - state_change)))::int         AS max_duration_s
        FROM pg_stat_activity
        WHERE pid <> pg_backend_pid()
        GROUP BY state
    """)

    total_conns = 0
    for row in rows:
        state   = row[0]
        total   = safe_int(row[1])
        max_dur = safe_int(row[2])
        total_conns += total
        dims = base_dims(state=state)
        metrics.append(gauge("postgresql.connections.by_state",      total,   dims, now_ms))
        metrics.append(gauge("postgresql.connections.max_duration_s", max_dur, dims, now_ms))

    metrics.append(gauge("postgresql.connections.total", total_conns, base_dims(), now_ms))

    rows2 = run_query(cur, "SELECT setting::int FROM pg_settings WHERE name = 'max_connections'")
    if rows2:
        max_conn  = safe_int(rows2[0][0])
        usage_pct = round(total_conns / float(max_conn) * 100, 2) if max_conn > 0 else 0.0
        metrics.append(gauge("postgresql.connections.max_connections", max_conn,  base_dims(), now_ms))
        metrics.append(gauge("postgresql.connections.usage_pct",       usage_pct, base_dims(), now_ms))

    rows3 = run_query(cur, """
        SELECT COUNT(*) FROM pg_stat_activity
        WHERE state = 'active'
          AND pid <> pg_backend_pid()
          AND query NOT ILIKE '%pg_stat_activity%'
    """)
    if rows3:
        metrics.append(gauge("postgresql.connections.running_queries", safe_int(rows3[0][0]), base_dims(), now_ms))

    return metrics


# ═════════════════════════════════════════════
# 2. PERFORMANCE DE QUERIES
# ═════════════════════════════════════════════

def collect_query_performance(cur, now_ms):
    print("  [2] Performance de queries...")
    metrics = []

    ext = run_query(cur, "SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements' LIMIT 1")
    if not ext:
        print("  [WARN] pg_stat_statements nao instalada. Pulando grupo.")
        return metrics

    SQL_BASE = """
        SELECT
            queryid::text,
            calls,
            round(total_exec_time::numeric, 2),
            round(mean_exec_time::numeric, 2),
            round(total_plan_time::numeric, 2),
            round(mean_plan_time::numeric, 2),
            rows,
            LEFT(query, 120)
        FROM pg_stat_statements
        WHERE calls > 0
    """

    def parse_rows(rows, prefix):
        result = []
        for row in rows:
            dims = base_dims(query_id=row[0], query_text=(row[7] or "").replace("\n", " "))
            result += [
                gauge("%s.calls"         % prefix, safe_int(row[1]),   dims, now_ms),
                gauge("%s.total_exec_ms" % prefix, safe_float(row[2]), dims, now_ms),
                gauge("%s.mean_exec_ms"  % prefix, safe_float(row[3]), dims, now_ms),
                gauge("%s.total_plan_ms" % prefix, safe_float(row[4]), dims, now_ms),
                gauge("%s.mean_plan_ms"  % prefix, safe_float(row[5]), dims, now_ms),
                gauge("%s.rows"          % prefix, safe_int(row[6]),   dims, now_ms),
            ]
        return result

    rows = run_query(cur, SQL_BASE + " AND mean_exec_time >= %s ORDER BY mean_exec_time DESC LIMIT %s", (THRESHOLD_MS, LIMIT))
    print("  %d queries lentas encontradas." % len(rows))
    metrics += parse_rows(rows, "postgresql.query.slow")

    rows = run_query(cur, SQL_BASE + " ORDER BY calls DESC LIMIT %s", (LIMIT,))
    metrics += parse_rows(rows, "postgresql.query.top_calls")

    rows = run_query(cur, SQL_BASE + " ORDER BY total_exec_time DESC LIMIT %s", (LIMIT,))
    metrics += parse_rows(rows, "postgresql.query.top_time")

    return metrics


# ═════════════════════════════════════════════
# 3. CACHE E I/O
# ═════════════════════════════════════════════

def collect_cache_io(cur, now_ms):
    print("  [3] Cache e I/O...")
    metrics = []

    rows = run_query(cur, """
        SELECT datname, blks_hit, blks_read, blk_read_time, blk_write_time
        FROM pg_stat_database
        WHERE datname NOT IN ('template0', 'template1')
    """)

    for row in rows:
        dims      = base_dims(database=row[0])
        blks_hit  = safe_int(row[1])
        blks_read = safe_int(row[2])
        total     = blks_hit + blks_read
        hit_ratio = round(blks_hit / float(total) * 100, 2) if total > 0 else 0.0
        metrics += [
            gauge("postgresql.cache.hit_ratio_pct", hit_ratio,          dims, now_ms),
            gauge("postgresql.cache.blks_hit",       blks_hit,           dims, now_ms),
            gauge("postgresql.cache.blks_read",      blks_read,          dims, now_ms),
            gauge("postgresql.io.blk_read_time_ms",  safe_float(row[3]), dims, now_ms),
            gauge("postgresql.io.blk_write_time_ms", safe_float(row[4]), dims, now_ms),
        ]

    return metrics


# ═════════════════════════════════════════════
# 4. TRANSACOES
# ═════════════════════════════════════════════

def collect_transactions(cur, now_ms):
    print("  [4] Transacoes...")
    metrics = []

    rows = run_query(cur, """
        SELECT datname, xact_commit, xact_rollback
        FROM pg_stat_database
        WHERE datname NOT IN ('template0', 'template1')
    """)

    for row in rows:
        dims      = base_dims(database=row[0])
        commits   = safe_int(row[1])
        rollbacks = safe_int(row[2])
        total_tx  = commits + rollbacks
        rb_rate   = round(rollbacks / float(total_tx) * 100, 2) if total_tx > 0 else 0.0
        metrics += [
            gauge("postgresql.transactions.commits",       commits,   dims, now_ms),
            gauge("postgresql.transactions.rollbacks",     rollbacks, dims, now_ms),
            gauge("postgresql.transactions.rollback_rate", rb_rate,   dims, now_ms),
        ]

    return metrics


# ═════════════════════════════════════════════
# 5. TUPLAS
# ═════════════════════════════════════════════

def collect_tuples(cur, now_ms):
    print("  [5] Tuplas...")
    metrics = []

    rows = run_query(cur, """
        SELECT datname, tup_returned, tup_fetched, tup_inserted, tup_updated, tup_deleted
        FROM pg_stat_database
        WHERE datname NOT IN ('template0', 'template1')
    """)

    for row in rows:
        dims       = base_dims(database=row[0])
        returned   = safe_int(row[1])
        fetched    = safe_int(row[2])
        efficiency = round(fetched / float(returned) * 100, 2) if returned > 0 else 0.0
        metrics += [
            gauge("postgresql.tuples.returned",   returned,         dims, now_ms),
            gauge("postgresql.tuples.fetched",    fetched,          dims, now_ms),
            gauge("postgresql.tuples.efficiency", efficiency,       dims, now_ms),
            gauge("postgresql.tuples.inserted",   safe_int(row[3]), dims, now_ms),
            gauge("postgresql.tuples.updated",    safe_int(row[4]), dims, now_ms),
            gauge("postgresql.tuples.deleted",    safe_int(row[5]), dims, now_ms),
        ]

    return metrics


# ═════════════════════════════════════════════
# 6. LOCKS
# ═════════════════════════════════════════════

def collect_locks(cur, now_ms):
    print("  [6] Locks...")
    metrics = []

    rows = run_query(cur, """
        SELECT mode, granted, COUNT(*) AS total
        FROM pg_locks
        GROUP BY mode, granted
    """)

    waiting_total = 0
    for row in rows:
        dims  = base_dims(mode=row[0], granted=str(row[1]))
        total = safe_int(row[2])
        metrics.append(gauge("postgresql.locks.count", total, dims, now_ms))
        if not row[1]:
            waiting_total += total

    metrics.append(gauge("postgresql.locks.waiting_total", waiting_total, base_dims(), now_ms))

    rows2 = run_query(cur, """
        SELECT datname, deadlocks FROM pg_stat_database
        WHERE datname NOT IN ('template0', 'template1')
    """)
    for row in rows2:
        dims = base_dims(database=row[0])
        metrics.append(gauge("postgresql.locks.deadlocks", safe_int(row[1]), dims, now_ms))

    return metrics


# ═════════════════════════════════════════════
# 7. TAMANHOS
# ═════════════════════════════════════════════

def collect_sizes(cur, now_ms):
    print("  [7] Tamanhos...")
    metrics = []

    rows = run_query(cur, """
        SELECT datname, pg_database_size(datname)
        FROM pg_database
        WHERE datname NOT IN ('template0', 'template1') AND datallowconn = true
        ORDER BY pg_database_size(datname) DESC
    """)
    for row in rows:
        dims = base_dims(database=row[0])
        metrics.append(gauge("postgresql.database.size_bytes", safe_int(row[1]), dims, now_ms))

    rows = run_query(cur, """
        SELECT n.nspname, c.relname,
               pg_total_relation_size(c.oid),
               pg_relation_size(c.oid),
               pg_indexes_size(c.oid)
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind = 'r'
          AND n.nspname NOT IN ('pg_catalog', 'information_schema')
        ORDER BY pg_total_relation_size(c.oid) DESC
        LIMIT 50
    """)
    for row in rows:
        dims = base_dims(schema=row[0], table_name=row[1])
        metrics += [
            gauge("postgresql.table.total_bytes", safe_int(row[2]), dims, now_ms),
            gauge("postgresql.table.table_bytes", safe_int(row[3]), dims, now_ms),
            gauge("postgresql.table.index_bytes", safe_int(row[4]), dims, now_ms),
        ]

    rows = run_query(cur, """
        SELECT schemaname, relname, n_live_tup, n_dead_tup,
               CASE WHEN (n_live_tup + n_dead_tup) > 0
                    THEN round(n_dead_tup::numeric / (n_live_tup + n_dead_tup) * 100, 2)
                    ELSE 0 END,
               pg_total_relation_size(schemaname || '.' || relname)
        FROM pg_stat_user_tables
        ORDER BY n_dead_tup DESC
        LIMIT 50
    """)
    for row in rows:
        dims = base_dims(schema=row[0], table_name=row[1])
        metrics += [
            gauge("postgresql.table.bloat.live_tuples",    safe_int(row[2]),   dims, now_ms),
            gauge("postgresql.table.bloat.dead_tuples",    safe_int(row[3]),   dims, now_ms),
            gauge("postgresql.table.bloat.dead_ratio_pct", safe_float(row[4]), dims, now_ms),
            gauge("postgresql.table.bloat.total_bytes",    safe_int(row[5]),   dims, now_ms),
        ]

    return metrics


# ═════════════════════════════════════════════
# 8. VACUUM E MANUTENCAO
# ═════════════════════════════════════════════

def collect_vacuum(cur, now_ms):
    print("  [8] Vacuum e manutencao...")
    metrics = []

    rows = run_query(cur, """
        SELECT
            s.schemaname,
            s.relname,
            s.n_dead_tup,
            EXTRACT(EPOCH FROM (now() - s.last_vacuum))::int       AS secs_since_vacuum,
            EXTRACT(EPOCH FROM (now() - s.last_autovacuum))::int   AS secs_since_autovacuum,
            EXTRACT(EPOCH FROM (now() - s.last_analyze))::int      AS secs_since_analyze,
            EXTRACT(EPOCH FROM (now() - s.last_autoanalyze))::int  AS secs_since_autoanalyze,
            age(c.relfrozenxid)                                    AS xid_age
        FROM pg_stat_user_tables s
        JOIN pg_class c ON c.relname = s.relname
        ORDER BY s.n_dead_tup DESC
        LIMIT 50
    """)

    for row in rows:
        dims = base_dims(schema=row[0], table_name=row[1])
        metrics += [
            gauge("postgresql.vacuum.dead_tuples",            safe_int(row[2]), dims, now_ms),
            gauge("postgresql.vacuum.secs_since_vacuum",      safe_int(row[3]), dims, now_ms),
            gauge("postgresql.vacuum.secs_since_autovacuum",  safe_int(row[4]), dims, now_ms),
            gauge("postgresql.vacuum.secs_since_analyze",     safe_int(row[5]), dims, now_ms),
            gauge("postgresql.vacuum.secs_since_autoanalyze", safe_int(row[6]), dims, now_ms),
            gauge("postgresql.vacuum.xid_age",                safe_int(row[7]), dims, now_ms),
        ]

    return metrics


# ═════════════════════════════════════════════
# 9. BACKGROUND WRITER E CHECKPOINTS
# ═════════════════════════════════════════════

def collect_bgwriter(cur, now_ms):
    print("  [9] Background writer e checkpoints...")
    metrics = []

    rows = run_query(cur, """
        SELECT checkpoints_timed, checkpoints_req,
               checkpoint_write_time, checkpoint_sync_time,
               buffers_checkpoint, buffers_clean, maxwritten_clean,
               buffers_backend, buffers_backend_fsync, buffers_alloc
        FROM pg_stat_bgwriter
    """)

    if not rows:
        return metrics

    row  = rows[0]
    dims = base_dims()
    metrics += [
        gauge("postgresql.bgwriter.checkpoints_timed",     safe_int(row[0]),   dims, now_ms),
        gauge("postgresql.bgwriter.checkpoints_req",       safe_int(row[1]),   dims, now_ms),
        gauge("postgresql.bgwriter.checkpoint_write_ms",   safe_float(row[2]), dims, now_ms),
        gauge("postgresql.bgwriter.checkpoint_sync_ms",    safe_float(row[3]), dims, now_ms),
        gauge("postgresql.bgwriter.buffers_checkpoint",    safe_int(row[4]),   dims, now_ms),
        gauge("postgresql.bgwriter.buffers_clean",         safe_int(row[5]),   dims, now_ms),
        gauge("postgresql.bgwriter.maxwritten_clean",      safe_int(row[6]),   dims, now_ms),
        gauge("postgresql.bgwriter.buffers_backend",       safe_int(row[7]),   dims, now_ms),
        gauge("postgresql.bgwriter.buffers_backend_fsync", safe_int(row[8]),   dims, now_ms),
        gauge("postgresql.bgwriter.buffers_alloc",         safe_int(row[9]),   dims, now_ms),
    ]

    return metrics


# ═════════════════════════════════════════════
# 10. REPLICACAO
# ═════════════════════════════════════════════

def collect_replication(cur, now_ms):
    print("  [10] Replicacao...")
    metrics = []

    rows = run_query(cur, """
        SELECT
            client_addr::text,
            state,
            pg_wal_lsn_diff(sent_lsn, replay_lsn) AS lag_bytes,
            pg_wal_lsn_diff(sent_lsn, flush_lsn)  AS flush_lag_bytes,
            pg_wal_lsn_diff(sent_lsn, write_lsn)  AS write_lag_bytes,
            sync_state
        FROM pg_stat_replication
    """)

    metrics.append(gauge("postgresql.replication.replica_count", len(rows), base_dims(), now_ms))

    for row in rows:
        dims = base_dims(client=row[0] or "unknown", state=row[1] or "unknown", sync_state=row[5] or "unknown")
        metrics += [
            gauge("postgresql.replication.lag_bytes",       safe_int(row[2]), dims, now_ms),
            gauge("postgresql.replication.flush_lag_bytes", safe_int(row[3]), dims, now_ms),
            gauge("postgresql.replication.write_lag_bytes", safe_int(row[4]), dims, now_ms),
        ]

    if not rows:
        print("  Sem replicas detectadas.")

    return metrics


# ═════════════════════════════════════════════
# ENVIO
# ═════════════════════════════════════════════

def build_payload(metrics):
    return [{"common": {"attributes": {"app.name": NR_APP, "host": HOSTNAME}}, "metrics": metrics}]

def send_to_newrelic(payload):
    data    = json.dumps(payload).encode("utf-8")
    headers = {"Content-Type": "application/json", "Api-Key": NR_LICENSE}
    return http_post(NR_ENDPOINT, headers, data)


# ═════════════════════════════════════════════
# MAIN
# ═════════════════════════════════════════════

try:
    print("=" * 55)
    print("pg_metrics — %s" % time.strftime("%Y-%m-%d %H:%M:%S"))
    print("Banco: %s | Host: %s" % (DBNAME, HOSTNAME))
    print("=" * 55)

    conn = psycopg2.connect(
        host=HOST, port=PORT, dbname=DBNAME,
        user=USER, password=PASSWORD,
        connect_timeout=5,
        application_name="pg_monitor",
    )
    conn.set_session(readonly=True, autocommit=True)
    cur = conn.cursor()

    now_ms      = int(time.time() * 1000)
    all_metrics = []

    all_metrics += collect_connections(cur, now_ms)
    all_metrics += collect_query_performance(cur, now_ms)
    all_metrics += collect_cache_io(cur, now_ms)
    all_metrics += collect_transactions(cur, now_ms)
    all_metrics += collect_tuples(cur, now_ms)
    all_metrics += collect_locks(cur, now_ms)
    all_metrics += collect_sizes(cur, now_ms)
    all_metrics += collect_vacuum(cur, now_ms)
    all_metrics += collect_bgwriter(cur, now_ms)
    all_metrics += collect_replication(cur, now_ms)
    all_metrics += collect_settings(cur, now_ms)

    cur.close()
    conn.close()

    print("-" * 55)
    print("Total de metricas coletadas: %d" % len(all_metrics))
    print("Enviando para New Relic...")

    status = send_to_newrelic(build_payload(all_metrics))

    if status == 202:
        print("New Relic: OK (202)")
    else:
        print("New Relic: erro HTTP %s" % status)

    print("=" * 55)

except psycopg2.OperationalError as e:
    print("[ERRO] Falha na conexao: %s" % str(e))
except psycopg2.ProgrammingError as e:
    print("[ERRO] Erro na query: %s" % str(e))
except Exception as e:
    print("[ERRO] Inesperado: %s" % str(e))


# ═════════════════════════════════════════════
# 11. CONFIGURACOES DO BANCO
# ═════════════════════════════════════════════

def collect_settings(cur, now_ms):
    print("  [11] Configuracoes do banco...")
    metrics = []

    NUMERIC_SETTINGS = [
        "max_connections",
        "autovacuum_max_workers",
        "autovacuum_vacuum_cost_delay",
        "max_parallel_workers",
        "max_parallel_workers_per_gather",
        "deadlock_timeout",
        "lock_timeout",
        "idle_in_transaction_session_timeout",
        "log_min_duration_statement",
        "random_page_cost",
        "seq_page_cost",
        "checkpoint_completion_target",
    ]

    SIZE_SETTINGS = [
        "shared_buffers",
        "work_mem",
        "maintenance_work_mem",
        "effective_cache_size",
        "wal_buffers",
        "max_wal_size",
        "min_wal_size",
    ]

    all_settings = NUMERIC_SETTINGS + SIZE_SETTINGS

    rows = run_query(cur, """
        SELECT name, setting, unit
        FROM pg_settings
        WHERE name = ANY(%s)
        ORDER BY name
    """, (all_settings,))

    unit_multipliers = {
        "B":   1,
        "kB":  1024,
        "MB":  1024 * 1024,
        "GB":  1024 * 1024 * 1024,
        "8kB": 8 * 1024,
    }

    for row in rows:
        name    = row[0]
        setting = row[1]
        unit    = (row[2] or "").strip()
        dims    = base_dims(config_name=name, unit=unit)

        try:
            value = float(setting)
            if unit in unit_multipliers:
                value = value * unit_multipliers[unit]
            metrics.append(gauge(
                "postgresql.config.%s" % name,
                value,
                dims,
                now_ms,
            ))
        except (ValueError, TypeError):
            if setting in ("on", "true"):
                metrics.append(gauge("postgresql.config.%s" % name, 1, dims, now_ms))
            elif setting in ("off", "false"):
                metrics.append(gauge("postgresql.config.%s" % name, 0, dims, now_ms))

    av_rows = run_query(cur, "SELECT setting FROM pg_settings WHERE name = 'autovacuum'")
    if av_rows:
        av_value = 1 if av_rows[0][0] == "on" else 0
        metrics.append(gauge("postgresql.config.autovacuum", av_value,
                             base_dims(config_name="autovacuum", unit="bool"), now_ms))

    print("  %d configuracoes coletadas." % len(metrics))
    return metrics
