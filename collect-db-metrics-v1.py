#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
pg_metrics.py — Coleta metricas do PostgreSQL e envia para o New Relic
Grupos: performance de queries + tamanhos de bancos/tabelas
Compativel com Python 2.7

Uso: python pg_metrics.py
"""

import psycopg2
import ConfigParser
import os
import sys
import json
import time
import socket

# ── HTTP helper (Python 2.7 e 3) ─────────────
try:
    import urllib2
    def http_post(url, headers, data):
        req = urllib2.Request(url, data=data)
        for k, v in headers.items():
            req.add_header(k, v)
        try:
            resp = urllib2.urlopen(req, timeout=15)
            return resp.getcode()
        except urllib2.HTTPError as e:
            return e.code
except ImportError:
    import urllib.request
    def http_post(url, headers, data):
        req = urllib.request.Request(url, data=data)
        for k, v in headers.items():
            req.add_header(k, v)
        try:
            resp = urllib.request.urlopen(req, timeout=15)
            return resp.getcode()
        except urllib.error.HTTPError as e:
            return e.code

# ── Carrega configuracao ──────────────────────
CONFIG_FILE = "config.ini"

if not os.path.exists(CONFIG_FILE):
    print("Arquivo '%s' nao encontrado." % CONFIG_FILE)
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

NR_LICENSE   = cfg.get("newrelic", "license_key")
NR_ENDPOINT  = cfg.get("newrelic", "api_endpoint")
NR_APP       = cfg.get("newrelic", "app_name")

HOSTNAME     = socket.gethostname()
# ─────────────────────────────────────────────


# ═════════════════════════════════════════════
# SQLS
# ═════════════════════════════════════════════

# Queries lentas: mean_exec_time acima do threshold
SQL_SLOW_QUERIES = """
    SELECT
        queryid::text                            AS query_id,
        calls,
        round(total_exec_time::numeric, 2)       AS total_exec_ms,
        round(mean_exec_time::numeric, 2)        AS mean_exec_ms,
        round(total_plan_time::numeric, 2)       AS total_plan_ms,
        round(mean_plan_time::numeric, 2)        AS mean_plan_ms,
        rows,
        LEFT(query, 120)                         AS query_text
    FROM pg_stat_statements
    WHERE calls > 0
      AND mean_exec_time >= %s
    ORDER BY mean_exec_ms DESC
    LIMIT %s;
"""

# Queries mais chamadas
SQL_MOST_CALLED = """
    SELECT
        queryid::text                            AS query_id,
        calls,
        round(total_exec_time::numeric, 2)       AS total_exec_ms,
        round(mean_exec_time::numeric, 2)        AS mean_exec_ms,
        round(mean_plan_time::numeric, 2)        AS mean_plan_ms,
        rows,
        LEFT(query, 120)                         AS query_text
    FROM pg_stat_statements
    WHERE calls > 0
    ORDER BY calls DESC
    LIMIT %s;
"""

# Queries com maior tempo acumulado
SQL_MOST_TIME = """
    SELECT
        queryid::text                            AS query_id,
        calls,
        round(total_exec_time::numeric, 2)       AS total_exec_ms,
        round(mean_exec_time::numeric, 2)        AS mean_exec_ms,
        round(mean_plan_time::numeric, 2)        AS mean_plan_ms,
        rows,
        LEFT(query, 120)                         AS query_text
    FROM pg_stat_statements
    WHERE calls > 0
    ORDER BY total_exec_ms DESC
    LIMIT %s;
"""

# Tamanho dos bancos
SQL_DB_SIZES = """
    SELECT
        datname                                  AS database,
        pg_database_size(datname)                AS size_bytes
    FROM pg_database
    WHERE datname NOT IN ('template0', 'template1')
      AND datallowconn = true
    ORDER BY size_bytes DESC;
"""

# Tamanho das tabelas (top 50)
SQL_TABLE_SIZES = """
    SELECT
        n.nspname                                AS schema,
        c.relname                                AS table_name,
        pg_total_relation_size(c.oid)            AS total_bytes,
        pg_relation_size(c.oid)                  AS table_bytes,
        pg_indexes_size(c.oid)                   AS index_bytes
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind = 'r'
      AND n.nspname NOT IN ('pg_catalog', 'information_schema')
    ORDER BY total_bytes DESC
    LIMIT 50;
"""

# Bloat de tabelas (espaco desperdicado por dead tuples)
SQL_BLOAT = """
    SELECT
        schemaname                               AS schema,
        relname                                  AS table_name,
        n_live_tup                               AS live_tuples,
        n_dead_tup                               AS dead_tuples,
        CASE WHEN (n_live_tup + n_dead_tup) > 0
             THEN round(n_dead_tup::numeric / (n_live_tup + n_dead_tup) * 100, 2)
             ELSE 0
        END                                      AS dead_ratio_pct,
        pg_total_relation_size(schemaname || '.' || relname) AS total_bytes
    FROM pg_stat_user_tables
    ORDER BY dead_tuples DESC
    LIMIT 50;
"""


# ═════════════════════════════════════════════
# COLETA
# ═════════════════════════════════════════════

def collect_query_performance(cur):
    metrics = []
    now_ms  = int(time.time() * 1000)

    # ── Queries lentas ────────────────────────
    print("  Coletando queries lentas...")
    cur.execute(SQL_SLOW_QUERIES, (THRESHOLD_MS, LIMIT))
    rows = cur.fetchall()
    print("  %d queries lentas encontradas." % len(rows))
    for row in rows:
        dims = {"query_id": row[0], "database": DBNAME, "query_text": row[7]}
        metrics += [
            gauge("postgresql.query.slow.calls",         row[1]        or 0, dims, now_ms),
            gauge("postgresql.query.slow.total_exec_ms", float(row[2]) or 0, dims, now_ms),
            gauge("postgresql.query.slow.mean_exec_ms",  float(row[3]) or 0, dims, now_ms),
            gauge("postgresql.query.slow.total_plan_ms", float(row[4]) or 0, dims, now_ms),
            gauge("postgresql.query.slow.mean_plan_ms",  float(row[5]) or 0, dims, now_ms),
            gauge("postgresql.query.slow.rows",          row[6]        or 0, dims, now_ms),
        ]

    # ── Queries mais chamadas ─────────────────
    print("  Coletando queries mais chamadas...")
    cur.execute(SQL_MOST_CALLED, (LIMIT,))
    rows = cur.fetchall()
    for row in rows:
        dims = {"query_id": row[0], "database": DBNAME, "query_text": row[6]}
        metrics += [
            gauge("postgresql.query.top_calls.calls",        row[1]        or 0, dims, now_ms),
            gauge("postgresql.query.top_calls.total_exec_ms",float(row[2]) or 0, dims, now_ms),
            gauge("postgresql.query.top_calls.mean_exec_ms", float(row[3]) or 0, dims, now_ms),
            gauge("postgresql.query.top_calls.mean_plan_ms", float(row[4]) or 0, dims, now_ms),
            gauge("postgresql.query.top_calls.rows",         row[5]        or 0, dims, now_ms),
        ]

    # ── Queries com maior tempo acumulado ─────
    print("  Coletando queries com maior tempo acumulado...")
    cur.execute(SQL_MOST_TIME, (LIMIT,))
    rows = cur.fetchall()
    for row in rows:
        dims = {"query_id": row[0], "database": DBNAME, "query_text": row[6]}
        metrics += [
            gauge("postgresql.query.top_time.calls",         row[1]        or 0, dims, now_ms),
            gauge("postgresql.query.top_time.total_exec_ms", float(row[2]) or 0, dims, now_ms),
            gauge("postgresql.query.top_time.mean_exec_ms",  float(row[3]) or 0, dims, now_ms),
            gauge("postgresql.query.top_time.mean_plan_ms",  float(row[4]) or 0, dims, now_ms),
            gauge("postgresql.query.top_time.rows",          row[5]        or 0, dims, now_ms),
        ]

    return metrics


def collect_sizes(cur):
    metrics = []
    now_ms  = int(time.time() * 1000)

    # ── Tamanho dos bancos ────────────────────
    print("  Coletando tamanho dos bancos...")
    cur.execute(SQL_DB_SIZES)
    rows = cur.fetchall()
    for row in rows:
        dims = {"database": row[0]}
        metrics.append(gauge("postgresql.database.size_bytes", int(row[1]) or 0, dims, now_ms))

    # ── Tamanho das tabelas ───────────────────
    print("  Coletando tamanho das tabelas...")
    cur.execute(SQL_TABLE_SIZES)
    rows = cur.fetchall()
    for row in rows:
        dims = {"schema": row[0], "table_name": row[1], "database": DBNAME}
        metrics += [
            gauge("postgresql.table.total_bytes", int(row[2]) or 0, dims, now_ms),
            gauge("postgresql.table.table_bytes", int(row[3]) or 0, dims, now_ms),
            gauge("postgresql.table.index_bytes", int(row[4]) or 0, dims, now_ms),
        ]

    # ── Bloat (dead tuples) ───────────────────
    print("  Coletando bloat das tabelas...")
    cur.execute(SQL_BLOAT)
    rows = cur.fetchall()
    for row in rows:
        dims = {"schema": row[0], "table_name": row[1], "database": DBNAME}
        metrics += [
            gauge("postgresql.table.bloat.live_tuples",    int(row[2])   or 0, dims, now_ms),
            gauge("postgresql.table.bloat.dead_tuples",    int(row[3])   or 0, dims, now_ms),
            gauge("postgresql.table.bloat.dead_ratio_pct", float(row[4]) or 0, dims, now_ms),
            gauge("postgresql.table.bloat.total_bytes",    int(row[5])   or 0, dims, now_ms),
        ]

    return metrics


# ═════════════════════════════════════════════
# HELPERS
# ═════════════════════════════════════════════

def gauge(name, value, attributes, timestamp):
    return {
        "name":       name,
        "type":       "gauge",
        "value":      value,
        "timestamp":  timestamp,
        "attributes": attributes,
    }


def build_payload(metrics):
    return [{
        "common": {
            "attributes": {
                "app.name": NR_APP,
                "host":     HOSTNAME,
            }
        },
        "metrics": metrics,
    }]


def send_to_newrelic(payload):
    data = json.dumps(payload).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "Api-Key":      NR_LICENSE,
    }
    return http_post(NR_ENDPOINT, headers, data)


# ═════════════════════════════════════════════
# EXECUCAO
# ═════════════════════════════════════════════

try:
    print("Conectando ao banco %s..." % DBNAME)
    conn = psycopg2.connect(
        host=HOST, port=PORT, dbname=DBNAME,
        user=USER, password=PASSWORD,
        connect_timeout=5,
    )
    conn.set_session(readonly=True, autocommit=True)
    cur = conn.cursor()
    print("Conectado.\n")

    all_metrics = []

    print("[1/2] Performance de queries")
    all_metrics += collect_query_performance(cur)

    print("\n[2/2] Tamanhos")
    all_metrics += collect_sizes(cur)

    cur.close()
    conn.close()

    print("\nTotal de metricas coletadas: %d" % len(all_metrics))
    print("Enviando para New Relic...")

    payload = build_payload(all_metrics)
    status  = send_to_newrelic(payload)

    if status == 202:
        print("New Relic: OK (202)")
    else:
        print("New Relic: erro HTTP %s" % status)

except psycopg2.OperationalError as e:
    print("Falha na conexao: " + str(e))
except psycopg2.ProgrammingError as e:
    print("Erro na query: " + str(e))
except Exception as e:
    print("Erro inesperado: " + str(e))
