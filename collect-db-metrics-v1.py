#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
metric_slow_queries.py — Queries lentas no PostgreSQL via pg_stat_statements
Envia metricas para o New Relic Metric API
Compativel com Python 2.7

Uso: python metric_slow_queries.py
"""

import psycopg2
import ConfigParser
import os
import sys
import json
import time
import socket

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

# ── Carrega configuracao ─────────────────────
CONFIG_FILE = "config.ini"

if not os.path.exists(CONFIG_FILE):
    print("Arquivo '%s' nao encontrado." % CONFIG_FILE)
    sys.exit(1)

cfg = ConfigParser.ConfigParser()
cfg.read(CONFIG_FILE)

HOST         = cfg.get("postgresql",   "host")
PORT         = cfg.getint("postgresql","port")
DBNAME       = cfg.get("postgresql",   "dbname")
USER         = cfg.get("postgresql",   "user")
PASSWORD     = cfg.get("postgresql",   "password")
THRESHOLD_MS = cfg.getint("slow_queries", "threshold_ms")
LIMIT        = cfg.getint("slow_queries", "limit")

NR_LICENSE   = cfg.get("newrelic", "license_key")
NR_ENDPOINT  = cfg.get("newrelic", "api_endpoint")
NR_APP       = cfg.get("newrelic", "app_name")
# ────────────────────────────────────────────

SQL = """
    SELECT
        queryid::text                          AS query_id,
        calls                                  AS chamadas,
        round(total_exec_time::numeric, 2)     AS total_ms,
        round(mean_exec_time::numeric, 2)      AS media_ms,
        rows                                   AS linhas_retornadas,
        LEFT(query, 120)                       AS query_texto
    FROM pg_stat_statements
    WHERE calls > 0
      AND mean_exec_time >= %s
    ORDER BY media_ms DESC
    LIMIT %s;
"""


def build_payload(rows):
    now_ms = int(time.time() * 1000)
    hostname = socket.gethostname()
    metrics = []

    for row in rows:
        dims = {
            "query_id":  row[0],
            "app.name":  NR_APP,
            "host":      hostname,
            "database":  DBNAME,
        }
        metrics.append({"name": "postgresql.slow_queries.calls",    "type": "gauge", "value": row[1], "timestamp": now_ms, "attributes": dims})
        metrics.append({"name": "postgresql.slow_queries.total_ms", "type": "gauge", "value": float(row[2]), "timestamp": now_ms, "attributes": dims})
        metrics.append({"name": "postgresql.slow_queries.mean_ms",  "type": "gauge", "value": float(row[3]), "timestamp": now_ms, "attributes": dims})
        metrics.append({"name": "postgresql.slow_queries.rows",     "type": "gauge", "value": row[4], "timestamp": now_ms, "attributes": dims})

    return [{"common": {"attributes": {"app.name": NR_APP, "host": hostname}}, "metrics": metrics}]


def send_to_newrelic(payload):
    data = json.dumps(payload).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "Api-Key":      NR_LICENSE,
    }
    status = http_post(NR_ENDPOINT, headers, data)
    return status


# ── Execucao ─────────────────────────────────
try:
    conn = psycopg2.connect(
        host=HOST, port=PORT, dbname=DBNAME,
        user=USER, password=PASSWORD,
        connect_timeout=5,
    )

    cur = conn.cursor()
    cur.execute(SQL, (THRESHOLD_MS, LIMIT))
    rows = cur.fetchall()

    if not rows:
        print("Nenhuma query acima de %sms encontrada." % THRESHOLD_MS)
    else:
        # Exibe no terminal
        print("Top %d queries lentas (media > %sms)\n" % (len(rows), THRESHOLD_MS))
        print("%-15s %10s %12s %10s %10s" % ("query_id", "chamadas", "total_ms", "media_ms", "linhas"))
        print("-" * 65)
        for row in rows:
            print("%-15s %10s %12s %10s %10s" % (row[0], row[1], row[2], row[3], row[4]))
            print("   Query: %s" % row[5].replace("\n", " "))
            print("")

        # Envia para New Relic
        print("Enviando %d metricas para o New Relic..." % (len(rows) * 4))
        payload = build_payload(rows)
        status = send_to_newrelic(payload)

        if status == 202:
            print("New Relic: OK (202)")
        else:
            print("New Relic: erro HTTP %s" % status)

    cur.close()
    conn.close()

except psycopg2.OperationalError as e:
    print("Falha na conexao: " + str(e))
except psycopg2.ProgrammingError as e:
    print("Erro na query: " + str(e))
