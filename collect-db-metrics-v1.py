import psycopg2
import ConfigParser
import os
import sys

# ── Carrega configuracao ─────────────────────
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
# ────────────────────────────────────────────

SQL = """
    SELECT
        queryid::text                        AS query_id,
        calls                                AS chamadas,
        round(total_time::numeric, 2)        AS total_ms,
        round((total_time / calls)::numeric, 2) AS media_ms,
        rows                                 AS linhas_retornadas,
        LEFT(query, 120)                     AS query_texto
    FROM pg_stat_statements
    WHERE calls > 0
      AND (total_time / calls) >= %s
    ORDER BY media_ms DESC
    LIMIT %s;
"""

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
        print("Top %d queries lentas (media > %sms)\n" % (len(rows), THRESHOLD_MS))
        print("%-15s %10s %12s %10s %10s" % ("query_id", "chamadas", "total_ms", "media_ms", "linhas"))
        print("-" * 65)
        for row in rows:
            print("%-15s %10s %12s %10s %10s" % (row[0], row[1], row[2], row[3], row[4]))
            print("   Query: %s" % row[5].replace("\n", " "))
            print("")

    cur.close()
    conn.close()

except psycopg2.OperationalError as e:
    print("Falha na conexao: " + str(e))
except psycopg2.ProgrammingError as e:
    print("Erro na query: " + str(e))
