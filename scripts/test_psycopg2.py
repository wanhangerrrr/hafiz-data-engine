import psycopg2

conn = psycopg2.connect(
    host="127.0.0.1",
    port=5432,
    dbname="public_data",
    user="admin",
    password="admin",
)
cur = conn.cursor()
cur.execute("select 1;")
print(cur.fetchone())
cur.close()
conn.close()