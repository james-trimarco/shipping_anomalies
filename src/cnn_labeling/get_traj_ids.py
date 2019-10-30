import psycopg2
import csv

conn=psycopg2.connect(
  database="old_ais",
  user="Matthias",
  host="/tmp/"
)

cur = conn.cursor()
cur.execute("select distinct traj_id from features.quants")
t = cur.fetchall()
with open("quant_traj_ids.csv",'w',newline='') as f:
    header = False
    writer = csv.writer(f)
    for id in t:
        writer.writerow(id)
    f.close()
cur.close()
