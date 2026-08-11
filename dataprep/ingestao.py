# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
import datetime

def date_range(start, stop, monthly=False):
    dates = []
    while start <= stop:
        dates.append(start)
        dt = datetime.datetime.strptime(start, "%Y-%m-%d") + datetime.timedelta(days=1)
        start = dt.strftime("%Y-%m-%d")

    if monthly:
        return [i for i in dates if i.endswith("-01")]
    
    return dates


def ingest_table_cohort(query, table, cohort):
    df = spark.sql(query.format(date=cohort))
  
    try:
        spark.sql(f"""DELETE FROM sandbox.asn.{table} WHERE dtRef = '{cohort}'""")

        (df.write
        .mode("append")
        .format("delta")
        .saveAsTable(f"sandbox.asn.{table}"))

    except:
        (df.write
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .format("delta")
        .saveAsTable(f"sandbox.asn.{table}"))

# COMMAND ----------

start = dbutils.widgets.get("start")
stop =  dbutils.widgets.get("stop")
table =  dbutils.widgets.get("table")

dates = date_range(start,stop, True)

with open(f"{table}.sql") as open_file:
    query = open_file.read()

for i in dates:
    ingest_table_cohort(query=query, table=f"{table}_t5", cohort=i)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT dtRef, COUNT(*) FROM sandbox.asn.fs_seller_avaliacao_t5 GROUP BY ALL