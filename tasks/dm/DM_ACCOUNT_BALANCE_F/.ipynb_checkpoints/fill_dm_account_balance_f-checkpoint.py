from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from datetime import datetime, timedelta

def fill_dm_account_balance_f(on_date: str, spark: SparkSession) -> DataFrame:
    md_account_d = spark.read.parquet("/user/hive/warehouse/ds.db/md_account_d")
    dm_account_turnover_f = spark.read.parquet('/user/hive/warehouse/dm.db/dm_account_turnover_f/')
    dm_account_balance_f = spark.read.parquet('/user/hive/warehouse/dm.db/dm_account_balance_f')
    md_exchange_rate_d = spark.read.parquet('/user/hive/warehouse/ds.db/md_exchange_rate_d')

    # Получение курса
    ruble_exchange_row = md_exchange_rate_d \
        .filter(F.col("CODE_ISO_NUM") == 643) \
        .select("REDUCED_COURCE") \
        .limit(1) \
        .collect()
    
    ruble_exchange_rate = ruble_exchange_row[0]["REDUCED_COURCE"] if ruble_exchange_row else 1.0

    date_as_date = F.to_date(F.lit(on_date), "yyyy-MM-dd")
    prev_date = F.date_sub(date_as_date, 1)

    # Фильтрация по строковым значениям
    dm_account_balance_f = dm_account_balance_f\
        .where(F.col('on_date') == prev_date)\
        .select('on_date', 'account_rk', 'balance_out', 
                (F.col('balance_out') * ruble_exchange_rate).alias('balance_out_rub'))

    # md_account_d: фильтрация по актуальности
    md_account_d_between_on_date = md_account_d\
        .where((F.lit(on_date) >= F.col("DATA_ACTUAL_DATE")) & (F.lit(on_date) <= F.col("DATA_ACTUAL_END_DATE")))

    # Остатки на предыдущую дату
    prev_balances = spark.table("dm.dm_account_balance_f") \
    .filter(F.col("on_date") == prev_date) \
    .select("account_rk", "balance_out", "balance_out_rub")
    
    # Обороты за текущую дату
    formatted_date = F.date_format(date_as_date, "dd-MM-yyyy")
    turnovers = dm_account_turnover_f.filter(F.col("on_date") == formatted_date)\
        .select("account_rk", "debet_amount", "credit_amount")

    # Счета + остатки
    accounts = md_account_d_between_on_date \
        .join(prev_balances, on="account_rk", how="left") \
        .withColumn("balance_out", F.coalesce(F.col("balance_out"), F.lit(0.0)))

    # Добавляем обороты
    accounts = accounts.join(turnovers, on="account_rk", how="left") \
        .withColumn("debet_amount", F.coalesce(F.col("debet_amount"), F.lit(0.0))) \
        .withColumn("credit_amount", F.coalesce(F.col("credit_amount"), F.lit(0.0)))
    
    # Расчёт по условию
    accounts = accounts.withColumn(
    "balance_out",
    F.when(F.col("char_type") == "А",
         F.col("balance_out") + F.col("debet_amount") - F.col("credit_amount")
    ).when(F.col("char_type") == "П",
         F.col("balance_out") - F.col("debet_amount") + F.col("credit_amount")
    ).otherwise(F.col("balance_out"))
    )

    # Расчёт рублёвого остатка
    accounts = accounts.withColumn(
    "balance_out_rub",
    F.col("balance_out") * ruble_exchange_rate
    )
    
    accounts = accounts.select('account_rk', 'balance_out', 'balance_out_rub')\
        .withColumn('on_date', F.lit(on_date))

    return accounts

