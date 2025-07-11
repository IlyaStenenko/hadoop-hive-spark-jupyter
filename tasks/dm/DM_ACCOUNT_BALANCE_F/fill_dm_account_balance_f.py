from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

def fill_dm_account_balance_f(on_date: str, spark: SparkSession) -> DataFrame:
    md_account_d = spark.read.parquet("/user/hive/warehouse/ds.db/md_account_d")
    ft_balance_f = spark.read.parquet('/user/hive/warehouse/ds.db/ft_balance_f')
    dm_account_turnover_f = spark.read.parquet('/user/hive/warehouse/dm.db/dm_account_turnover_f/')

    #Получение акутального курса
    md_exchange_rate_d = spark.read.parquet('/user/hive/warehouse/ds.db/md_exchange_rate_d')
    ruble_exchange_row = md_exchange_rate_d \
    .filter(F.col("CODE_ISO_NUM") == 643) \
    .select("REDUCED_COURCE") \
    .limit(1) \
    .collect()
    if ruble_exchange_row:
        ruble_exchange_rate = ruble_exchange_row[0]["REDUCED_COURCE"]
    else:
        ruble_exchange_rate = 1.0

    dm_account_balance_f = ft_balance_f.where(F.col('on_date') == on_date)\
    .select('on_date', 'account_rk', 'balance_out', (F.col('balance_out') * ruble_exchange_rate).alias('balance_out_rub'))
    on_date_lit = F.to_date(F.lit(on_date), 'yyyy-MM-dd')
    md_account_d_between_on_date = md_account_d.where(on_date_lit.between(F.col("DATA_ACTUAL_DATE"), F.col("DATA_ACTUAL_END_DATE")))
    
    prev_date = F.date_sub(F.to_date(F.lit(on_date), "yyyy-MM-dd"), 1)
    prev_balances = ft_balance_f.filter(
    F.col("on_date") == prev_date
    ).select("account_rk", "balance_out")

    #обороты по счёту
    turnovers = dm_account_turnover_f.filter(
    F.col("on_date") == on_date_lit
    ).select("account_rk", "debet_amount", "credit_amount")

    # Добавление остатков
    accounts = md_account_d_between_on_date \
    .join(prev_balances, on="account_rk", how="left") \
    .withColumn("balance_out", F.coalesce(F.col("balance_out"), F.lit(0.0)))

    # Добавление оборотов
    accounts = accounts.join(turnovers, on="account_rk", how="left") \
    .withColumn("debet_amount", F.coalesce(F.col("debet_amount"), F.lit(0.0))) \
    .withColumn("credit_amount", F.coalesce(F.col("credit_amount"), F.lit(0.0)))

    # balance_out по условию
    accounts = accounts.withColumn(
    "new_balance_out",
    F.when(F.col("char_type") == "А",
         F.col("balance_out") + F.col("debet_amount") - F.col("credit_amount")
    ).when(F.col("char_type") == "П",
         F.col("balance_out") - F.col("debet_amount") + F.col("credit_amount")
    ).otherwise(F.col("balance_out"))  # если тип неизвестен
    )

    return accounts

