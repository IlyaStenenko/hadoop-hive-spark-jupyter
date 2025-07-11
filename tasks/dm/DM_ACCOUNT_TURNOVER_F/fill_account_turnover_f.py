from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import DecimalType, IntegerType, StringType

def fill_account_turnover_f(on_date: str, spark: SparkSession) -> DataFrame:
    
    #Загрузка таблиц
    ft_posting_f = spark.read.parquet('/user/hive/warehouse/ds.db/ft_posting_f')
    md_exchange_rate_d = spark.read.parquet('/user/hive/warehouse/ds.db/md_exchange_rate_d')

    posting_on_date = ft_posting_f.where(F.col('oper_date') == on_date)
    
    #??? а нужно ли
    credit_account_rk = posting_on_date.select(F.col('credit_account_rk').alias('account_rk'))
    debet_account_rk = posting_on_date.select(F.col('debet_account_rk').alias('account_rk'))
    account_rk = credit_account_rk.union(debet_account_rk).distinct();

    posting_on_date = posting_on_date.join(account_rk, posting_on_date['credit_account_rk'] == account_rk['account_rk'], how = 'right')

    #credit
    credit_amount = posting_on_date\
    .filter(F.col("debet_amount")\
    .isNotNull())\
    .groupby('account_rk')\
    .agg(F.round(F.sum('credit_amount'), 3).alias('credit_amount'))

    #643 - это iso рубля?
    data = md_exchange_rate_d \
        .filter(F.col('code_iso_num') == 643) \
        .orderBy(F.col('data_actual_end_date').desc()) \
        .select('data_actual_end_date', 'reduced_cource') \
        .first()

    if data is None:
        value = 1
    else:
    # Проверяем дату — если актуальна, берем reduced_cource, иначе 1
        if data['data_actual_end_date'] >= on_date:
            value = data['reduced_cource']
        else:
            value = 1

    dm_account_turnover_f = credit_amount\
    .select('account_rk', 'credit_amount', (F.col('credit_amount') * value)\
    .alias('credit_amount_rub'))

    #debit
    debet_amount = posting_on_date\
    .filter(F.col("debet_amount")\
    .isNotNull()) \
    .groupby('account_rk') \
    .agg(F.round(F.sum('debet_amount'), 3).alias('debet_amount'))

    debet_amount_with_rub = debet_amount\
    .select('account_rk', 'debet_amount', (F.col('debet_amount'))\
    .alias('debet_amount_rub'))

    #Объединение credit с debit
    dm_account_turnover_f = dm_account_turnover_f\
    .join(debet_amount_with_rub, on = 'account_rk', how = 'inner')
    
    dm_account_turnover_f = dm_account_turnover_f.withColumn("on_date", F.lit(on_date))

    dm_account_turnover_f = dm_account_turnover_f.select(
    F.col("account_rk").cast(IntegerType()).alias("account_rk"),
    F.col("credit_amount").cast(DecimalType(23, 8)).alias("credit_amount"),
    F.col("credit_amount_rub").cast(DecimalType(23, 8)).alias("credit_amount_rub"),
    F.col("debet_amount").cast(DecimalType(23, 8)).alias("debet_amount"),
    F.col("debet_amount_rub").cast(DecimalType(23, 8)).alias("debet_amount_rub"),
    F.col("on_date").cast(StringType()).alias("on_date")
)
    
    return dm_account_turnover_f