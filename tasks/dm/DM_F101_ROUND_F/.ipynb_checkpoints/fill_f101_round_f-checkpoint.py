from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

def fill_f101_round_f(on_date: str, spark):
    report_on_date = datetime.strptime(on_date, "%Y-%m-%d")
    report_month = report_on_date - relativedelta(months=1)
    prev_month_start = report_month.replace(day=1)
    prev_month_end = (prev_month_start + relativedelta(months=1))- relativedelta(days=1)
    balance_in_date = (prev_month_start - relativedelta(days=1)).strftime("%d.%m.%Y")
    prev_month_start_str = prev_month_start.strftime("%Y-%m-%d")
    prev_month_end_str = prev_month_end.strftime("%Y-%m-%d")
    
    accounts = spark.table("DS.MD_ACCOUNT_D") \
        .withColumn("ledger_account", F.expr("substring(account_number, 1, 5)")) \
        .select("account_rk", "ledger_account", "currency_code", "char_type")
    
    balances_in = spark.table("DS.FT_BALANCE_F") \
        .where(F.col("on_date") == balance_in_date) \
        .select("account_rk", 
                F.col("balance_out").alias("balance_in"))
    
    balances_out = spark.table("DM.DM_ACCOUNT_BALANCE_F") \
        .where(F.col("on_date") == prev_month_end) \
        .select("account_rk", 
                F.col("balance_out").alias("balance_out_val"),
                F.col("balance_out_rub").alias("balance_out_rub"))

    turnovers = spark.table("DM.DM_ACCOUNT_TURNOVER_F")\
    .select("account_rk", "debet_amount", "credit_amount", 
            "debet_amount_rub", "credit_amount_rub")
    
    df = accounts \
        .join(balances_in, "account_rk", "left") \
        .join(balances_out, "account_rk", "left") \
        .join(turnovers, "account_rk", "left") \
        .join(spark.table("DS.MD_LEDGER_ACCOUNT_S").select("ledger_account", "chapter"), 
              "ledger_account", "left")

    result = df.groupBy("ledger_account", "chapter", "char_type").agg(
        F.first(F.lit(prev_month_start).cast("date")).alias("from_date"),
        F.first(F.lit(prev_month_end).cast("date")).alias("to_date"),

        # Остатки на начало
        F.sum(F.when(F.col("currency_code").isin("810", "643"), F.col("balance_in"))).alias("balance_in_rub"),
        F.sum(F.when(~F.col("currency_code").isin("810", "643"), F.col("balance_in"))).alias("balance_in_val"),
        F.sum(F.col("balance_in")).alias("balance_in_total"),

        # Обороты дебет
        F.sum(F.when(F.col("currency_code").isin("810", "643"), F.col("debet_amount_rub"))).alias("turn_deb_rub"),
        F.sum(F.when(~F.col("currency_code").isin("810", "643"), F.col("debet_amount_rub"))).alias("turn_deb_val"),
        F.sum(F.col("debet_amount_rub")).alias("turn_deb_total"),

        # Обороты кредит
        F.sum(F.when(F.col("currency_code").isin("810", "643"), F.col("credit_amount_rub"))).alias("turn_cre_rub"),
        F.sum(F.when(~F.col("currency_code").isin("810", "643"), F.col("credit_amount"))).alias("turn_cre_val"),
        F.sum(F.col("credit_amount_rub")).alias("turn_cre_total"),

        # Остатки на конец
        F.sum(F.when(F.col("currency_code").isin("810", "643"), F.col("balance_out_rub"))).alias("balance_out_rub"),
        F.sum(F.when(~F.col("currency_code").isin("810", "643"), F.col("balance_out_val"))).alias("balance_out_val"),
        F.sum(
            F.when(F.col("currency_code").isin("810", "643"), F.col("balance_out_rub"))
             .otherwise(F.col("balance_out_val"))
        ).alias("balance_out_total")
    )

    result_casted = result.select(
    F.col("ledger_account").cast("char"),
    F.col("chapter").cast("char"),
    F.col("char_type").cast("char"),
    F.col("from_date").cast("date"),
    F.col("to_date").cast("date"),

    F.col("balance_in_rub").cast("decimal(23,8)"),
    F.col("balance_in_val").cast("decimal(23,8)"),
    F.col("balance_in_total").cast("decimal(23,8)"),

    F.col("turn_deb_rub").cast("decimal(23,8)"),
    F.col("turn_deb_val").cast("decimal(23,8)"),
    F.col("turn_deb_total").cast("decimal(23,8)"),

    F.col("turn_cre_rub").cast("decimal(23,8)"),
    F.col("turn_cre_val").cast("decimal(23,8)"),
    F.col("turn_cre_total").cast("decimal(23,8)"),

    F.col("balance_out_rub").cast("decimal(23,8)"),
    F.col("balance_out_val").cast("decimal(23,8)"),
    F.col("balance_out_total").cast("decimal(23,8)")
    )

    return result_casted