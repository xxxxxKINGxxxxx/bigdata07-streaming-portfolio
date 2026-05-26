from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, sum as spark_sum, avg, count,
    round as spark_round, expr, to_date,
    year as spark_year, lag, when, dense_rank
)
from pyspark.sql.window import Window
import snowflake.connector
from dotenv import load_dotenv
import os
from typing import Final, List, Tuple

load_dotenv(r"E:\School\Abdallah Assignment\assignment 2\.env")

DATA_PATH: Final[str] = r"E:\School\Abdallah Assignment\assignment 2\data\Global_Superstore.csv"
APP_NAME: Final[str] = "BigData07_Assignment2_Task4"


def create_spark_session(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


def load_clean_data(spark: SparkSession, path: str) -> DataFrame:
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .option("encoding", "UTF-8")
        .load(path)
    )
    df = df.withColumnRenamed("Order.Date",    "Order_Date") \
           .withColumnRenamed("Ship.Date",     "Ship_Date") \
           .withColumnRenamed("Ship.Mode",     "Ship_Mode") \
           .withColumnRenamed("Customer.ID",   "Customer_ID") \
           .withColumnRenamed("Customer.Name", "Customer_Name") \
           .withColumnRenamed("Order.ID",      "Order_ID") \
           .withColumnRenamed("Order.Priority","Order_Priority") \
           .withColumnRenamed("Product.ID",    "Product_ID") \
           .withColumnRenamed("Product.Name",  "Product_Name") \
           .withColumnRenamed("Row.ID",        "Row_ID") \
           .withColumnRenamed("Shipping.Cost", "Shipping_Cost") \
           .withColumnRenamed("Sub.Category",  "Sub_Category")

    df = df.withColumn("Sales",    expr("try_cast(Sales as double)")) \
           .withColumn("Profit",   expr("try_cast(Profit as double)")) \
           .withColumn("Quantity", expr("try_cast(Quantity as int)")) \
           .withColumn("Discount", expr("try_cast(Discount as double)"))

    df = df.withColumn("Order_Date", to_date(col("Order_Date"), "yyyy-MM-dd HH:mm:ss.SSS"))
    df = df.dropna(subset=["Sales", "Profit", "Order_Date", "Category", "Region"])
    df = df.filter(col("Sales") > 0)
    df = df.withColumn("Profit_Margin", spark_round((col("Profit") / col("Sales")) * 100, 2))
    df = df.withColumn("Year", spark_year(col("Order_Date")))
    return df


def build_aggregations(df: DataFrame) -> dict:
    # Table 1: Category Sales Summary
    category_sales = (
        df.groupBy("Category")
        .agg(
            spark_round(spark_sum("Sales"),  2).alias("TOTAL_SALES"),
            spark_round(spark_sum("Profit"), 2).alias("TOTAL_PROFIT"),
            count("*").alias("ORDER_COUNT"),
            spark_round(avg("Profit_Margin"), 2).alias("AVG_PROFIT_MARGIN")
        )
        .orderBy(col("TOTAL_SALES").desc())
    )

    # Table 2: Yearly Revenue Trend
    yearly_revenue = (
        df.groupBy("Year")
        .agg(
            spark_round(spark_sum("Sales"),  2).alias("TOTAL_REVENUE"),
            spark_round(spark_sum("Profit"), 2).alias("TOTAL_PROFIT"),
            count("*").alias("TOTAL_ORDERS"),
            spark_round(avg("Profit_Margin"), 2).alias("AVG_MARGIN")
        )
        .orderBy("Year")
    )

    # Table 3: Region Performance
    region_perf = (
        df.groupBy("Region")
        .agg(
            spark_round(avg("Sales"),         2).alias("AVG_ORDER_VALUE"),
            spark_round(spark_sum("Sales"),   2).alias("TOTAL_REVENUE"),
            spark_round(avg("Profit_Margin"), 2).alias("AVG_MARGIN"),
            count("*").alias("TOTAL_ORDERS")
        )
        .orderBy(col("TOTAL_REVENUE").desc())
    )

    # Table 4: Sub-Category Profitability with Window Rank
    agg_sub = (
        df.groupBy("Category", "Sub_Category")
        .agg(
            spark_round(spark_sum("Sales"),  2).alias("TOTAL_SALES"),
            spark_round(spark_sum("Profit"), 2).alias("TOTAL_PROFIT"),
            spark_round(avg("Profit_Margin"), 2).alias("AVG_MARGIN"),
            count("*").alias("ORDER_COUNT")
        )
    )
    window_spec = Window.partitionBy("Category").orderBy(col("TOTAL_SALES").desc())
    subcategory_rank = agg_sub.withColumn("SALES_RANK", dense_rank().over(window_spec)) \
                              .orderBy("Category", "SALES_RANK")

    return {
        "CATEGORY_SALES":    category_sales,
        "YEARLY_REVENUE":    yearly_revenue,
        "REGION_PERFORMANCE": region_perf,
        "SUBCATEGORY_RANK":  subcategory_rank,
    }


def get_snowflake_connection() -> snowflake.connector.SnowflakeConnection:
    # WHY environment variables?
    # Credentials in code = security vulnerability.
    # .env file stays on your machine only.
    # The .env.example (empty values) gets submitted.
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    )
    return conn


def setup_snowflake_database(conn: snowflake.connector.SnowflakeConnection) -> None:
    # WHY CREATE DATABASE IF NOT EXISTS?
    # Idempotent — safe to run multiple times.
    # Second run won't fail or overwrite data.
    cursor = conn.cursor()
    db   = os.getenv("SNOWFLAKE_DATABASE")
    sch  = os.getenv("SNOWFLAKE_SCHEMA")
    wh   = os.getenv("SNOWFLAKE_WAREHOUSE")

    print(f"\nSetting up Snowflake: {db}.{sch}")
    cursor.execute(f"CREATE WAREHOUSE IF NOT EXISTS {wh} WAREHOUSE_SIZE='X-SMALL' AUTO_SUSPEND=60")
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db}")
    cursor.execute(f"USE DATABASE {db}")
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {sch}")
    cursor.execute(f"USE SCHEMA {sch}")
    cursor.execute(f"USE WAREHOUSE {wh}")
    print("✅ Database and schema ready.")
    cursor.close()


def df_to_snowflake(
    spark_df: DataFrame,
    table_name: str,
    conn: snowflake.connector.SnowflakeConnection
) -> None:
    # WHY collect() → Python list → Snowflake?
    # We avoid pandas (no Python 3.14 wheels).
    # collect() brings Spark rows to the driver
    # as Python Row objects, then we insert via
    # Snowflake's executemany() in batches.
    # For 50K rows this is fast enough (~seconds).
    db  = os.getenv("SNOWFLAKE_DATABASE")
    sch = os.getenv("SNOWFLAKE_SCHEMA")
    wh  = os.getenv("SNOWFLAKE_WAREHOUSE")

    cursor = conn.cursor()
    cursor.execute(f"USE DATABASE {db}")
    cursor.execute(f"USE SCHEMA {sch}")
    cursor.execute(f"USE WAREHOUSE {wh}")

    # Build CREATE TABLE from Spark schema
    type_map = {
        "double":  "FLOAT",
        "float":   "FLOAT",
        "int":     "INTEGER",
        "integer": "INTEGER",
        "long":    "BIGINT",
        "string":  "VARCHAR(500)",
        "boolean": "BOOLEAN",
    }

    col_defs = ", ".join([
        f"{field.name} {type_map.get(field.dataType.simpleString(), 'VARCHAR(500)')}"
        for field in spark_df.schema.fields
    ])

    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    cursor.execute(f"CREATE TABLE {table_name} ({col_defs})")
    print(f"  Created table: {table_name}")

    # Collect rows and insert in batches of 1000
    rows: List[Tuple] = [tuple(row) for row in spark_df.collect()]
    batch_size: int = 1000
    placeholders: str = ", ".join(["%s"] * len(spark_df.columns))

    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        cursor.executemany(
            f"INSERT INTO {table_name} VALUES ({placeholders})",
            batch
        )

    print(f"  ✅ Loaded {len(rows):,} rows into {table_name}")
    cursor.close()


def validate_snowflake(conn: snowflake.connector.SnowflakeConnection) -> None:
    # Run 2 validation SQL queries in Snowflake
    # These produce insights not computed in PySpark
    db  = os.getenv("SNOWFLAKE_DATABASE")
    sch = os.getenv("SNOWFLAKE_SCHEMA")
    wh  = os.getenv("SNOWFLAKE_WAREHOUSE")

    cursor = conn.cursor()
    cursor.execute(f"USE DATABASE {db}")
    cursor.execute(f"USE SCHEMA {sch}")
    cursor.execute(f"USE WAREHOUSE {wh}")

    print("\n── Validation Query 1: Revenue vs Profit efficiency ──")
    cursor.execute("""
        SELECT
            CATEGORY,
            TOTAL_SALES,
            TOTAL_PROFIT,
            ROUND((TOTAL_PROFIT / TOTAL_SALES) * 100, 2) AS PROFIT_EFFICIENCY_PCT
        FROM CATEGORY_SALES
        ORDER BY PROFIT_EFFICIENCY_PCT DESC
    """)
    for row in cursor.fetchall():
        print(row)

    print("\n── Validation Query 2: Best growth year across all categories ──")
    cursor.execute("""
        SELECT
            YEAR,
            TOTAL_REVENUE,
            TOTAL_ORDERS,
            ROUND((TOTAL_REVENUE / TOTAL_ORDERS), 2) AS AVG_REVENUE_PER_ORDER
        FROM YEARLY_REVENUE
        ORDER BY AVG_REVENUE_PER_ORDER DESC
    """)
    for row in cursor.fetchall():
        print(row)

    cursor.close()


def main() -> None:
    spark: SparkSession = create_spark_session(APP_NAME)
    spark.sparkContext.setLogLevel("ERROR")

    print("Loading and cleaning data...")
    df = load_clean_data(spark, DATA_PATH)
    print(f"Dataset ready: {df.count():,} rows")

    print("\nBuilding aggregations...")
    tables = build_aggregations(df)
    for name, agg_df in tables.items():
        print(f"  {name}: {agg_df.count()} rows")

    print("\nConnecting to Snowflake...")
    conn = get_snowflake_connection()
    print("✅ Connected to Snowflake")

    setup_snowflake_database(conn)

    print("\nLoading tables into Snowflake...")
    for table_name, agg_df in tables.items():
        df_to_snowflake(agg_df, table_name, conn)

    validate_snowflake(conn)
    conn.close()

    print("\n✅ Task 4 complete — all tables loaded into Snowflake.")
    spark.stop()


if __name__ == "__main__":
    main()
