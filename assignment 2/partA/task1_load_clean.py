from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, count, when, to_date,
    round as spark_round, expr
)
from typing import Final

DATA_PATH: Final[str] = r"E:\School\Abdallah Assignment\assignment 2\data\Global_Superstore.csv"
APP_NAME: Final[str] = "BigData07_Assignment2_Task1"


def create_spark_session(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


def load_dataframe(spark: SparkSession, path: str) -> DataFrame:
    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .option("encoding", "UTF-8")
        .load(path)
    )


def rename_and_cast(df: DataFrame) -> DataFrame:
    df = df.withColumnRenamed("Order.Date",     "Order_Date") \
           .withColumnRenamed("Ship.Date",      "Ship_Date") \
           .withColumnRenamed("Ship.Mode",      "Ship_Mode") \
           .withColumnRenamed("Customer.ID",    "Customer_ID") \
           .withColumnRenamed("Customer.Name",  "Customer_Name") \
           .withColumnRenamed("Order.ID",       "Order_ID") \
           .withColumnRenamed("Order.Priority", "Order_Priority") \
           .withColumnRenamed("Product.ID",     "Product_ID") \
           .withColumnRenamed("Product.Name",   "Product_Name") \
           .withColumnRenamed("Row.ID",         "Row_ID") \
           .withColumnRenamed("Shipping.Cost",  "Shipping_Cost") \
           .withColumnRenamed("Sub.Category",   "Sub_Category")

    # try_cast returns NULL instead of crashing on bad values
    df = df.withColumn("Sales",         expr("try_cast(Sales as double)")) \
           .withColumn("Profit",        expr("try_cast(Profit as double)")) \
           .withColumn("Quantity",      expr("try_cast(Quantity as int)")) \
           .withColumn("Discount",      expr("try_cast(Discount as double)")) \
           .withColumn("Shipping_Cost", expr("try_cast(Shipping_Cost as double)"))

    return df


def print_data_quality_report(df: DataFrame) -> None:
    total_rows: int = df.count()
    print("\n" + "="*60)
    print("   DATA QUALITY REPORT — Global Superstore")
    print("="*60)
    print(f"Total rows loaded : {total_rows:,}")

    print("\n── Column names and types ──")
    for name, dtype in df.dtypes:
        print(f"  {name:<25} {dtype}")

    print("\n── Null counts per column ──")
    null_exprs = [
        count(when(col(c).isNull(), c)).alias(c)
        for c in df.columns
    ]
    df.select(null_exprs).show(truncate=False)

    print("\n── Category distribution ──")
    df.groupBy("Category").count().orderBy("count", ascending=False).show()

    print("\n── Segment distribution ──")
    df.groupBy("Segment").count().orderBy("count", ascending=False).show()

    print("\n── Region distribution ──")
    df.groupBy("Region").count().orderBy("count", ascending=False).show()

    print("\n── Sales / Profit statistics ──")
    df.select("Sales", "Profit", "Quantity", "Discount").describe().show()


def clean_dataframe(df: DataFrame) -> DataFrame:
    rows_before: int = df.count()

    # WHY this date format?
    # Actual values in CSV: '2011-01-07 00:00:00.000'
    # Format string:         yyyy-MM-dd HH:mm:ss.SSS
    # to_date discards the time portion — we only
    # need the date for time-series analysis.
    df = df.withColumn("Order_Date", to_date(col("Order_Date"), "yyyy-MM-dd HH:mm:ss.SSS")) \
           .withColumn("Ship_Date",  to_date(col("Ship_Date"),  "yyyy-MM-dd HH:mm:ss.SSS"))

    # Drop rows missing critical fields
    df = df.dropna(subset=["Sales", "Profit", "Order_Date", "Category", "Region"])

    # Remove zero or negative sales
    df = df.filter(col("Sales") > 0)

    rows_after: int = df.count()
    print(f"\n── Cleaning summary ──")
    print(f"Rows before : {rows_before:,}")
    print(f"Rows after  : {rows_after:,}")
    print(f"Removed     : {rows_before - rows_after:,}")
    return df


def engineer_derived_columns(df: DataFrame) -> DataFrame:
    # Profit_Margin: profit as % of revenue — standard retail KPI
    df = df.withColumn(
        "Profit_Margin",
        spark_round((col("Profit") / col("Sales")) * 100, 2)
    )

    # Sales_Band: order value tier classification
    df = df.withColumn(
        "Sales_Band",
        when(col("Sales") >= 1000, "High")
        .when(col("Sales") >= 300,  "Medium")
        .otherwise("Low")
    )

    print("\n── Derived columns sample ──")
    df.select("Sales", "Profit", "Profit_Margin", "Sales_Band").show(10)
    return df


def main() -> None:
    spark: SparkSession = create_spark_session(APP_NAME)
    spark.sparkContext.setLogLevel("ERROR")
    print(f"Spark version: {spark.version}")

    print(f"\nLoading: {DATA_PATH}")
    df_raw = load_dataframe(spark, DATA_PATH)

    df_typed = rename_and_cast(df_raw)
    print_data_quality_report(df_typed)

    df_clean = clean_dataframe(df_typed)
    df_final = engineer_derived_columns(df_clean)

    print("\n── Final sample ──")
    df_final.select(
        "Order_Date", "Category", "Region",
        "Segment", "Sales", "Profit",
        "Profit_Margin", "Sales_Band"
    ).show(10, truncate=False)

    print(f"\n✅ Task 1 complete — {df_final.count():,} rows ready.")
    spark.stop()


if __name__ == "__main__":
    main()
