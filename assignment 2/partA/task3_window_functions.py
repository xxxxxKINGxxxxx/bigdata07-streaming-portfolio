from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, sum as spark_sum, round as spark_round,
    rank, dense_rank, lag, when, expr, to_date,
    year as spark_year
)
from pyspark.sql.window import Window
from typing import Final

DATA_PATH: Final[str] = r"E:\School\Abdallah Assignment\assignment 2\data\Global_Superstore.csv"
APP_NAME: Final[str] = "BigData07_Assignment2_Task3"


def create_spark_session(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


def load_clean_data(spark: SparkSession, path: str) -> DataFrame:
    from pyspark.sql.functions import to_date, when
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


# ─────────────────────────────────────────────
# WINDOW 1 — RANKING
# Rank Sub-Categories by total sales WITHIN
# each Category partition.
#
# WHY partitionBy("Category")?
# We want rank 1,2,3 separately inside each
# category — not a global rank across all.
# partitionBy resets the rank counter for
# each new Category value.
#
# WHY dense_rank vs rank?
# rank():       1,2,2,4 (skips 3 after a tie)
# dense_rank(): 1,2,2,3 (no gaps after ties)
# dense_rank is cleaner for business reporting.
# ─────────────────────────────────────────────
def window1_rank_subcategory(df: DataFrame) -> None:
    print("\n" + "="*60)
    print("WINDOW 1 — Rank Sub-Categories by Sales within Category")
    print("="*60)
    print("Shows which sub-category is #1, #2, #3 in revenue")
    print("within its own category group.\n")

    # Step 1: Aggregate total sales per sub-category
    agg_df = (
        df.groupBy("Category", "Sub_Category")
        .agg(spark_round(spark_sum("Sales"), 2).alias("Total_Sales"),
             spark_round(spark_sum("Profit"), 2).alias("Total_Profit"))
    )

    # Step 2: Define window — partition by Category, order by Sales desc
    window_spec = Window.partitionBy("Category").orderBy(col("Total_Sales").desc())

    # Step 3: Apply dense_rank over the window
    result = agg_df.withColumn("Sales_Rank", dense_rank().over(window_spec))

    result.orderBy("Category", "Sales_Rank").show(30, truncate=False)


# ─────────────────────────────────────────────
# WINDOW 2 — RUNNING CUMULATIVE TOTAL
# Cumulative revenue by year, partitioned by
# Category.
#
# WHY rowsBetween(unboundedPreceding, currentRow)?
# This defines the window FRAME — which rows
# each row "looks at" when computing the sum:
# - unboundedPreceding = from the very first row
#   in this partition
# - currentRow = up to and including this row
# Together: "sum all rows from start to now"
# That's exactly a running cumulative total.
# ─────────────────────────────────────────────
def window2_cumulative_revenue(df: DataFrame) -> None:
    print("\n" + "="*60)
    print("WINDOW 2 — Cumulative Revenue by Year per Category")
    print("="*60)
    print("Shows running total of revenue year by year,")
    print("separately for each product category.\n")

    # Aggregate yearly revenue per category
    yearly_df = (
        df.groupBy("Category", "Year")
        .agg(spark_round(spark_sum("Sales"), 2).alias("Yearly_Sales"))
        .orderBy("Category", "Year")
    )

    # Window: partition by Category, order by Year,
    # frame from first row to current row
    window_spec = (
        Window.partitionBy("Category")
        .orderBy("Year")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    result = yearly_df.withColumn(
        "Cumulative_Revenue",
        spark_round(spark_sum("Yearly_Sales").over(window_spec), 2)
    )

    result.orderBy("Category", "Year").show(20, truncate=False)


# ─────────────────────────────────────────────
# WINDOW 3 — PERIOD-OVER-PERIOD GROWTH
# Year-over-year revenue growth % per Category.
#
# WHY lag()?
# lag("Yearly_Sales", 1) reaches back exactly
# 1 row in the ordered window to get the
# PREVIOUS year's value for that category.
# This is how you compare "this year vs last year"
# without a self-join.
#
# Growth formula:
# ((current - previous) / previous) * 100
# ─────────────────────────────────────────────
def window3_yoy_growth(df: DataFrame) -> None:
    print("\n" + "="*60)
    print("WINDOW 3 — Year-over-Year Revenue Growth % by Category")
    print("="*60)
    print("Shows % change in revenue vs previous year")
    print("for each product category.\n")

    # Aggregate yearly revenue per category
    yearly_df = (
        df.groupBy("Category", "Year")
        .agg(spark_round(spark_sum("Sales"), 2).alias("Yearly_Sales"))
    )

    # Window: partition by Category, order by Year
    window_spec = Window.partitionBy("Category").orderBy("Year")

    # lag() fetches the previous year's value
    result = yearly_df \
        .withColumn("Prev_Year_Sales", lag("Yearly_Sales", 1).over(window_spec)) \
        .withColumn(
            "YoY_Growth_Pct",
            when(
                col("Prev_Year_Sales").isNotNull(),
                spark_round(
                    ((col("Yearly_Sales") - col("Prev_Year_Sales"))
                     / col("Prev_Year_Sales")) * 100,
                    2
                )
            ).otherwise(None)
        )

    result.orderBy("Category", "Year").show(20, truncate=False)


def main() -> None:
    spark: SparkSession = create_spark_session(APP_NAME)
    spark.sparkContext.setLogLevel("ERROR")

    print("Loading and cleaning data...")
    df = load_clean_data(spark, DATA_PATH)
    print(f"Dataset ready: {df.count():,} rows")

    window1_rank_subcategory(df)
    window2_cumulative_revenue(df)
    window3_yoy_growth(df)

    print("\n✅ Task 3 complete — rank, cumulative, and growth windows done.")
    spark.stop()


if __name__ == "__main__":
    main()
