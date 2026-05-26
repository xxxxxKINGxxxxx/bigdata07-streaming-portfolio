from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, sum as spark_sum, avg, count,
    round as spark_round, desc, expr
)
from typing import Final

DATA_PATH: Final[str] = r"E:\School\Abdallah Assignment\assignment 2\data\Global_Superstore.csv"
APP_NAME: Final[str] = "BigData07_Assignment2_Task2"


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

    # Add derived columns needed for analysis
    df = df.withColumn("Profit_Margin", spark_round((col("Profit") / col("Sales")) * 100, 2))
    df = df.withColumn("Sales_Band",
            when(col("Sales") >= 1000, "High")
            .when(col("Sales") >= 300, "Medium")
            .otherwise("Low"))
    return df


# ─────────────────────────────────────────────
# Q1 — DataFrame API
# Business Question: Which Category generates
# the most total revenue and profit?
# WHY this matters: Tells management where to
# invest inventory and marketing budget.
# ─────────────────────────────────────────────
def q1_category_revenue_dataframe(df: DataFrame) -> None:
    print("\n" + "="*60)
    print("Q1 [DataFrame API] — Revenue & Profit by Category")
    print("="*60)
    print("Business Question: Which category drives the most")
    print("revenue AND profit? Are they the same category?\n")

    result = (
        df.groupBy("Category")
        .agg(
            spark_round(spark_sum("Sales"),  2).alias("Total_Sales"),
            spark_round(spark_sum("Profit"), 2).alias("Total_Profit"),
            count("*").alias("Order_Count"),
            spark_round(avg("Profit_Margin"), 2).alias("Avg_Profit_Margin_Pct")
        )
        .orderBy(desc("Total_Sales"))
    )
    result.show(truncate=False)


# ─────────────────────────────────────────────
# Q2 — DataFrame API
# Business Question: Which Region has the
# highest average order value?
# WHY this matters: High AOV regions should
# get priority sales team resources.
# ─────────────────────────────────────────────
def q2_region_avg_order_value_dataframe(df: DataFrame) -> None:
    print("\n" + "="*60)
    print("Q2 [DataFrame API] — Average Order Value by Region")
    print("="*60)
    print("Business Question: Which region spends the most")
    print("per order on average?\n")

    result = (
        df.groupBy("Region")
        .agg(
            spark_round(avg("Sales"),         2).alias("Avg_Order_Value"),
            spark_round(spark_sum("Sales"),   2).alias("Total_Revenue"),
            spark_round(avg("Profit_Margin"), 2).alias("Avg_Margin_Pct"),
            count("*").alias("Total_Orders")
        )
        .orderBy(desc("Avg_Order_Value"))
    )
    result.show(truncate=False)


# ─────────────────────────────────────────────
# Q3 — Spark SQL
# Business Question: What is the yearly
# revenue trend across all markets?
# WHY this matters: Reveals growth trajectory
# and whether the business is scaling.
# ─────────────────────────────────────────────
def q3_yearly_revenue_trend_sql(spark: SparkSession, df: DataFrame) -> None:
    print("\n" + "="*60)
    print("Q3 [Spark SQL] — Yearly Revenue Trend")
    print("="*60)
    print("Business Question: How has total revenue grown")
    print("year over year across all markets?\n")

    df.createOrReplaceTempView("orders")

    result = spark.sql("""
        SELECT
            YEAR(Order_Date)            AS Year,
            ROUND(SUM(Sales),  2)       AS Total_Revenue,
            ROUND(SUM(Profit), 2)       AS Total_Profit,
            COUNT(*)                    AS Total_Orders,
            ROUND(AVG(Profit_Margin), 2) AS Avg_Margin_Pct
        FROM orders
        GROUP BY YEAR(Order_Date)
        ORDER BY Year ASC
    """)
    result.show(truncate=False)


# ─────────────────────────────────────────────
# Q4 — Spark SQL
# Business Question: Which Sub-Categories are
# operating at a loss (negative profit)?
# WHY this matters: Loss-making sub-categories
# destroy value — must be restructured or cut.
# ─────────────────────────────────────────────
def q4_loss_making_subcategories_sql(spark: SparkSession, df: DataFrame) -> None:
    print("\n" + "="*60)
    print("Q4 [Spark SQL] — Loss-Making Sub-Categories")
    print("="*60)
    print("Business Question: Which sub-categories have")
    print("negative total profit (destroying value)?\n")

    df.createOrReplaceTempView("orders")

    result = spark.sql("""
        SELECT
            Sub_Category,
            Category,
            ROUND(SUM(Sales),          2) AS Total_Sales,
            ROUND(SUM(Profit),         2) AS Total_Profit,
            ROUND(AVG(Profit_Margin),  2) AS Avg_Margin_Pct,
            COUNT(*)                      AS Order_Count
        FROM orders
        GROUP BY Sub_Category, Category
        ORDER BY Total_Profit ASC
    """)
    result.show(20, truncate=False)


def main() -> None:
    spark: SparkSession = create_spark_session(APP_NAME)
    spark.sparkContext.setLogLevel("ERROR")

    print("Loading and cleaning data...")
    df = load_clean_data(spark, DATA_PATH)
    print(f"Dataset ready: {df.count():,} rows\n")

    # Q1 & Q2 — DataFrame API
    q1_category_revenue_dataframe(df)
    q2_region_avg_order_value_dataframe(df)

    # Q3 & Q4 — Spark SQL
    q3_yearly_revenue_trend_sql(spark, df)
    q4_loss_making_subcategories_sql(spark, df)

    print("\n✅ Task 2 complete — all 4 business questions answered.")
    spark.stop()


if __name__ == "__main__":
    main()
