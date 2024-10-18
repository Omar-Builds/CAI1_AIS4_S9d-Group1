# Initialize Spark session
spark = SparkSession.builder \
    .appName("Dynamic Sales Analysis") \
    .getOrCreate()

# Load DataFrames from CSV files
fact_sales_df = spark.read.csv("hdfs://localhost:8020/user/data_source/Fact_Sales.csv", header=True, inferSchema=True)
dim_date_df = spark.read.csv("hdfs://localhost:8020/user/data_source/Dim_date.csv", header=True, inferSchema=True)

# Show schemas to verify column names and types
fact_sales_df.printSchema()
dim_date_df.printSchema()

# Create temporary views
fact_sales_df.createOrReplaceTempView("fact_sales")
dim_date_df.createOrReplaceTempView("dim_date")

# Number of days columns to consider
num_days = 1000

# Generate dynamic CASE statement for sales on each day
sales_on_day_case = "CASE " + " ".join([f"WHEN dd.d = 'd_{i}' THEN fs.d_{i} " for i in range(1, num_days + 1)]) + " END AS sales_on_day"

# Build the SQL query dynamically
sql_query = f"""
WITH SalesOnNoEventsNoSNAP AS (
    SELECT
        fs.store_id,  -- Store ID from fact_sales
        fs.sell_price, -- Selling price of the product
        {sales_on_day_case}
    FROM fact_sales fs
    JOIN dim_date dd ON dd.d LIKE 'd_%'  -- Match the days in dim_date with the sales columns
    WHERE (dd.snap_CA = 0 AND dd.snap_TX = 0 AND dd.snap_WI = 0)  -- No SNAP participation
    AND (dd.event_name_1 IS NULL AND dd.event_name_2 IS NULL) -- No events
)

SELECT
    store_id,
    SUM(sell_price * sales_on_day) AS total_sales
FROM SalesOnNoEventsNoSNAP
WHERE sales_on_day > 0  -- Filter out rows without sales
GROUP BY store_id
ORDER BY total_sales DESC;
"""

# Execute the SQL query
result_df = spark.sql(sql_query)

# Show the result
result_df.show()

# Stop the Spark session
spark.stop()
