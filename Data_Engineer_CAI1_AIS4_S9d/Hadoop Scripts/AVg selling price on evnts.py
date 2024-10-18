# Initialize Spark session
spark = SparkSession.builder \
    .appName("Average Event Sales Price") \
    .getOrCreate()

# Paths to your CSV files in HDFS
fact_sales_path = "hdfs://localhost:8020/user/data_source/Fact_Sales.csv"
dim_date_path = "hdfs://localhost:8020/user/data_source/Dim_date.csv"  # Assuming this is the correct path

# Load CSV files into DataFrames
fact_sales_df = spark.read.csv(fact_sales_path, header=True, inferSchema=True)
dim_date_df = spark.read.csv(dim_date_path, header=True, inferSchema=True)

# Create temporary views for SQL queries
fact_sales_df.createOrReplaceTempView("fact_sales")
dim_date_df.createOrReplaceTempView("dim_date")

# Constructing the SQL query
sql_query = """
WITH SalesData AS (
    SELECT 
        fs.sale_id, 
        fs.store_id, 
        fs.item_id, 
        fs.sell_price,
        CASE 
            WHEN ed.d = 'd_1' THEN fs.d_1
            WHEN ed.d = 'd_2' THEN fs.d_2
            WHEN ed.d = 'd_3' THEN fs.d_3
            WHEN ed.d = 'd_4' THEN fs.d_4
            WHEN ed.d = 'd_5' THEN fs.d_5
            -- Add more cases for each d_x column up to d_1000
            WHEN ed.d = 'd_1000' THEN fs.d_1000
        END AS event_sales
    FROM 
        fact_sales fs
    JOIN 
        dim_date ed ON ed.d IS NOT NULL -- Join on the Dim_date table
)

SELECT 
    AVG(sd.sell_price * sd.event_sales) AS avg_event_sales_price
FROM 
    SalesData sd
WHERE 
    sd.event_sales IS NOT NULL
"""

# Execute the SQL query
avg_event_sales_price_df = spark.sql(sql_query)

# Show the result
avg_event_sales_price_df.show()

# Stop the Spark session
spark.stop()
