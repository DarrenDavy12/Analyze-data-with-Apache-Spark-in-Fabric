📊 Analyze Data with Apache Spark in Microsoft Fabric

This project follows a hands-on lab from Microsoft Learn, demonstrating how to ingest, transform, analyze, and visualize data using Apache Spark in Microsoft Fabric.

🚀 Overview

In this exercise, we:

Created a Fabric workspace
Built a Lakehouse
Ingested CSV data
Used PySpark DataFrames for analysis
Transformed and stored data in Parquet format
Created Delta tables
Queried data using SQL
Visualized insights with matplotlib and seaborn



🧱 Architecture

Local CSV Files → Lakehouse (Files) → Spark DataFrames
        ↓
   Data Transformation (PySpark)
        ↓
 Parquet Storage / Delta Tables
        ↓
   SQL Queries & Visualizations



📂 Dataset

The dataset consists of sales order data across multiple years:

2019.csv
2020.csv
2021.csv

Source:
https://github.com/MicrosoftLearning/dp-data/raw/main/orders.zip



⚙️ Steps Performed

1. Create Workspace
Created a new Microsoft Fabric workspace
Enabled Fabric capacity (Trial / Premium / Fabric)

2. Create Lakehouse
Created a Lakehouse for data storage
Disabled Lakehouse schemas (important constraint)

3. Upload Data
Downloaded and extracted dataset
Uploaded orders folder into Lakehouse Files

4. Create Notebook
Created a Fabric notebook
Used PySpark as the primary language

5. Load Data into DataFrame
df = spark.read.format("csv").schema(orderSchema).load("Files/orders/*.csv")

- Defined a schema for structured data
- Loaded all CSV files using wildcard *


6. Explore Data
Select & Filter
customers = df.select("CustomerName", "Email")


Filter Example
customers = df.select("CustomerName", "Email") \
              .where(df['Item'] == 'Road-250 Red, 52')

Aggregation
productSales = df.groupBy("Item").sum()

Yearly Sales
yearlySales = df.select(year(col("OrderDate")).alias("Year")) \
                .groupBy("Year").count().orderBy("Year")


7. Transform Data
Added new columns:
Year, Month
FirstName, LastName
transformed_df = df.withColumn("Year", year(col("OrderDate"))) \
                   .withColumn("Month", month(col("OrderDate")))


8. Save Data
Save as Parquet
transformed_df.write.mode("overwrite").parquet("Files/transformed_data/orders")


Partitioned Data
orders_df.write.partitionBy("Year", "Month") \
         .mode("overwrite") \
         .parquet("Files/partitioned_data")



9. Create Delta Table
df.write.format("delta").saveAsTable("salesorders")

Enables:
Transactions
Versioning
Efficient querying



10. Query with SQL
SELECT YEAR(OrderDate) AS OrderYear,
       SUM((UnitPrice * Quantity) + Tax) AS GrossRevenue
FROM salesorders
GROUP BY YEAR(OrderDate)
ORDER BY OrderYear;



11. Data Visualization
Matplotlib Example
plt.bar(x=df_sales['OrderYear'], height=df_sales['GrossRevenue'])
plt.title('Revenue by Year')
plt.show()


Seaborn Example
sns.barplot(x="OrderYear", y="GrossRevenue", data=df_sales)


📈 Key Learnings
How to use Spark DataFrames for scalable data processing
Schema definition for structured data ingestion
Efficient storage with Parquet
Performance optimization via partitioning
Using Delta tables in a lakehouse architecture
Combining PySpark and SQL
Data visualization using Python libraries


🧹 Cleanup
To avoid unnecessary resource usage:
Stop Spark session
Delete the workspace after completion


📌 Requirements
Access to Microsoft Fabric
Basic knowledge of:
Python
SQL
Data concepts


📎 References
Microsoft Learn Lab: Analyze data with Apache Spark in Fabric
Apache Spark Documentation
Matplotlib & Seaborn Docs


💡 Notes
Spark sessions may take time to initialize on first run
Always define schemas for better data quality
Partitioning improves performance for large datasets



