from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col, year, month

def write_to_csv(df, file_path):
    df.write \
      .csv(file_path, mode='overwrite', header=True)

def write_to_postgres(df, table_name, conn_string, db_properties):
    df.write \
      .jdbc(url=conn_string, table=table_name, mode='overwrite', properties=db_properties)

def process_data():
    spark = SparkSession.builder \
        .appName("Process Data") \
        .config("spark.jars", "file:///home/tugas/postgresql-42.7.3.jar") \
        .getOrCreate()
    
    df_report = spark.read.csv('file:///home/tugas/sql_report.csv', header=True, inferSchema=True)
    df_product = spark.read.csv('file:///home/tugas/sql_product.csv', header=True, inferSchema=True)
    df_report = df_report.withColumn("order_date_new", col("order_date_new").cast("date"))
    df_total_sales_by_product = df_report.join(df_product, "product_id", "inner")
    
    conn_string = "jdbc:postgresql://localhost:5432/olapdata"
    db_properties = {
        "user": "postgres",
        "password": "postgres",
        "driver": "org.postgresql.Driver"
    }
    

    write_to_postgres(df_total_sales_by_product, 'total_sales', conn_string, db_properties)

if __name__ == "__main__":
    process_data()
