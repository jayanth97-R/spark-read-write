from pyspark.sql import SparkSession

def read_and_write(jdbc_url, connection_properties, table_name, partition_column,
                                         lower_bound, upper_bound, num_partitions, output_path, jdbc_driver_path):
    try:
        spark = SparkSession.builder \
            .appName("JDBC Read and Write as Parquet") \
            .config("spark.driver.extraClassPath", jdbc_driver_path) \
            .config("spark.executor.memory", "10g") \
            .master("local[*]") \
            .getOrCreate()

        jdbc_options = {
            "url": jdbc_url,
            "dbtable": f"(SELECT *, EXTRACT(DAY FROM event_time) AS {partition_column} FROM {table_name} limit 1000000) as sub_qry",
            "partitionColumn": partition_column,
            "lowerBound": lower_bound,
            "upperBound": upper_bound,
            "numPartitions": num_partitions
        }
        jdbc_options.update(connection_properties)

        df = spark.read \
            .format("jdbc") \
            .options(**jdbc_options) \
            .load()

        df.write \
            .mode("overwrite") \
            .parquet(output_path)

        print(f"Data written as Parquet to: {output_path}")

    except Exception as e:
        print(f"Error occurred: {e}")
        raise e

if __name__ == "__main__":
    jdbc_url = "jdbc:postgresql://localhost:5432/spark"
    connection_properties = {
        "user": "jayanth",
        "password": "password",
        "driver": "org.postgresql.Driver"
    }

    table_name = "sales"
    partition_column = "day"
    lower_bound = "1"
    upper_bound = "31"
    num_partitions = "4"

    output_path = "/Users/jayanthsamudayapalepu/Dev/Spark-POC/out-parquet_files"

    jdbc_driver_path = "/Users/jayanthsamudayapalepu/Dev/Spark-POC/postgresql-42.7.2.jar"

    read_and_write(jdbc_url, connection_properties, table_name, partition_column,
                                        lower_bound, upper_bound, num_partitions, output_path, jdbc_driver_path)
    

#     # echo "# spark-read-write" >> README.md
# git init
# git add README.md
# git commit -m "first commit"
# git branch -M main
# git remote add origin https://github.com/jayanth97-R/spark-read-write.git
# git push -u origin main
