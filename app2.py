# Import necessary libraries
from pyspark.sql.functions import concat, lit, split
from flask import Flask
import pandas
from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os
import boto3
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType
def send_df():
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars /home/jos/Downloads/postgresql-42.5.4.jar pyspark-shell'
    # os.environ['PYSPARK_SUBMIT_ARGS'] = '-- packages com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.3 pyspark-shell'

    load_dotenv()

    USER_NAME = os.getenv('USER_NAME')
    PASSWORD = os.getenv('PASSWORD')

    # Create a SparkSession
    spark = SparkSession.builder.appName("Create PostgreSQL Table").getOrCreate()
    # spark = SparkSession.builder \
    #     .appName("Write DataFrame to S3") \
    #     .config("spark.hadoop.fs.s3a.access.key", "AKIAZIVCGE6ARU6GXGMR") \
    #     .config("spark.hadoop.fs.s3a.secret.key", "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") \
    #     .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    #     .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true") \
    #     .getOrCreate()

    # Get the data from local database by making connection
    df = spark.read.format("jdbc").option("url", "jdbc:postgresql://localhost:5432/postgres") \
        .option("driver", "org.postgresql.Driver").option("dbtable", "cs_table") \
        .option("user", "postgres").option("password", "postgres").load()


    # df.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", "AKIAZIVCGE6ARU6GXGMR")
    # df.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", "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")

    # manipulate the data frame
    df = df.withColumn('Name', concat('first',lit(" "),'last')) # Add a column with first and last name 
    df = df.drop('first', 'last') # delete the extra columns
    df = df.orderBy(df.id.asc()) # Sort the data based on id
    df = df.withColumnRenamed('date_of_birth','dob') # Rename the column name

    df = df.select("id","name","department","dob") # change the order of columns in pyspark

    # Change the format of date of birth
    df = df.withColumn('dob', concat(split(df['dob'], '-').getItem(0),lit('/'), \
                                split(df['dob'], '-').getItem(1),lit('/'), \
                                split(df['dob'], '-').getItem(2)))

        
    print(df.dtypes)

    # Configure JDBC properties for PostgreSQL RDS
    jdbc_url = f"jdbc:postgresql://database-1.c2tuuysyqfj3.ap-south-1.rds.amazonaws.com:5432/postgres?user={USER_NAME}&password={PASSWORD}"
    connection_properties = {
        "driver": "org.postgresql.Driver"
    }
    str = ""
    val =''
    for col in df.dtypes:
        val = 'varchar(20)' if col[1]=='string' else col[1]
        str+=col[0] + " " + val + ','

    str =str[0:-1]
    print(str)

    # Connect to the postgres database
    spark.read.jdbc(url=jdbc_url, table="(SELECT 1) AS Salias", properties=connection_properties)

    # Define the schema for the table
    # Create the table and populate the whole proccesed data into the AWS RDS(Success) 
    # set the column name and datatypes
    table_name = "dea_table"
    df.write.option("createTableColumnTypes", \
                    str) \
        .jdbc(url=jdbc_url, table=table_name, mode="overwrite", properties=connection_properties)

    # Load the resultent 
    df_read_remote = spark.read.jdbc(url=jdbc_url, table=table_name, properties=connection_properties)

    x = df_read_remote
    pandas_df = df_read_remote.toPandas()

    return pandas_df
    print(pandas_df)
    # # Write the PySpark DataFrame to the preexisting table in the PostgreSQL RDS instance
    # df.write.jdbc(url=jdbc_url, table="dea_table", mode="overwrite", properties=connection_properties)


    # # spark.stop()

