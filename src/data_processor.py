import shutil
import os
from typing import Dict, Any
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


class DataProcessor:
    spark: SparkSession

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def load_mysql_data(
        self, jdbc_url: str, db_table: str, db_user: str, db_password: str
    ) -> DataFrame:
        """
        Load data from MySQL database.

        :param jdbc_url: JDBC URL for the MySQL database
        :param db_table: Name of the table to load data from
        :param db_user: Database username
        :param db_password: Database password
        :return: DataFrame containing the loaded MySQL data
        """
        return (
            self.spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("driver", "com.mysql.cj.jdbc.Driver")
            .option("dbtable", db_table)
            .option("user", db_user)
            .option("password", db_password)
            .load()
        )

    def load_mongo_data(self, db_name: str, collection_name: str) -> DataFrame:
        """
        Load data from MongoDB.

        :param db_name: Name of the MongoDB database
        :param collection_name: Name of the collection to load data from
        :return: DataFrame containing the loaded MongoDB data
        """
        return (
            self.spark.read.format("mongo")
            .option("database", db_name)
            .option("collection", collection_name)
            .load()
        )

    def load_csv_data(self, file_path: str) -> DataFrame:
        """
        Load data from a CSV file.

        :param file_path: Path to the CSV file
        :return: DataFrame containing the loaded CSV data
        """
        return self.spark.read.csv(file_path, header=True, inferSchema=True)

    def combine_data(
        self, mysql_df: DataFrame, mongo_df: DataFrame, csv_df: DataFrame
    ) -> DataFrame:
        """
        Combine data from MySQL, MongoDB, and CSV sources.
        The combined DataFrame will contain columns: id, name, age, city.

        :param mysql_df: DataFrame containing MySQL data
        :param mongo_df: DataFrame containing MongoDB data
        :param csv_df: DataFrame containing CSV data
        :return: Combined DataFrame from all sources
        """
        mysql_df = mysql_df.selectExpr("id", "name", "age", "city")
        mongo_df = mongo_df.selectExpr("id", "name", "age", "city")
        csv_df = csv_df.selectExpr("id", "name", "age", "city")

        # Combine the DataFrames
        combined_df = mysql_df.unionByName(mongo_df).unionByName(csv_df)
        return combined_df

    def save_to_csv(self, df: DataFrame, output_file: str) -> None:
        """
        Save the DataFrame to a CSV file.

        :param df: DataFrame to save
        :param output_file: Path to save the CSV file
        """
        temp_dir = "temp_combined_output"
        df.coalesce(1).write.option("header", "true").mode("overwrite").csv(temp_dir)

        for file_name in os.listdir(temp_dir):
            if file_name.startswith("part-") and file_name.endswith(".csv"):
                os.rename(os.path.join(temp_dir, file_name), output_file)

        shutil.rmtree(temp_dir)

    def process_data(self, config: Dict[str, Any]) -> DataFrame:
        """
        Process data from all sources and combine them.

        :param config: Dictionary containing configuration parameters
        :return: Combined DataFrame from all sources
        """
        mysql_df = self.load_mysql_data(
            config["mysql_url"],
            config["mysql_table"],
            config["mysql_user"],
            config["mysql_password"],
        )
        print("MySQL Data:")
        mysql_df.show()

        mongo_df = self.load_mongo_data(config["mongo_db"], config["mongo_collection"])
        print("MongoDB Data:")
        mongo_df.show()

        csv_df = self.load_csv_data(config["csv_path"])
        print("CSV Data:")
        csv_df.show()

        combined_df = self.combine_data(mysql_df, mongo_df, csv_df)
        self.save_to_csv(combined_df, config["output_path"])

        return combined_df
