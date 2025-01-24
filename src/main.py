import yaml
import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from typing import Dict, List, Tuple, Union, Any
from src.data_processor import DataProcessor


def create_spark_session(mysql_jar_path: str, mongodb_uri: str) -> SparkSession:
    """
    Create and return a SparkSession with necessary configurations.

    :param mysql_jar_path: Path to MySQL JDBC connector JAR
    :param mongodb_uri: URI for MongoDB connection
    :return: Configured SparkSession
    """
    return (
        SparkSession.builder.appName("CombinedDataPipeline")
        .config("spark.jars", mysql_jar_path)
        .config(
            "spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1"
        )
        .config("spark.mongodb.input.uri", mongodb_uri)
        .getOrCreate()
    )


def load_config(config_path: str) -> Dict[str, Any]:
    """
    Load configuration from a YAML file and environment variables

    :param config_path: Path to the YAML configuration file
    :return: Dictionary containing configuration parameters
    """
    # Load environment variables
    load_dotenv()

    # Load the YAML template
    with open(config_path, "r") as file:
        config = yaml.safe_load(file)

    # Replace all environment variables in the config
    for key in config:
        env_var = config[key][2:-1]  # Remove ${ and }
        config[key] = os.getenv(env_var)

    return config


def main(config: Dict[str, Any]) -> Tuple[DataFrame, SparkSession]:
    """
    Main function to process data using the provided configuration

    :param config: Dictionary containing configuration parameters
    :return: Tuple of the combined DataFrame and the SparkSession
    """
    spark = create_spark_session(
        config["mysql_connector_path"],
        f"{config['mongodb_uri']}/{config['mongo_db']}.{config['mongo_collection']}",
    )
    data_processor = DataProcessor(spark)
    combined_df = data_processor.process_data(config)
    print("Combined Data:")
    combined_df.show()
    print(f"Combined data has been written to {config['output_path']}")
    return combined_df, spark


if __name__ == "__main__":
    config = load_config("config.yaml")
    combined_df, spark = main(config)
    spark.stop()
