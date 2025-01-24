import os
import sys
import pytest
from typing import Dict, Any
from pyspark.sql import DataFrame, SparkSession
from src.main import create_spark_session, load_config, main
from src.data_processor import DataProcessor

# Add the project root directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.fixture(scope="session")
def config():
    """Fixture to load configuration"""
    return load_config("config.yaml")


@pytest.fixture(scope="session")
def spark(config):
    """Fixture to create SparkSession"""
    spark = create_spark_session(
        config["mysql_connector_path"],
        f"{config['mongodb_uri']}/{config['mongo_db']}.{config['mongo_collection']}",
    )
    yield spark
    spark.stop()


@pytest.fixture(scope="session")
def combined_df(spark, config):
    """Fixture to get the combined DataFrame"""
    df, _ = main(config)
    return df


def test_output_file_exists(config):
    """Test if the output file exists at the specified path"""
    assert os.path.exists(
        config["output_path"]
    ), f"Output file {config['output_path']} does not exist"


def test_basic_dataframe_structure(combined_df):
    """Test if the DataFrame has the basic expected structure"""
    # This test should pass even with 'pass' in combine_data
    assert isinstance(combined_df, DataFrame), "Result is not a DataFrame"
    assert combined_df is not None, "DataFrame is None"


def test_data_combination(combined_df):
    """Test if the data combination logic works correctly"""
    # Check if the result is not an empty list
    assert not isinstance(
        combined_df, list
    ), "Combined data should not be an empty list"
    assert isinstance(combined_df, DataFrame), "Result should be a DataFrame"

    # Check columns
    expected_columns = {"id", "name", "age", "city"}
    assert (
        set(combined_df.columns) == expected_columns
    ), f"DataFrame schema mismatch. Expected {expected_columns}, got {set(combined_df.columns)}"

    # Check number of rows
    assert combined_df.count() == 6, "Combined DataFrame should have exactly 6 rows"

    # Convert DataFrame to list of rows for easier comparison
    rows = combined_df.orderBy("id").collect()

    # Expected data
    expected_data = [
        (1, "Tom Watts", 55, "Chicago"),
        (2, "Jeff Steele", 45, "San Francisco"),
        (3, "Joy Bobin", 21, "Boston"),
        (4, "Matthew Watts", 33, "NYC"),
        (5, "Alice Johnson", 28, "Chicago"),
        (6, "Bob Williams", 35, "Los Angeles"),
    ]

    # Check each row matches expected data
    for actual, expected in zip(rows, expected_data):
        assert (
            actual["id"] == expected[0]
        ), f"Mismatched id: expected {expected[0]}, got {actual['id']}"
        assert (
            actual["name"] == expected[1]
        ), f"Mismatched name: expected {expected[1]}, got {actual['name']}"
        assert (
            actual["age"] == expected[2]
        ), f"Mismatched age: expected {expected[2]}, got {actual['age']}"
        assert (
            actual["city"] == expected[3]
        ), f"Mismatched city: expected {expected[3]}, got {actual['city']}"


def test_dataframe_not_empty(combined_df):
    """Test if the DataFrame is not empty"""
    # This basic test should still run
    assert combined_df is not None, "DataFrame is None"


def run_tests(
    combined_df: DataFrame, spark: SparkSession, config: Dict[str, Any]
) -> None:
    """Run all tests for the data processing pipeline"""
    try:
        test_output_file_exists(config)
        test_basic_dataframe_structure(combined_df)
        test_data_combination(combined_df)
        test_dataframe_not_empty(combined_df)
        print("All tests passed successfully!")
    finally:
        spark.stop()


if __name__ == "__main__":
    print("Please run main.py instead to execute the pipeline and tests.")
