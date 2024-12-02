# data_ingestion.py
import os
import json
from followupfromjessicadisneypositions import logging
import snowflake.connector
from typing import List, Dict


def setup_snowflake_connection(
        user: str = None,
        password: str = None,
        account: str = None,
        warehouse: str = None,
        database: str = None,
        schema: str = None
) -> snowflake.connector.connection:
    """
    Connect to Snowflake.
    Args:
    user, password, account, warehouse, database, schema: Connection params
    Returns:
    Snowflake connection object
    """
    # Prioritize passed parameters, then environment variables
    conn = snowflake.connector.connect(
        user=user or os.getenv('SNOWFLAKE_USER'),
        password=password or os.getenv('SNOWFLAKE_PASSWORD'),
        account=account or os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse=warehouse or os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=database or os.getenv('SNOWFLAKE_DATABASE'),
        schema=schema or os.getenv('SNOWFLAKE_SCHEMA')
    )
    return conn


def ingest_data(
        file_path: str,
        logger: logging.Logger = None,
        connection: snowflake.connector.connection = None
) -> List[Dict]:
    """
    Load JSON data into Snowflake.
    Args:
    file_path (str): JSON file path
    logger (logging.Logger, optional): Logger
    connection (snowflake.connector.connection, optional): Existing Snowflake connection
    Returns:
    List[Dict]: Loaded data
    """
    # Create connection if not provided
    close_connection = False
    if connection is None:
        connection = setup_snowflake_connection()
        close_connection = True

    try:
        if logger:
            logger.info(f"Starting data ingestion from {file_path}")

        # Load JSON data
        with open(file_path, 'r') as file:
            data = json.load(file)

        # Create cursor
        with connection.cursor() as cursor:
            # Create table (if not exists)
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS customer_data (
                id INT,
                timestamp TIMESTAMP,
                name STRING,
                email STRING,
                product_id STRING,
                product_name STRING,
                price FLOAT,
                comments STRING
            )
            """)

            # Insert data
            for record in data:
                customer_info = record['customer_info']
                for purchase in record['purchase_history']:
                    cursor.execute("""
                    INSERT INTO customer_data 
                    (id, timestamp, name, email, product_id, product_name, price, comments)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        record['id'],
                        record['timestamp'],
                        customer_info['name'],
                        customer_info['email'],
                        purchase['product_id'],
                        purchase['product_name'],
                        purchase['price'],
                        record.get('comments', '')
                    ))

            # Commit transaction
            connection.commit()

        if logger:
            logger.info(f"Successfully ingested {len(data)} records")

        return data

    except Exception as e:
        if logger:
            logger.error(f"Data ingestion failed: {str(e)}", exc_info=True)
        raise
    finally:
        # Close connection if we created it
        if close_connection and connection:
            connection.close()