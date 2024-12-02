import re
import pandas as pd
from datetime import datetime


def clean_text(text):
    """
    Cleans text by removing special characters, extra spaces, and non-alphanumeric content.
    """
    if not text:
        return None  # Handle missing text
    # Remove special characters
    text = re.sub(r'[^A-Za-z0-9\s]+', '', text)
    # Remove extra whitespaces
    text = ' '.join(text.split())
    return text


def flatten_json(nested_json):
    """
    Flattens a list of nested JSON objects into a DataFrame.
    """
    flat_data = []
    for record in nested_json:
        if "purchase_history" in record:
            for purchase in record["purchase_history"]:
                flat_record = record.copy()
                flat_record.pop("purchase_history", None)
                flat_record.update(purchase)  # Add nested fields as top-level fields
                flat_data.append(flat_record)
        else:
            flat_data.append(record)
    return pd.DataFrame(flat_data)


def handle_missing_values(data, strategy="drop", fill_value=None):
    """
    Handles missing values in a dataset.

    Parameters:
        data (list[dict]): The dataset (list of dictionaries).
        strategy (str): 'drop' to remove rows with missing values,
                        'fill' to replace with `fill_value`.
        fill_value (any): Value to use for imputation if strategy='fill'.
    """
    processed_data = []
    for record in data:
        if strategy == "drop":
            if None not in record.values():
                processed_data.append(record)  # Keep only records without missing values
        elif strategy == "fill":
            for key, value in record.items():
                if value is None:
                    record[key] = fill_value
            processed_data.append(record)
    return processed_data


def normalize_timestamps(data, date_field="timestamp", date_format="%Y-%m-%d %H:%M:%S"):
    """
    Normalizes the timestamp field to a consistent format.
    """
    for record in data:
        try:
            record[date_field] = datetime.strptime(record[date_field], date_format).strftime(date_format)
        except (ValueError, TypeError):
            record[date_field] = None  # Handle invalid or missing dates
    return data


def remove_duplicates(data, unique_key="id"):
    """
    Removes duplicate records based on a unique key.
    """
    seen = set()
    unique_data = []
    for record in data:
        if record[unique_key] not in seen:
            seen.add(record[unique_key])
            unique_data.append(record)
    return unique_data


def preprocess_data(data):
    """
    Full preprocessing pipeline: clean text, flatten JSON, handle missing values, normalize dates, and remove duplicates.
    """
    # Clean text fields
    for record in data:
        if "comments" in record:
            record["comments"] = clean_text(record["comments"])

    # Flatten JSON
    df = flatten_json(data)

    # Handle missing values
    df = handle_missing_values(df.to_dict(orient="records"), strategy="fill", fill_value="N/A")

    # Normalize timestamps
    df = normalize_timestamps(df, date_field="timestamp")

    # Remove duplicates
    df = remove_duplicates(df)

    return df

