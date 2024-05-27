
from src.config.logging import logger
from typing import List
from typing import Dict
import pandas as pd
import json


def read_jsonl(file_path: str) -> List[Dict]:
    """
    Reads a JSONL file and returns a list of dictionaries.
    
    :param file_path: Path to the JSONL file
    :return: List of dictionaries with the data
    """
    logger.info(f'Reading JSONL file from {file_path}')
    data = []
    with open(file_path, 'r') as file:
        for line in file:
            data.append(json.loads(line))
    logger.info('Finished reading JSONL file')
    return data


def convert_to_dataframe(data: List[Dict]) -> pd.DataFrame:
    """
    Converts a list of dictionaries to a pandas DataFrame.
    
    :param data: List of dictionaries
    :return: DataFrame with the data
    """
    logger.info('Converting data to DataFrame')
    df = pd.DataFrame(data)
    return df


def reorder_columns(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    """
    Reorders the columns of a DataFrame.
    
    :param df: Input DataFrame
    :param columns: List of columns in the desired order
    :return: DataFrame with reordered columns
    """
    logger.info(f'Reordering columns: {columns}')
    df = df[columns]
    return df


def save_to_csv(df: pd.DataFrame, file_path: str) -> str:
    """
    Saves a DataFrame to a CSV file.
    
    :param df: DataFrame to save
    :param file_path: Path to the output CSV file
    :return: Path to the saved CSV file
    """
    logger.info(f'Saving DataFrame to CSV file at {file_path}')
    df.to_csv(file_path, index=False)
    logger.info('Finished saving CSV file')
    return file_path


def main():
    jsonl_path = './data/vais-batch-2-results.jsonl'
    csv_path = './data/vais-batch-2-results.csv'
    desired_columns = ['query', 'title', 'url', 'snippet', 'creation_date', 'modified_date']
    
    # Step 1: Read the JSONL file
    data = read_jsonl(jsonl_path)
    
    # Step 2: Convert to DataFrame
    df = convert_to_dataframe(data)
    
    # Step 3: Rename columns as needed
    df.rename(columns={'link': 'url'}, inplace=True)
    
    # Step 4: Reorder columns
    df = reorder_columns(df, desired_columns)
    
    # Step 5: Save to CSV
    save_to_csv(df, csv_path)

    logger.info(f'Process completed. CSV saved at {csv_path}')


if __name__ == "__main__":
    main()

