from src.config.logging import logger
import pandas as pd
import logging


def load_and_combine_csv(file_paths: list[str], logger: logging.Logger) -> pd.DataFrame:
    """
    Loads multiple CSV files and combines them into a single DataFrame.
    
    Args:
        file_paths (list[str]): List of paths to the CSV files.
        logger (logging.Logger): Logger instance for logging.
    
    Returns:
        pd.DataFrame: Combined DataFrame from all CSV files.
    """
    try:
        dataframes = [pd.read_csv(file) for file in file_paths]
        combined_df = pd.concat(dataframes, ignore_index=True)
        logger.info("CSV files loaded and combined successfully.")
        return combined_df
    except Exception as e:
        logger.error(f"Error loading and combining CSV files: {e}")
        raise


def filter_and_sort_dataframe(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """
    Filters the DataFrame to keep rows where the creation_date is 2023 or 2024,
    or where the creation_date is empty or None. Then sorts the remaining rows
    by creation_date in descending order.
    
    Args:
        df (pd.DataFrame): The DataFrame to filter and sort.
        logger (logging.Logger): Logger instance for logging.
    
    Returns:
        pd.DataFrame: The filtered and sorted DataFrame.
    """
    try:
        filtered_df = df[
            (df['creation_date'].str.contains('2023|2024', na=False)) |
            (df['creation_date'].isnull()) |
            (df['creation_date'] == '')
        ]
        filtered_sorted_df = filtered_df.sort_values(by='creation_date', ascending=False, na_position='last')
        logger.info("DataFrame filtered and sorted successfully.")
        return filtered_sorted_df
    except Exception as e:
        logger.error(f"Error filtering and sorting DataFrame: {e}")
        raise


def rank_dataframe(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """
    Adds a rank column to the DataFrame based on the creation_date in descending order,
    grouped by the 'query' column, and ranks within each group starting from 1.
    
    Args:
        df (pd.DataFrame): The DataFrame to rank.
        logger (logging.Logger): Logger instance for logging.
    
    Returns:
        pd.DataFrame: The ranked DataFrame.
    """
    try:
        # Temporarily fill NA values to avoid errors during ranking
        df['temp_creation_date'] = df['creation_date'].fillna('9999-12-31')
        
        # Group by 'query' and rank within each group
        df['rank'] = df.groupby('query')['temp_creation_date'].rank(method='first', ascending=False).astype('Int64')
        
        # Drop the temporary column
        df.drop(columns=['temp_creation_date'], inplace=True)
        
        logger.info("DataFrame ranked successfully.")
        return df
    except Exception as e:
        logger.error(f"Error ranking DataFrame: {e}")
        raise


def save_dataframe(df: pd.DataFrame, output_path: str, logger: logging.Logger) -> None:
    """
    Saves the DataFrame to a CSV file.
    
    Args:
        df (pd.DataFrame): The DataFrame to save.
        output_path (str): The path to save the CSV file.
        logger (logging.Logger): Logger instance for logging.
    """
    try:
        # Sort the DataFrame by 'query' to group results together
        df_sorted_by_query = df.sort_values(by=['query', 'rank'])
        df_sorted_by_query.to_csv(output_path, index=False)
        logger.info(f"DataFrame saved successfully to {output_path}.")
    except Exception as e:
        logger.error(f"Error saving DataFrame to CSV: {e}")
        raise

def run() -> None:
    """
    Main function to execute the data processing pipeline.
    """
    file_paths = [
        './data/vais-batch-1-results.csv',
        './data/vais-batch-2-results.csv',
        './data/vais-cdn-results.csv'
    ]
    try:
        combined_df = load_and_combine_csv(file_paths, logger)
        filtered_sorted_df = filter_and_sort_dataframe(combined_df, logger)
        ranked_df = rank_dataframe(filtered_sorted_df, logger)
        output_path = './data/vais_consolidated_results.csv'
        save_dataframe(ranked_df, output_path, logger)
        logger.info("Data processing pipeline completed successfully.")
    except Exception as e:
        logger.error(f"Error in data processing pipeline: {e}")
        raise


if __name__ == "__main__":
    run()
