from src.config.logging import logger
import pandas as pd


def load_and_combine_csv(file_paths: list[str]) -> pd.DataFrame:
    """
    Loads multiple CSV files and combines them into a single DataFrame.
    
    Args:
        file_paths (list[str]): List of paths to the CSV files.
    
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




def save_dataframe(df: pd.DataFrame, output_path: str) -> None:
    """
    Saves the DataFrame to a CSV file.
    
    Args:
        df (pd.DataFrame): The DataFrame to save.
        output_path (str): The path to save the CSV file.
    """
    try:
        df.to_csv(output_path, index=False)
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
        combined_df = load_and_combine_csv(file_paths)
        output_path = './data/vais_consolidated_results.csv'
        save_dataframe(combined_df, output_path)
        logger.info("Data processing pipeline completed successfully.")
    except Exception as e:
        logger.error(f"Error in data processing pipeline: {e}")
        raise


if __name__ == "__main__":
    run()
