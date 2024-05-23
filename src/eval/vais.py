
from src.config.logging import logger
from src.vais.search import get
from typing import List
from typing import Dict 
import jsonlines
import csv
import os 


DATA_STORE_ID = 'vais-serp-evals-2_1716478907619'


def ensure_directory(path: str) -> None:
    """
    Ensure that the directory for the given path exists. If not, it creates the directory.
    Args:
        path (str): The file path where the directory needs to be checked or created.
    """
    directory = os.path.dirname(path)
    if not os.path.exists(directory):
        os.makedirs(directory)
        logger.info(f"Directory {directory} created.")


def evaluate(input_path: str, output_path: str) -> None:
    """
    Evaluates the sustainability reports of companies by reading their names and websites from a CSV,
    constructing a search query for each, fetching the search results, and writing the results to a JSON lines file.

    Args:
        input_path (str): The path to the input CSV file with columns 'company_name' and 'url'.
        output_path (str): The path to the output JSON lines file where search results will be stored.
    """
    logger.info("Starting the evaluation process.")
    ensure_directory(output_path)  # Ensure the output directory exists
    
    results: List[Dict] = []
    try:
        with open(input_path, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                company_name = row['company_name'].strip()
                url = row['url'].strip()
                search_query = f"{company_name} Sustainability Report 2023 filetype:pdf site:{url}"
                logger.info(f'Searching with Query: {search_query}')
                logger.debug(f"Constructed search query: {search_query}")
                
                search_results = get(search_query, DATA_STORE_ID)
                for result in search_results:
                    result['query'] = search_query
                    results.append(result)
        
        with jsonlines.open(output_path, mode='w') as writer:
            writer.write_all(results)
        logger.info(f"Results successfully written to {output_path}")
    except Exception as e:
        logger.error(f"An error occurred during the evaluation process: {str(e)}")
        raise


if __name__ == "__main__":
    input_csv_path = "./data/test-2.csv"
    output_jsonl_path = "./data/vais-batch-2-results.jsonl"
    evaluate(input_csv_path, output_jsonl_path)
