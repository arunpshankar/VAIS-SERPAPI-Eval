
from requests.exceptions import ConnectionError
from tenacity import retry_if_exception_type
from tenacity import stop_after_attempt
from requests.exceptions import Timeout
from tenacity import wait_exponential
from src.config.logging import logger
from src.serp.search import get
from tenacity import retry
from typing import List
from typing import Dict 
import jsonlines
import csv
import os 


class APISingleton:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super().__new__(cls, *args, **kwargs)
            cls._instance._initialize()
        return cls._instance

    def _initialize(self):
        # Place any API loading or initialization logic here
        self.api_loaded = True
        logger.info("API initialized and loaded once.")

    def fetch_search_results(self, query: str) -> List[Dict]:
        return get(query)


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


@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=60), retry=retry_if_exception_type((ConnectionError, Timeout)))
def fetch_search_results(query: str) -> List[Dict]:
    """
    Fetch search results from the API with retry logic.
    Args:
        query (str): The search query string.
    Returns:
        List[Dict]: A list of search results.
    """
    api = APISingleton()
    return api.fetch_search_results(query)


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

                try:
                    search_results = fetch_search_results(search_query)
                    for result in search_results:
                        result['query'] = search_query
                        results.append(result)
                except Exception as e:
                    logger.error(f"Failed to fetch search results for query '{search_query}': {str(e)}")

        with jsonlines.open(output_path, mode='w') as writer:
            writer.write_all(results)
        logger.info(f"Results successfully written to {output_path}")
    except Exception as e:
        logger.error(f"An error occurred during the evaluation process: {str(e)}")
        raise


if __name__ == "__main__":
    input_csv_path = "./data/test-2.csv"
    output_jsonl_path = "./data/serp-batch-2-results.jsonl"
    evaluate(input_csv_path, output_jsonl_path)