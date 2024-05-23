from typing import Optional, Dict, Any, List
import requests
import logging
import yaml

logging.basicConfig(level=logging.INFO)

def load_api_key(file_path: str) -> Optional[str]:
    """
    Load the API key from a YAML file.

    Parameters:
        file_path (str): The path to the YAML file containing the API key.

    Returns:
        Optional[str]: The API key if found, otherwise None.
    """
    try:
        with open(file_path, 'r') as file:
            config = yaml.safe_load(file)
            logging.info("API key loaded successfully.")
            return config['serphouse']['key']
    except Exception as e:
        logging.error(f"Error loading API key: {e}")
        return None


def fetch_search_results(query: str) -> Dict[str, Any]:
    """
    Fetch search results from the SerpHouse API for a given query.

    Parameters:
        query (str): The search query.

    Returns:
        Dict[str, Any]: The JSON response from the API.

    Raises:
        Exception: If the API call fails or returns an unexpected response.
    """
    url = "https://api.serphouse.com/serp/live"
    payload = {
        "data": {
            "q": query,
            "domain": "google.com",
            "lang": "en",
            "device": "desktop",
            "serp_type": "web",
            "loc": "United States",
            "verbatim": "0",
            "gfilter": "0",
            "page": "1",
            "num_result": "10"
        }
    }
    api_key = load_api_key('./credentials/keys.yaml')

    if not api_key:
        raise ValueError("API key is missing.")

    headers = {
        'accept': "application/json",
        'content-type': "application/json",
        'authorization': f"Bearer {api_key}"
    }

    try:
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()  # Raises an HTTPError for bad responses
        logging.info("Search results fetched successfully.")
        return response.json()
    except requests.RequestException as e:
        logging.error(f"Failed to fetch search results: {e}")
        raise


def process_results(response: Dict[str, Any]) -> List[Dict[str, str]]:
    """
    Process the search results and extract title, snippet, and link.

    Parameters:
        response (Dict[str, Any]): The JSON response from the API.

    Returns:
        List[Dict[str, str]]: A list of dictionaries containing title, snippet, and link.
    """
    try:
        results = response['results']['results']['organic']
        processed_results = []
        for item in results:
            result = {
                'title': item['title'],
                'snippet': item['snippet'],
                'link': item['link']
            }
            processed_results.append(result)
        logging.info("Search results processed successfully.")
        return processed_results
    except KeyError as e:
        logging.error(f"Error processing results: {e}")
        raise


if __name__ == "__main__":
    query = "procter and gamble annual report 2023 site:assets.ctfassets.net"
    try:
        response = fetch_search_results(query)
        results = process_results(response)
        print(results)
        for result in results:
            print(result)
            print('-' * 100)
    except Exception as e:
        logging.error(f"An error occurred: {e}")
