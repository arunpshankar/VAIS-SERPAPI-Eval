from typing import Optional
from typing import Dict
from typing import Any 
import requests
import logging
import yaml 


def load_api_key(file_path: str) -> Optional[str]:
    try:
        with open(file_path, 'r') as file:
            config = yaml.safe_load(file)
            return config['serphouse']['key']
    except Exception as e:
        print(e)
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
    
    headers = {
        'accept': "application/json",
        'content-type': "application/json",
        'authorization': f"Bearer {api_key}"
    }

    try:
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()  # Raises an HTTPError for bad responses
        return response.json()
    except requests.RequestException as e:
        logging.error(f"Failed to fetch search results: {e}")
        raise

if __name__ == "__main__":
    query = "procter and gamble annual rpeort 2023 site:assets.ctfassets.net"
    response = fetch_search_results(query)
    results = response['results']['results']['organic']
    for item in results:
        rank = item['position']
        title = item['title']
        url = item['link']
        snippet = item['snippet']
        print(rank, title)
        print(url)
        print(snippet)
        print('-' * 100)

