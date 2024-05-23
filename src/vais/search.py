
from google.cloud import discoveryengine_v1beta as discoveryengine
from google.api_core.client_options import ClientOptions
from google.protobuf import json_format
from src.config.logging import logger 
from src.config.setup import config
from typing import Optional
from typing import List
from typing import Dict 


LOCATION = "global" 


def search_data_store(search_query: str, data_store_id: str) -> Optional[discoveryengine.SearchResponse]:
    """
    Search the data store using Google Cloud's Discovery Engine API.
    Args:
        search_query (str): The search query string.
    Returns:
        Optional[discoveryengine.SearchResponse]: The search response from the Discovery Engine API.
    """
    try:
        client_options = (
            ClientOptions(api_endpoint=f"{LOCATION}-discoveryengine.googleapis.com")
            if LOCATION != "global"
            else None
        )

        client = discoveryengine.SearchServiceClient(client_options=client_options)

        serving_config = client.serving_config_path(
            project=config.PROJECT_ID,
            location=LOCATION,
            data_store=data_store_id,
            serving_config="default_config",
        )

        content_search_spec = discoveryengine.SearchRequest.ContentSearchSpec(
            snippet_spec=discoveryengine.SearchRequest.ContentSearchSpec.SnippetSpec(
                return_snippet=True
            )
        )

        request = discoveryengine.SearchRequest(
            serving_config=serving_config,
            query=search_query,
            page_size=10,
            content_search_spec=content_search_spec,
            query_expansion_spec=discoveryengine.SearchRequest.QueryExpansionSpec(
                condition=discoveryengine.SearchRequest.QueryExpansionSpec.Condition.AUTO,
            ),
            spell_correction_spec=discoveryengine.SearchRequest.SpellCorrectionSpec(
                mode=discoveryengine.SearchRequest.SpellCorrectionSpec.Mode.AUTO
            ),
        )

        response = client.search(request)
        return response

    except Exception as e:
        logger.error(f"Error during data store search: {e}")
        return None


def extract_relevant_data(response: Optional[discoveryengine.SearchResponse]) -> List[Dict[str, str]]:
    """
    Extracts title, snippet, and link from the search response.
    Args:
        response (Optional[discoveryengine.SearchResponse]): The search response object from the Discovery Engine API.
    Returns:
        List[Dict[str, str]]: A list of dictionaries containing the extracted information.
    """
    extracted_data = []

    if response is None:
        logger.error("No response received to extract data.")
        return extracted_data

    for result in response.results:
        data = {
            "title": "",
            "snippet": "",
            "link": ""
        }

        # Convert protocol buffer message to JSON
        result_json = json_format.MessageToDict(result.document._pb)

        # Extracting fields from JSON
        struct_data = result_json.get('structData', {})
        derived_struct_data = result_json.get('derivedStructData', {})

        # Extracting title
        title = derived_struct_data.get("title")
        if title:
            data["title"] = title

        # Extracting snippet
        snippets = derived_struct_data.get("snippets")
        if snippets:
            data["snippet"] = snippets[0]['snippet']

        # Extracting link
        link = derived_struct_data.get("link")
        if link:
            data["link"] = link

        extracted_data.append(data)

    return extracted_data


def get(query: str, data_store_id: str) -> List[Dict[str, str]]:
    """
    Retrieves and returns the top search results for a given query from the data store.
    Args:
        query (str): Search query.
        data_store_id (str): Unique identifier of the data store.
    Returns:
        List[Dict[str, str]]: A list containing the search results.
    """
    response = search_data_store(query, data_store_id)
    return extract_relevant_data(response)


if __name__ == "__main__":
    query = "ViaSat, Inc. Sustainability Report 2023 filetype:pdf site:investors.viasat.com"
    data_store_id = "vais-serp-evals-2_1716478907619"
    results = get(query, data_store_id)
    print(results)
