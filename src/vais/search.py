from google.cloud import discoveryengine_v1beta as discoveryengine
from google.api_core.client_options import ClientOptions
from google.protobuf import json_format
from src.config.logging import logger 
from src.config.setup import config
from datetime import timedelta
from datetime import datetime
from typing import Optional
from typing import List
from typing import Dict 


LOCATION = "global" 


def convert_to_human_readable_date(date_str: str) -> str:
    """
    Convert various date string formats to a human-readable date format 'YYYY-MM-DD'.
    
    Args:
        date_str (str): The date string to convert.
        
    Returns:
        str: The human-readable date string, or an empty string if the input is invalid.
    """
    if not date_str:
        logger.warn(f"Invalid date string '{date_str}': String is None or empty")
        return ""
    
    try:
        # Handle format 'D:YYYYMMDDHHMMSS-XX'00'', 'D:YYYYMMDDHHMMSSZ', 'D:YYYYMMDDHHMMSS'
        if date_str.startswith("D:"):
            date_str = date_str[2:]
            try:
                dt = datetime.strptime(date_str[:14], "%Y%m%d%H%M%S")
            except ValueError:
                logger.warn(f"Invalid datetime part in date string '{date_str}'")
                return ""
            
            timezone_offset = date_str[14:]
            
            if timezone_offset == "":
                hours_offset = 0
            elif timezone_offset.startswith("Z"):
                hours_offset = 0
            elif timezone_offset and timezone_offset[0] in "+-":
                try:
                    hours_offset = int(timezone_offset[:3])
                except ValueError:
                    logger.warn(f"Invalid timezone offset in date string '{date_str}'")
                    return ""
            else:
                logger.warn(f"Invalid timezone offset in date string '{date_str}'")
                return ""
            
            dt = dt - timedelta(hours=hours_offset)
        
        elif len(date_str) > 20:
            dt = datetime.strptime(date_str, "%a %b %d %H:%M:%S %Y")
        
        else:
            logger.warn(f"Invalid date string '{date_str}': Unrecognized format")
            return ""
        
        human_readable_date = dt.strftime("%Y-%m-%d")

        return human_readable_date
    
    except ValueError as e:
        logger.error(f"Error converting date string '{date_str}': {e}")
        return ""



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
    Extracts title, snippet, link, creation and modified dates from the search response.
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
            "link": "", 
            "creation_date": "",
            "modified_date": ""
        }

        # Convert protocol buffer message to JSON
        result_json = json_format.MessageToDict(result.document._pb)

        # Extracting fields from JSON
        # struct_data = result_json.get('structData', {})
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

        # Extracting creation and modification date
        pagemap = derived_struct_data.get("pagemap", None)
        if pagemap:
            metatags = pagemap.get('metatags', None)
            if metatags:
                creationdate = metatags[0].get('creationdate', None)
                moddate = metatags[0].get('moddate', None)
            
                data["creation_date"] = convert_to_human_readable_date(creationdate)
                data["modified_date"] = convert_to_human_readable_date(moddate)

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
    query = "Nvidia Corp. Sustainability Report 2018 filetype:pdf site:nvidia.com"
    data_store_id = "vais-serp-evals-2_1716478907619"
    results = get(query, data_store_id)
    print(results)
