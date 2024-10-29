import time

import pandas as pd
import requests

# API configuration
URL = "https://api.usaspending.gov/api/v2/search/spending_by_award/"
HEADERS = {"Content-Type": "application/json"}

# Payload with specific filters for the request
PAYLOAD = {
    "subawards": False,  # We're only interested in prime awards
    "limit": 100,  # Limit the number of results per page to 100
    "filters": {
        "award_type_codes": ["A", "B", "C", "D"],  # Contracts
        "time_period": [
            {"start_date": "2007-10-01", "end_date": "2008-09-30"}  # FY 2008
        ],
        "place_of_performance_locations": [
            {"country": "USA", "state": "PR"}  # Puerto Rico as performance location
        ],
    },
    "fields": [
        "Award ID",
        "Recipient Name",
        "Start Date",
        "End Date",
        "Award Amount",
        "Awarding Agency",
        "Awarding Sub Agency",
        "Funding Agency",
        "Funding Sub Agency",
        "Award Type",
    ],
}


def make_request(url: str, payload: dict, retries: int = 5) -> dict:
    """
    Make a POST request to the API with exponential backoff in case of failures.

    Args:
        url (str): API endpoint URL.
        payload (dict): JSON payload with the filters and fields.
        retries (int): Number of retry attempts before giving up.

    Returns:
        dict: JSON response from the API, or None if the request failed.
    """
    for attempt in range(retries):
        try:
            response = requests.post(url, json=payload, headers=HEADERS, timeout=60)
            response.raise_for_status()  # Raise an exception for 4xx/5xx responses
            return response.json()  # Return the response as a JSON object
        except requests.exceptions.RequestException as error:
            wait_time = 2**attempt  # Exponential backoff: 2^attempt seconds
            print(f"Error: {error}. Retrying in {wait_time} seconds...")
            time.sleep(wait_time)

    print("Error: Request failed after multiple retries.")
    return None  # Return None if all attempts failed


def get_data() -> None:
    """
    Download paginated data from the API and store it in a CSV file.
    """
    all_data = []  # Initialize an empty list to store all results
    page = 1  # Start from the first page

    while True:
        print(f"Downloading page {page}...")
        PAYLOAD["page"] = page  # Update the current page in the payload

        response = make_request(URL, PAYLOAD)  # Make the API request
        if response is None:
            print("Error: No data received. Stopping execution.")
            break

        data = response.get("results", [])  # Extract results from the response
        if not data:
            print("No more data available.")
            break

        all_data.extend(data)  # Append the data from the current page
        page += 1  # Move to the next page

    save_to_csv(all_data)  # Save the collected data to a CSV file


def save_to_csv(data: list) -> None:
    """
    Save the collected data into a CSV file.

    Args:
        data (list): List of dictionaries representing the API results.
    """
    df = pd.DataFrame(data)  # Convert the data into a DataFrame
    output_file = "prime_award_results.csv"  # Define the output file name
    df.to_csv(output_file, index=False)  # Save the DataFrame to CSV
    print(f"Data saved to {output_file}")


if __name__ == "__main__":
    get_data()  # Run the data download process
