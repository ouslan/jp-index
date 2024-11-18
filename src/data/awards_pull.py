import time
import pandas as pd
import requests

# API configuration
URL = "https://api.usaspending.gov/api/v2/search/spending_by_award/"
HEADERS = {"Content-Type": "application/json"}

def build_payload(start_date: str, end_date: str) -> dict:
    """
    Build the API request payload for a given fiscal year range.

    Args:
        start_date (str): Start date for the fiscal year (e.g., "2007-10-01").
        end_date (str): End date for the fiscal year (e.g., "2008-09-30").

    Returns:
        dict: Payload with filters for the API request.
    """
    return {
        "subawards": False,
        "limit": 100,
        "filters": {
            "award_type_codes": ["A", "B", "C", "D"],
            "time_period": [{"start_date": start_date, "end_date": end_date}],
            "place_of_performance_locations": [{"country": "USA", "state": "PR"}],
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
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as error:
            wait_time = 2**attempt
            print(f"Error: {error}. Retrying in {wait_time} seconds...")
            time.sleep(wait_time)

    print("Error: Request failed after multiple retries.")
    return None

def get_data_for_year(year: int) -> list:
    """
    Retrieve all award data for a specific fiscal year.

    Args:
        year (int): The fiscal year to retrieve data for.

    Returns:
        list: A list of awards retrieved from the API.
    """
    start_date = f"{year - 1}-10-01"  # Fiscal year starts on Oct 1 of the previous year
    end_date = f"{year}-09-30"  # Fiscal year ends on Sep 30 of the current year
    all_data = []
    page = 1

    while True:
        print(f"Downloading page {page} for FY {year}...")
        payload = build_payload(start_date, end_date)
        payload["page"] = page

        response = make_request(URL, payload)
        if response is None:
            print(f"Error: No data received for FY {year}.")
            break

        data = response.get("results", [])
        if not data:
            print(f"No more data available for FY {year}.")
            break

        all_data.extend(data)
        page += 1

    return all_data

def save_to_csv(data: list, year: int) -> None:
    """
    Save the collected data to a CSV file.

    Args:
        data (list): List of awards data.
        year (int): The fiscal year for which the data was retrieved.
    """
    df = pd.DataFrame(data)
    output_file = f"prime_award_results_{year}.csv"
    df.to_csv(output_file, index=False)
    print(f"Data for FY {year} saved to {output_file}")

def main():
    """
    Main function to retrieve data for multiple fiscal years.
    """
    start_year = 2008  # Starting year (you can adjust this as needed)
    end_year = 2023  # Ending year (you can adjust this as needed)

    for year in range(start_year, end_year + 1):
        print(f"Starting data retrieval for FY {year}...")
        data = get_data_for_year(year)

        if data:
            save_to_csv(data, year)
        else:
            print(f"No data available for FY {year}.")

if __name__ == "__main__":
    main()
