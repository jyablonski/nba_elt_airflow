import requests
import pandas as pd


def scrape_endpoint(
    endpoint: str, context: dict, base_api_endpoint: str = "https://api.jyablonski.dev/"
) -> pd.DataFrame:
    try:
        response = requests.get(f"{base_api_endpoint}/{endpoint}")

        df = pd.DataFrame(response.json())
        df["scrape_ts"] = context["data_interval_end"]

        print(f"Successfully scraped {endpoint} data with {len(df)} records")
        return df
    except BaseException as e:
        raise e(f"Error Occurred while scraping {base_api_endpoint}/{endpoint}, {e}")
