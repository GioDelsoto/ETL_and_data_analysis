import os
import pandas as pd
import json
from datetime import datetime
import requests
from airflow.utils.log.logging_mixin import LoggingMixin

# Logger setup for logging information and errors
logger = LoggingMixin().log

def fetch_google_ads_campaigns(store_id, customer_id, start_date, end_date, access_token, developer_token):
    """
    Fetches Google Ads campaign data between the specified start and end dates.

    Args:
        store_id (str): The store identifier used for naming output files.
        customer_id (str): Google Ads customer account ID.
        start_date (datetime): The start date of the data query.
        end_date (datetime): The end date of the data query.
        client_id (str): The client ID for OAuth 2.0 authentication.
        client_secret (str): The client secret for OAuth 2.0 authentication.
        refresh_token (str): The refresh token for OAuth 2.0 authentication.
        developer_token (str): The developer token for accessing the Google Ads API.

    Returns:
        tuple: Paths to the saved CSV files containing the full campaign data and unique campaigns.
    """

    # Ensure start and end dates are 'date' objects
    start_date = start_date.date() if isinstance(start_date, datetime) else start_date
    end_date = end_date.date() if isinstance(end_date, datetime) else end_date

    # Format the dates as strings in 'YYYY-MM-DD' format
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')

    # Configure the Google Ads API URL for fetching campaign data
    google_ads_url = f"https://googleads.googleapis.com/v18/customers/{customer_id}/googleAds:searchStream"

    # GAQL query to retrieve campaign cost and conversion data
    gaql_query = f"""
        SELECT
            campaign.id,
            campaign.name,
            campaign.advertising_channel_type,
            metrics.cost_micros,
            metrics.conversions,
            segments.date
        FROM
            campaign
        WHERE
            segments.date BETWEEN '{start_date_str}' AND '{end_date_str}'
    """

    # Set up the request headers, including the authorization token and developer token
    headers = {
        "Authorization": f"Bearer {access_token}",
        "developer-token": developer_token,
        "Content-Type": "application/json"
    }

    # Request body containing the GAQL query
    data = {
        "query": gaql_query
    }

    # Send request to Google Ads API
    response = requests.post(google_ads_url, headers=headers, json=data)

    # If the request was successful, process the data
    if response.status_code == 200:
        # Extract data from the response
        if len(response.json()) == 0:
            logger.info(f"No Google Ads data found for store {store_id} between {start_date_str} and {end_date_str}")
            return None, None

        response_data = response.json()[0]

        # List to store campaign data
        campaign_data = []

        # Iterate through the results and collect campaign details
        for batch in response_data['results']:
            campaign_id = batch['campaign']['id']
            campaign_name = batch['campaign']['name']
            advertising_channel_type = batch['campaign']['advertisingChannelType']
            cost_micros = float(batch['metrics']['costMicros']) / 1_000_000  # Convert cost from micros to currency
            conversions = batch['metrics']['conversions']
            date = batch['segments']['date']

            # Append data to the campaign_data list
            campaign_data.append({
                'campaign_id': campaign_id,
                'campaign_name': campaign_name,
                'channel': advertising_channel_type,
                'spend': cost_micros,
                'conversions': conversions,
                'date': date
            })

        # Create a DataFrame from the campaign data
        df_google = pd.DataFrame(campaign_data)
        df_google['date'] = pd.to_datetime(df_google['date'], format='%Y-%m-%d')
        # Create a unique campaigns DataFrame by removing duplicates based on 'campaign_id'
        unique_campaigns_google = df_google[['campaign_id','campaign_name', 'channel']].drop_duplicates(subset=['campaign_id'], keep='first')

        # Get the base directory for storing data files
        base_dir = os.path.dirname(os.path.abspath(__file__))

        # Directories for storing CSV files
        all_data_folder = os.path.join(base_dir, '../../temp_data/google_data')
        unique_campaigns_folder = os.path.join(base_dir, '../../temp_data/google_campaign')

        # Ensure that the directories exist
        os.makedirs(all_data_folder, exist_ok=True)
        os.makedirs(unique_campaigns_folder, exist_ok=True)

        # Define file paths for saving the data
        all_data_file_name = f"{store_id}_google_data_{start_date_str}_to_{end_date_str}.csv"
        unique_campaigns_file_name = f"{store_id}_unique_campaigns_{start_date_str}_to_{end_date_str}.csv"

        all_data_path = os.path.join(all_data_folder, all_data_file_name)
        unique_campaigns_path = os.path.join(unique_campaigns_folder, unique_campaigns_file_name)

        # Save the full campaign data to CSV
        try:
            df_google.to_csv(all_data_path, index=False)
            logger.info(f"Full Google Ads data saved at {all_data_path}")
        except Exception as e:
            logger.error(f"Error saving full Google Ads data to CSV: {e}")
            raise

        # Save the unique campaigns data to CSV
        try:
            unique_campaigns_google.to_csv(unique_campaigns_path, index=False)
            logger.info(f"Unique campaigns data saved at {unique_campaigns_path}")
        except Exception as e:
            logger.error(f"Error saving unique campaigns data to CSV: {e}")
            raise

        # Return the paths to the saved files
        return all_data_path, unique_campaigns_path

    else:
        # Log error if the request failed
        logger.error(f"Error in request: {response.status_code}, {response.text}")
