import os
import pandas as pd
import json
from datetime import datetime
import requests
from airflow.utils.log.logging_mixin import LoggingMixin

logger = LoggingMixin().log

def fetch_facebook_campaigns(store_id, token, ad_account, start_date, end_date):
    """
    Function to fetch Facebook campaigns data between a start and end date, and save the data into two CSV files.

    Arguments:
    store_id -- The store ID to be used in the CSV file names.
    token -- The Facebook API token.
    ad_account -- The Facebook Ad Account ID.
    start_date -- The start date in datetime format.
    end_date -- The end date in datetime format.

    Returns:
    all_data_path -- The path to the saved CSV file with all data.
    unique_campaigns_path -- The path to the saved CSV file with unique campaign data.
    """

    base_url = "https://graph.facebook.com/v21.0/"
    api_fields = [
        "spend", "objective", "campaign_name", "campaign_id",
        "actions", "cpm", "cpp"
    ]

    # Ensure dates are `date` objects
    start_date = start_date.date() if isinstance(start_date, datetime) else start_date
    end_date = end_date.date() if isinstance(end_date, datetime) else end_date

    # Format dates
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')

    # Build the API URL with time_increment=1
    url = (base_url
           + f"act_{ad_account}/insights?"
           + f"&fields={','.join(api_fields)}"
           + f"&time_range={{since:'{start_date_str}',until:'{end_date_str}'}}"
           + f"&time_increment=1"
           + f"&limit=1000"
           + f"&filtering=[{{field:'spend',operator:'GREATER_THAN',value:'0'}}]"
           + f"&level=campaign"
           + f"&access_token={token}")

    # Make the API request
    response = requests.get(url)
    response_decoded = json.loads(response.content.decode("utf-8"))

    # Check if data exists
    if 'data' not in response_decoded or len(response_decoded['data']) == 0:
        logger.info("No campaigns found.")
        return None, None

    # Process the data
    spend = []
    campaign_name = []
    campaign_id = []
    purchase = []
    cpm = []
    cpp = []
    objective = []
    dates = []

    for campaign in response_decoded['data']:
        spend.append(float(campaign.get('spend', 0)))
        campaign_name.append(campaign.get('campaign_name', ''))
        campaign_id.append(campaign.get('campaign_id', ''))
        cpm.append(float(campaign.get('cpm', 0)))
        cpp.append(float(campaign.get('cpp', 0)))
        objective.append(campaign.get('objective', ''))
        dates.append(campaign.get('date_start', ''))  # 'date_start' contains daily data

        # Get actions like purchases
        purchase.append(next(
            (int(action['value']) for action in campaign.get('actions', [])
             if action['action_type'] == 'purchase'),
            0
        ))

    # Create full dataframe
    df_facebook = pd.DataFrame({
        'campaign_name': campaign_name,
        'campaign_id': campaign_id,
        'spend': spend,
        'purchase': purchase,
        'cpm': cpm,
        'cpr': cpp,
        'objective': objective,
        'date': dates
    })

    # Create unique campaign dataframe
    unique_campaigns_df = df_facebook[['campaign_name', 'campaign_id', 'objective']].drop_duplicates(subset = ['campaign_id'], keep = 'first')

    base_dir = os.path.dirname(os.path.abspath(__file__))

    # Directories for storing CSVs
    all_data_folder = os.path.join(base_dir, '../../temp_data/facebook_data')
    unique_campaigns_folder = os.path.join(base_dir, '../../temp_data/facebook_campaign')

    # Ensure the folders exist
    os.makedirs(all_data_folder, exist_ok=True)
    os.makedirs(unique_campaigns_folder, exist_ok=True)

    # File paths
    all_data_file_name = f"{store_id}_fb_data_{start_date_str}_to_{end_date_str}.csv"
    unique_campaigns_file_name = f"{store_id}_unique_campaigns_{start_date_str}_to_{end_date_str}.csv"

    all_data_path = os.path.join(all_data_folder, all_data_file_name)
    unique_campaigns_path = os.path.join(unique_campaigns_folder, unique_campaigns_file_name)

    # Save CSVs
    try:
        df_facebook.to_csv(all_data_path, index=False)
        logger.info(f"Full Facebook data saved at {all_data_path}")
    except Exception as e:
        logger.error(f"Error saving full Facebook data to CSV: {e}")
        raise

    try:
        unique_campaigns_df.to_csv(unique_campaigns_path, index=False)
        logger.info(f"Unique campaigns data saved at {unique_campaigns_path}")
    except Exception as e:
        logger.error(f"Error saving unique campaigns data to CSV: {e}")
        raise

    return all_data_path, unique_campaigns_path
