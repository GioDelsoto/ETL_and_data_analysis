import pandas as pd
import psycopg2
from psycopg2 import sql
from airflow.utils.log.logging_mixin import LoggingMixin
#from tasks.data_load.db_insert_campaign import db_register_campaign

logger = LoggingMixin().log

def db_insert_campaign_data(df_campaign: pd.DataFrame, db_url: str, store_id: int) -> None:
    """
    Inserts a DataFrame of campaign data into the campaign results table in the database.
    """
    try:
        conn = psycopg2.connect(db_url)
        cursor = conn.cursor()
        for _, row in df_campaign.iterrows():
            try:

                # # Check if campaign exists in campaign table
                # campaign_query = sql.SQL("""
                #     SELECT campaign_id FROM etl_schema.facebook_ads_campaigns WHERE id = %s
                # """)
                # cursor.execute(campaign_query, (row['campaign_id'],))
                # campaign_exists = cursor.fetchone()

                # # If campaign doesn't exist, insert it


                # if not campaign_exists:
                #     df_campaign_register = pd.DataFrame({"id": [row['campaign_id']], "name": [row['campaign_name']], "Campaign_objective": [row['objective']], "store_id": [store_id]})
                #     db_register_campaign(df_campaign_register, db_url, store_id)

                query = sql.SQL("""
                    INSERT INTO etl_schema.facebook_ads_campaigns_results (
                        spend, results, cpm, cpr, date, campaign_id, store_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """)
                cursor.execute(query, (
                    row['spend'], row['results'], row['cpm'], row['cpr'], row['date'], row['campaign_id'], store_id
                ))


            except psycopg2.Error as e:
                logger.error(f"Failed to insert fb data {row['date']} - {row['campaign_id']}: Store {store_id}: {e}", extra={
                "sql_state": e.pgcode,
                "pg_error": e.pgerror,
                })

        conn.commit()

    except Exception as conn_error:
        logger.critical(f"Database connection failed at db_insert_campaign_data: {conn_error}", exc_info=True)

    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()
