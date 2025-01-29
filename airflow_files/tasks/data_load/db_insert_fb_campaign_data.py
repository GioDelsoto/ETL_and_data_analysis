import pandas as pd
import psycopg2
from psycopg2 import sql
from airflow.utils.log.logging_mixin import LoggingMixin

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
                query = sql.SQL("""
                    INSERT INTO etl_schema.facebook_ads_campaigns_results (
                        spend, purchase, cpm, cpr, date, campaign_id
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (date, campaign_id) DO NOTHING
                """)
                cursor.execute(query, (
                    row['spend'], row['purchase'], row['cpm'], row['cpr'], row['date'], row['campaign_id']
                ))


            except psycopg2.Error as e:
                logger.error(f"Failed to insert fb data {row['campaign_id']}: Store {store_id}: {e}", extra={
                "sql_state": e.pgcode,
                "pg_error": e.pgerror,
                })

        conn.commit()

    except Exception as conn_error:
        logger.critical(f"Database connection failed at db_register_fb_campaign: {conn_error}", exc_info=True)

    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()
