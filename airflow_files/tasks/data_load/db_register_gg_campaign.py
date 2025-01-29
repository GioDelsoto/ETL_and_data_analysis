import pandas as pd
import psycopg2
from psycopg2 import sql
from airflow.utils.log.logging_mixin import LoggingMixin

logger = LoggingMixin().log

def db_register_campaign(df_campaign: pd.DataFrame, db_url: str, store_id: int) -> None:

    try:
        conn = psycopg2.connect(db_url)
        cursor = conn.cursor()
        for _, row in df_campaign.iterrows():
            try:

                query = sql.SQL("""
                    INSERT INTO etl_schema.google_ads_campaigns (
                        id, name, channel, store_id
                    ) VALUES (%s, %s, %s, %s)
                    ON CONFLICT (id) DO NOTHING
                """)
                cursor.execute(query, (
                    row['campaign_id'], row['campaign_name'], row['channel'], store_id
                ))
            except psycopg2.Error as e:
                logger.error(f"Failed to insert fb campaign - {row['campaign_id']}: Store {store_id}: {e}", extra={
                "sql_state": e.pgcode,
                "pg_error": e.pgerror,
                })

        conn.commit()

    except Exception as conn_error:
        logger.critical(f"Database connection failed at db_register_gg_campaign.py: {conn_error}", exc_info=True)

    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()
