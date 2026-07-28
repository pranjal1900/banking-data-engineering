"""
Banking Data Engineering Platform — Incremental Processing (Watermarking)
=======================================================================
Handles fetching the last processed timestamp for a given dataset,
so we only read and process new files, minimizing compute costs.
"""

from datetime import datetime
import psycopg2
from config import config

class WatermarkManager:
    def __init__(self):
        self.db_config = config['database']

    def _get_connection(self):
        return psycopg2.connect(
            host=self.db_config['host'],
            port=self.db_config['port'],
            dbname=self.db_config['dbname'],
            user=self.db_config['user'],
            password=self.db_config['password']
        )

    def get_last_watermark(self, dataset: str) -> str:
        """Fetches the last processed timestamp for a dataset."""
        conn = self._get_connection()
        cur = conn.cursor()
        
        try:
            cur.execute(
                "SELECT last_processed FROM quality.pipeline_watermarks WHERE dataset = %s",
                (dataset,)
            )
            result = cur.fetchone()
            return result[0].isoformat() if result else "1970-01-01T00:00:00"
        finally:
            cur.close()
            conn.close()

    def update_watermark(self, dataset: str, new_watermark: datetime, run_id: str):
        """Updates the watermark after a successful pipeline run."""
        conn = self._get_connection()
        cur = conn.cursor()
        
        try:
            cur.execute("""
                INSERT INTO quality.pipeline_watermarks (dataset, last_processed, last_run_id)
                VALUES (%s, %s, %s)
                ON CONFLICT (dataset) 
                DO UPDATE SET 
                    last_processed = EXCLUDED.last_processed,
                    last_run_id = EXCLUDED.last_run_id,
                    updated_at = NOW()
            """, (dataset, new_watermark, run_id))
            conn.commit()
        finally:
            cur.close()
            conn.close()
