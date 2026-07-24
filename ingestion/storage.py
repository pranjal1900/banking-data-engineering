"""
Banking Data Engineering Platform — Storage Abstraction
=====================================================
Handles reading/writing data to local filesystem or AWS S3.
Uses `config.yaml` to determine the active environment.
"""

import os
import boto3
from pathlib import Path
import pandas as pd
from typing import Union, List

# Load config using the loader we built in Phase 1
from config import config

class StorageManager:
    def __init__(self):
        self.env = config["env"]
        self.local_base = Path(config["storage"]["local_dir"])
        
        # S3 Setup
        if self.env == "prod":
            self.s3_bucket = config["storage"]["s3_bucket"]
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
                region_name=os.getenv('AWS_REGION', 'us-east-1')
            )
        
        # Ensure local dirs exist
        self._create_local_dirs()

    def _create_local_dirs(self):
        """Creates the local data lake directory structure."""
        layers = ['raw', 'processed', 'curated', 'rejected', 'sample']
        datasets = ['customers', 'accounts', 'transactions', 'merchants', 'branches']
        
        for layer in layers:
            for dataset in datasets:
                (self.local_base / layer / dataset).mkdir(parents=True, exist_ok=True)

    def write_dataframe(self, df: pd.DataFrame, layer: str, dataset: str, filename: str):
        """
        Writes a Pandas DataFrame to the target storage (Local or S3).
        """
        if self.env == "local" or self.env == "dev":
            self._write_local(df, layer, dataset, filename)
        else:
            self._write_s3(df, layer, dataset, filename)

    def _write_local(self, df: pd.DataFrame, layer: str, dataset: str, filename: str):
        """Writes data locally in CSV or Parquet format."""
        file_path = self.local_base / layer / dataset / filename
        
        if filename.endswith('.csv'):
            df.to_csv(file_path, index=False)
        elif filename.endswith('.parquet'):
            df.to_parquet(file_path, index=False)
        else:
            raise ValueError(f"Unsupported file format: {filename}")
            
    def _write_s3(self, df: pd.DataFrame, layer: str, dataset: str, filename: str):
        """Writes data to AWS S3."""
        s3_key = f"data/{layer}/{dataset}/{filename}"
        
        # Write to temporary local file first
        tmp_path = f"/tmp/{filename}"
        
        if filename.endswith('.csv'):
            df.to_csv(tmp_path, index=False)
        elif filename.endswith('.parquet'):
            df.to_parquet(tmp_path, index=False)
            
        # Upload to S3
        self.s3_client.upload_file(tmp_path, self.s3_bucket, s3_key)
        
        # Clean up temp file
        os.remove(tmp_path)

    def list_files(self, layer: str, dataset: str) -> List[str]:
        """Lists all files in a specific dataset folder."""
        if self.env == "local" or self.env == "dev":
            path = self.local_base / layer / dataset
            return [str(p) for p in path.glob('*.*')]
        else:
            prefix = f"data/{layer}/{dataset}/"
            response = self.s3_client.list_objects_v2(Bucket=self.s3_bucket, Prefix=prefix)
            if 'Contents' in response:
                return [f"s3://{self.s3_bucket}/{obj['Key']}" for obj in response['Contents']]
            return []
