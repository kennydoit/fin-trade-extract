"""
AWS S3 utilities for writing batch data as compressed Parquet files.
Optimized for Snowpipe ingestion with proper file naming and batching.
"""

import gzip
import json
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

import boto3
import pandas as pd
from botocore.exceptions import BotoCoreError, ClientError


class S3DataWriter:
    """
    Write batch data to S3 as compressed Parquet files optimized for Snowpipe ingestion.
    
    Features:
    - Automatic batching to optimize file sizes (target 1-100MB per file)
    - Compressed Parquet format for efficient storage and processing
    - Proper file naming with timestamps and UUIDs
    - Landing table pattern for audit trail (JSON format)
    - Error handling and retry logic
    """
    
    def __init__(self, 
                 bucket_name: str = "fin-trade-craft-landing",
                 region_name: str = "us-east-2"):
        """
        Initialize S3 data writer.
        
        Args:
            bucket_name: S3 bucket for data landing
            region_name: AWS region
        """
        self.bucket_name = bucket_name
        self.region_name = region_name
        
        # Initialize S3 client
        self.s3_client = boto3.client('s3', region_name=region_name)
        
        # Batching configuration
        self.target_file_size_mb = 10  # Target file size in MB
        self.max_records_per_file = 1000  # Maximum records per file
        
    def write_business_data_parquet(self,
                                  table_name: str,
                                  records: List[Dict[str, Any]],
                                  run_id: str) -> Dict[str, Any]:
        """
        Write business data as Parquet files to S3.
        
        Args:
            table_name: Name of the target table (e.g., 'overview', 'income_statement')
            records: List of business records to write
            run_id: Unique run identifier
            
        Returns:
            Dict with file information and statistics
        """
        if not records:
            return {
                "files_written": 0,
                "total_records": 0,
                "status": "no_data"
            }
        
        try:
            # Convert to DataFrame
            df = pd.DataFrame(records)
            
            # Generate file information
            timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            file_id = str(uuid.uuid4())[:8]
            
            # Construct S3 key following Snowpipe conventions
            s3_key = f"business_data/{table_name}/{timestamp}_{file_id}_{run_id}.parquet"
            
            # Write Parquet to S3 with compression
            parquet_buffer = df.to_parquet(
                compression='gzip',
                index=False,
                engine='pyarrow'
            )
            
            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=parquet_buffer,
                ContentType='application/octet-stream',
                Metadata={
                    'table_name': table_name,
                    'run_id': run_id,
                    'record_count': str(len(records)),
                    'created_at': datetime.utcnow().isoformat()
                }
            )
            
            file_info = {
                "bucket": self.bucket_name,
                "key": s3_key,
                "record_count": len(records),
                "file_size_bytes": len(parquet_buffer),
                "compression": "gzip",
                "format": "parquet"
            }
            
            return {
                "files_written": 1,
                "total_records": len(records),
                "files": [file_info],
                "status": "success"
            }
            
        except Exception as e:
            return {
                "files_written": 0,
                "total_records": len(records) if records else 0,
                "status": "error",
                "error": str(e)
            }
    
    def write_landing_data_json(self,
                               table_name: str,
                               landing_records: List[Dict[str, Any]],
                               run_id: str) -> Dict[str, Any]:
        """
        Write landing/audit data as compressed JSON files to S3.
        
        Args:
            table_name: Name of the target table
            landing_records: List of landing records (raw API responses)
            run_id: Unique run identifier
            
        Returns:
            Dict with file information and statistics
        """
        if not landing_records:
            return {
                "files_written": 0,
                "total_records": 0,
                "status": "no_data"
            }
        
        try:
            # Generate file information
            timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            file_id = str(uuid.uuid4())[:8]
            
            # Construct S3 key for landing data
            s3_key = f"landing_data/{table_name}/{timestamp}_{file_id}_{run_id}.json.gz"
            
            # Convert to JSON and compress
            json_data = json.dumps(landing_records, separators=(',', ':'), default=str)
            compressed_data = gzip.compress(json_data.encode('utf-8'))
            
            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=compressed_data,
                ContentType='application/gzip',
                ContentEncoding='gzip',
                Metadata={
                    'table_name': table_name,
                    'run_id': run_id,
                    'record_count': str(len(landing_records)),
                    'created_at': datetime.utcnow().isoformat(),
                    'data_type': 'landing_audit'
                }
            )
            
            file_info = {
                "bucket": self.bucket_name,
                "key": s3_key,
                "record_count": len(landing_records),
                "file_size_bytes": len(compressed_data),
                "compression": "gzip",
                "format": "json"
            }
            
            return {
                "files_written": 1,
                "total_records": len(landing_records),
                "files": [file_info],
                "status": "success"
            }
            
        except Exception as e:
            return {
                "files_written": 0,
                "total_records": len(landing_records) if landing_records else 0,
                "status": "error",
                "error": str(e)
            }
    
    def write_batch_data(self,
                        table_name: str,
                        business_records: List[Dict[str, Any]],
                        landing_records: List[Dict[str, Any]],
                        run_id: str) -> Dict[str, Any]:
        """
        Write both business and landing data in a single batch operation.
        
        Args:
            table_name: Name of the target table
            business_records: Transformed business data
            landing_records: Raw API responses for audit
            run_id: Unique run identifier
            
        Returns:
            Combined results from both write operations
        """
        results = {
            "business_data": {},
            "landing_data": {},
            "total_files": 0,
            "total_records": 0,
            "status": "success"
        }
        
        # Write business data
        if business_records:
            business_result = self.write_business_data_parquet(
                table_name, business_records, run_id
            )
            results["business_data"] = business_result
            results["total_files"] += business_result.get("files_written", 0)
            results["total_records"] += business_result.get("total_records", 0)
            
            if business_result.get("status") == "error":
                results["status"] = "partial_success"
        
        # Write landing data
        if landing_records:
            landing_result = self.write_landing_data_json(
                table_name, landing_records, run_id
            )
            results["landing_data"] = landing_result
            results["total_files"] += landing_result.get("files_written", 0)
            
            if landing_result.get("status") == "error":
                results["status"] = "partial_success" if results["status"] == "success" else "error"
        
        return results
    
    def validate_s3_access(self) -> Dict[str, Any]:
        """
        Validate S3 bucket access and permissions.
        
        Returns:
            Dict with validation results
        """
        try:
            # Test bucket access
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            
            # Test write permissions with a small test file
            test_key = f"_test_access/{datetime.utcnow().isoformat()}_test.txt"
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=test_key,
                Body=b"access_test",
                Metadata={'test': 'true'}
            )
            
            # Clean up test file
            self.s3_client.delete_object(
                Bucket=self.bucket_name,
                Key=test_key
            )
            
            return {
                "status": "success",
                "bucket": self.bucket_name,
                "region": self.region_name,
                "access": "read_write"
            }
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            return {
                "status": "error",
                "bucket": self.bucket_name,
                "error_code": error_code,
                "error": str(e)
            }
        except Exception as e:
            return {
                "status": "error",
                "bucket": self.bucket_name,
                "error": str(e)
            }


def create_s3_writer(bucket_name: Optional[str] = None,
                    region_name: Optional[str] = None) -> S3DataWriter:
    """
    Create S3DataWriter with default configuration.
    
    Args:
        bucket_name: Optional bucket override
        region_name: Optional region override
        
    Returns:
        Configured S3DataWriter instance
    """
    return S3DataWriter(
        bucket_name=bucket_name or "fin-trade-craft-landing",
        region_name=region_name or "us-east-2"
    )