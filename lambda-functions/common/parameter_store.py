"""
AWS Systems Manager Parameter Store utilities for secure configuration management.
Handles encrypted parameter retrieval following least-privilege security principles.
"""

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from typing import Dict, Any, Optional


class ParameterStoreManager:
    """
    Manage AWS Systems Manager Parameter Store operations for secure config.
    
    Features:
    - Encrypted parameter retrieval
    - Automatic decryption with KMS
    - Error handling and fallback
    - Caching for performance in Lambda
    """
    
    def __init__(self, region_name: str = "us-east-2"):
        """
        Initialize Parameter Store manager.
        
        Args:
            region_name: AWS region for Parameter Store
        """
        self.region_name = region_name
        self.ssm_client = boto3.client('ssm', region_name=region_name)
        self._cache = {}  # Simple caching for Lambda performance
        
    def get_parameter(self, 
                     parameter_name: str, 
                     with_decryption: bool = True,
                     use_cache: bool = True) -> Optional[str]:
        """
        Retrieve a parameter value from Parameter Store.
        
        Args:
            parameter_name: Full parameter name (e.g., '/fin-trade-craft/alpha_vantage_key')
            with_decryption: Whether to decrypt SecureString parameters
            use_cache: Whether to use cached values (recommended for Lambda)
            
        Returns:
            Parameter value or None if not found
        """
        # Check cache first
        cache_key = f"{parameter_name}_{with_decryption}"
        if use_cache and cache_key in self._cache:
            return self._cache[cache_key]
        
        try:
            response = self.ssm_client.get_parameter(
                Name=parameter_name,
                WithDecryption=with_decryption
            )
            
            value = response['Parameter']['Value']
            
            # Cache the value
            if use_cache:
                self._cache[cache_key] = value
            
            return value
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            
            if error_code == 'ParameterNotFound':
                print(f"Parameter not found: {parameter_name}")
                return None
            elif error_code in ['AccessDenied', 'UnauthorizedOperation']:
                print(f"Access denied to parameter: {parameter_name}")
                raise Exception(f"Access denied to parameter: {parameter_name}")
            else:
                print(f"Error retrieving parameter {parameter_name}: {e}")
                raise Exception(f"Error retrieving parameter: {error_code}")
                
        except Exception as e:
            print(f"Unexpected error retrieving parameter {parameter_name}: {e}")
            raise
    
    def get_multiple_parameters(self, 
                               parameter_names: list[str],
                               with_decryption: bool = True,
                               use_cache: bool = True) -> Dict[str, str]:
        """
        Retrieve multiple parameters in a single call.
        
        Args:
            parameter_names: List of parameter names to retrieve
            with_decryption: Whether to decrypt SecureString parameters
            use_cache: Whether to use cached values
            
        Returns:
            Dict mapping parameter names to values
        """
        # Check cache for all parameters
        results = {}
        uncached_params = []
        
        if use_cache:
            for param_name in parameter_names:
                cache_key = f"{param_name}_{with_decryption}"
                if cache_key in self._cache:
                    results[param_name] = self._cache[cache_key]
                else:
                    uncached_params.append(param_name)
        else:
            uncached_params = parameter_names
        
        # Retrieve uncached parameters
        if uncached_params:
            try:
                # Parameter Store get_parameters has a limit of 10 parameters per call
                batch_size = 10
                for i in range(0, len(uncached_params), batch_size):
                    batch = uncached_params[i:i + batch_size]
                    
                    response = self.ssm_client.get_parameters(
                        Names=batch,
                        WithDecryption=with_decryption
                    )
                    
                    # Process successful parameters
                    for param in response['Parameters']:
                        param_name = param['Name']
                        value = param['Value']
                        results[param_name] = value
                        
                        # Cache the value
                        if use_cache:
                            cache_key = f"{param_name}_{with_decryption}"
                            self._cache[cache_key] = value
                    
                    # Handle invalid parameters
                    for invalid_param in response.get('InvalidParameters', []):
                        print(f"Invalid parameter: {invalid_param}")
                        results[invalid_param] = None
                        
            except ClientError as e:
                print(f"Error retrieving multiple parameters: {e}")
                raise
        
        return results
    
    def get_alpha_vantage_key(self) -> str:
        """
        Retrieve the Alpha Vantage API key from Parameter Store.
        
        Returns:
            Alpha Vantage API key
            
        Raises:
            Exception if key is not found or access denied
        """
        parameter_name = "/fin-trade-craft/alpha_vantage_key"
        
        api_key = self.get_parameter(parameter_name, with_decryption=True)
        
        if not api_key:
            raise Exception(f"Alpha Vantage API key not found at {parameter_name}")
        
        return api_key
    
    def validate_parameter_access(self, parameter_name: str) -> Dict[str, Any]:
        """
        Validate access to a specific parameter without retrieving the value.
        
        Args:
            parameter_name: Parameter name to validate
            
        Returns:
            Dict with validation results
        """
        try:
            # Try to get parameter metadata (doesn't return the value)
            response = self.ssm_client.describe_parameters(
                Filters=[
                    {
                        'Key': 'Name',
                        'Values': [parameter_name]
                    }
                ]
            )
            
            if response['Parameters']:
                param_info = response['Parameters'][0]
                return {
                    "status": "accessible",
                    "parameter_name": parameter_name,
                    "type": param_info.get('Type'),
                    "description": param_info.get('Description', 'No description'),
                    "last_modified": param_info.get('LastModifiedDate')
                }
            else:
                return {
                    "status": "not_found",
                    "parameter_name": parameter_name,
                    "error": "Parameter does not exist"
                }
                
        except ClientError as e:
            error_code = e.response['Error']['Code']
            return {
                "status": "error",
                "parameter_name": parameter_name,
                "error_code": error_code,
                "error": str(e)
            }
        except Exception as e:
            return {
                "status": "error",
                "parameter_name": parameter_name,
                "error": str(e)
            }
    
    def clear_cache(self) -> None:
        """Clear the parameter cache."""
        self._cache.clear()


def get_alpha_vantage_key(region_name: str = "us-east-2") -> str:
    """
    Convenience function to get Alpha Vantage API key from Parameter Store.
    
    Args:
        region_name: AWS region
        
    Returns:
        Alpha Vantage API key
        
    Raises:
        Exception if key is not found or access denied
    """
    param_store = ParameterStoreManager(region_name)
    return param_store.get_alpha_vantage_key()


def validate_ssm_access(region_name: str = "us-east-2") -> Dict[str, Any]:
    """
    Validate access to the Alpha Vantage API key parameter.
    
    Args:
        region_name: AWS region
        
    Returns:
        Dict with validation results
    """
    param_store = ParameterStoreManager(region_name)
    return param_store.validate_parameter_access("/fin-trade-craft/alpha_vantage_key")