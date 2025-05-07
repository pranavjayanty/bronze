import pandas as pd
from typing import Optional, List, Dict, Any
from datetime import datetime

class SampleExtractor:
    """Sample extractor class that demonstrates data extraction patterns."""
    
    def __init__(self):
        self.logger = None  # Will be set by pipeline
    
    def fetch_data(self) -> List[Dict[str, Any]]:
        """
        Fetch data from external source and return as a list of dictionaries.
        This is a sample implementation - replace with actual data fetching logic.
        
        Returns:
            List[Dict[str, Any]]: Raw data from external source
        """
        # Sample data - replace with actual API calls or data fetching
        return [
            {
                "id": 1,
                "name": "Sample Item 1",
                "value": 100,
                "timestamp": datetime.now().isoformat()
            },
            {
                "id": 2,
                "name": "Sample Item 2",
                "value": 200,
                "timestamp": datetime.now().isoformat()
            }
        ]
    
    def transform_data(self, raw_data: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Transform raw data into a pandas DataFrame.
        
        Args:
            raw_data: List of dictionaries containing raw data
            
        Returns:
            pd.DataFrame: Transformed data
        """
        # Convert to DataFrame
        df = pd.DataFrame(raw_data)
        
        # Add metadata columns
        df['ingestion_timestamp'] = pd.Timestamp.now()
        df['source'] = 'sample_extractor'
        
        return df
    
    def parse(self, input_path: Optional[str] = None) -> pd.DataFrame:
        """
        Main method to fetch and transform data.
        The input_path parameter is kept for compatibility but not used.
        
        Args:
            input_path: Optional path parameter (not used in this implementation)
            
        Returns:
            pd.DataFrame: Processed data ready for database insertion
        """
        try:
            # Fetch data from external source
            raw_data = self.fetch_data()
            
            # Transform data into DataFrame
            df = self.transform_data(raw_data)
            
            return df
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"Error processing data: {str(e)}")
            raise
