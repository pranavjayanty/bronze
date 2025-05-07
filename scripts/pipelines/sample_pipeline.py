import argparse
from bronze.extractors.sample_extractor import SampleExtractor
from bronze.utils.pipeline import run_pipeline

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Sample data pipeline')
    parser.add_argument('--input-path', required=True, help='Path to input file')
    args = parser.parse_args()
    
    # Run the pipeline with specific components
    run_pipeline(
        extractor_class=SampleExtractor,
        ddl_filename='create_bronze_table.sql',
        table_name='sample_data',
        input_path=args.input_path
    )

if __name__ == "__main__":
    main() 