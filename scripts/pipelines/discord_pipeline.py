import argparse
from bronze.extractors.discord_extractor import DiscordExtractor
from bronze.utils.pipeline import run_pipeline

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Discord data pipeline')
    parser.add_argument('--input-path', required=True, help='Path to input file')
    args = parser.parse_args()
    
    # Run the pipeline with Discord-specific components
    run_pipeline(
        extractor_class=DiscordExtractor,
        ddl_filename='create_discord_table.sql',
        table_name='discord_data',
        input_path=args.input_path
    )

if __name__ == "__main__":
    main() 