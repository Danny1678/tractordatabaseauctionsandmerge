#!/usr/bin/env python3

import json
import logging
from datetime import datetime
import pandas as pd
import re
from difflib import SequenceMatcher

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    filename=f'merge_log_{datetime.now().strftime("%Y%m%d_%H%M%S")}.txt'
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

def similar(a, b):
    """Calculate string similarity ratio"""
    return SequenceMatcher(None, a, b).ratio()

def clean_model_name(model):
    """Clean and standardize model names for better matching"""
    if not model:
        return ""
    model = str(model).lower()
    
    # Extract and store year if present
    year = None
    year_match = re.match(r'^(19|20)\d{2}\s+(.+)$', model)
    if year_match:
        year = year_match.group(1)
        model = year_match.group(2)
    
    # Handle common brand variations
    brand_mappings = {
        # Case/Farmall/IH variations
        'j.i. case': 'case',
        'j.i.case': 'case',
        'ji case': 'case',
        'case ih': 'case',
        'farmall': 'case farmall',
        'international harvester': 'international',
        'international': 'international',
        'ih': 'international',
        'mccormick': 'international',
        'mccormick-deering': 'international',
        'mccormick farmall': 'case farmall',
        
        # Massey variations with specific models
        'massey-harris': 'massey harris',
        'massey harris': 'massey harris',
        'massey ferguson': 'massey ferguson',
        'massey-ferguson': 'massey ferguson',
        'mf': 'massey ferguson',
        
        # Minneapolis variations
        'minneapolis moline': 'minneapolis moline',
        'minneapolis-moline': 'minneapolis moline',
        
        # New Holland/Ford variations
        'new holland': 'new holland',
        'ford new holland': 'new holland',
        'ford': 'ford',
        'fordson': 'ford',
        
        # John Deere variations
        'john deere': 'john deere',
        'deere': 'john deere',
        'jd': 'john deere',
        
        # Allis variations
        'allis chalmers': 'allis chalmers',
        'allis-chalmers': 'allis chalmers',
        'ac': 'allis chalmers',
        
        # Other common variations
        'oliver': 'oliver',
        'white': 'white',
        'deutz': 'deutz',
        'deutz-fahr': 'deutz',
        'deutz allis': 'deutz',
        'fiat': 'fiat',
        'agco': 'agco',
        'versatile': 'versatile',
        'mahindra': 'mahindra',
        'kubota': 'kubota',
        'yanmar': 'yanmar',
        'zetor': 'zetor',
        'claas': 'claas',
        'fendt': 'fendt',
        'steyr': 'steyr',
        'branson': 'branson',
        'challenger': 'challenger',
        'kioti': 'kioti',
        'ls': 'ls tractor',
        'tym': 'tym',
        'montana': 'montana',
        'solis': 'solis'
    }
    
    # Specific model mappings for common models
    model_mappings = {
        # Massey Harris models
        'massey harris 44': 'massey harris 44-6',
        'massey harris 44 special': 'massey harris 44-6',
        'massey harris 55': 'massey harris 55k',
        'massey harris 101': 'massey harris 101 super',
        'massey harris 101 senior': 'massey harris 101 super',
        'massey harris 30': 'massey harris 30k',
        'massey harris 33': 'massey harris 33k',
        
        # Modern Kubota models
        'kubota bx23': 'kubota bx23s',
        'kubota bx25': 'kubota bx25d',
        'kubota bx2380': 'kubota bx23s',
        'kubota m7060': 'kubota m7-131',
        'kubota m8560': 'kubota m8-111',
        
        # Common Ford models
        'ford jubilee': 'ford golden jubilee',
        'ford 8n': 'ford 8n',
        'ford 9n': 'ford 9n',
        'ford 2n': 'ford 2n',
        
        # Common Farmall models
        'case farmall super m': 'case farmall m',
        'case farmall super h': 'case farmall h',
        'case farmall super c': 'case farmall c',
        'case farmall super a': 'case farmall a'
    }
    
    # Sort brand mappings by length (longest first) to avoid partial matches
    sorted_brands = sorted(brand_mappings.keys(), key=len, reverse=True)
    for old in sorted_brands:
        if old in model:
            model = model.replace(old, brand_mappings[old])
            break
    
    # Apply specific model mappings
    for old, new in model_mappings.items():
        if model.startswith(old):
            model = new
            break
    
    # Remove common prefixes/suffixes and descriptors
    remove_terms = [
        'tractor', 'mfwd', '2wd', '4wd', 'series', 'diesel', 'gas',
        'utility', 'row crop', 'standard', 'industrial', 'agricultural',
        'orchard', 'vineyard', 'high crop', 'wheatland', 'rice special',
        'special', 'deluxe', 'premium', 'classic', 'limited', 'standard',
        'workmaster', 'powermaster', 'commander', 'regular'
    ]
    for term in remove_terms:
        model = re.sub(r'\b' + term + r'\b', '', model, flags=re.IGNORECASE)
    
    # Remove parenthetical content
    model = re.sub(r'\(.*?\)', '', model)
    
    # Remove special characters and extra spaces
    model = re.sub(r'[^\w\s-]', '', model)
    model = model.strip()
    
    # Add year back if it exists and the model is from 2000 or later
    if year and int(year) >= 2000:
        model = f"{year} {model}"
    
    return model

def clean_horsepower(hp):
    """Convert horsepower to numeric value"""
    if pd.isna(hp):
        return None
    try:
        # Extract numeric value if it's a string
        if isinstance(hp, str):
            match = re.search(r'(\d+(?:\.\d+)?)', hp)
            if match:
                return float(match.group(1))
        return float(hp)
    except (ValueError, TypeError):
        return None

def find_best_match(model, specs_df, threshold=0.80):  # Lowered threshold slightly
    """Find the best matching model using fuzzy matching"""
    if not model:
        return None
    
    # First try exact match
    if model in specs_df.index:
        return model
    
    # Extract brand and model number
    parts = model.split()
    if len(parts) < 2:
        return None
    
    brand = parts[0]
    model_num = ' '.join(parts[1:])
    
    # Find models from the same brand
    brand_models = specs_df[specs_df.index.str.contains(brand, case=False, na=False)].index
    
    best_match = None
    best_ratio = 0
    
    for spec_model in brand_models:
        # Compare only the model number part
        spec_parts = spec_model.split()
        if len(spec_parts) < 2:
            continue
        spec_model_num = ' '.join(spec_parts[1:])
        
        # Calculate similarity
        ratio = similar(model_num, spec_model_num)
        if ratio > best_ratio and ratio >= threshold:
            best_ratio = ratio
            best_match = spec_model
    
    return best_match

def merge_auction_data():
    logging.info("Starting data merge process...")
    
    # Read the auction data
    try:
        with open('auction_results.json', 'r', encoding='utf-8') as f:
            auction_data = json.load(f)
        logging.info(f"Read auction_results.json: {len(auction_data)} listings")
    except Exception as e:
        logging.error(f"Error reading auction_results.json: {str(e)}")
        return
    
    # Read the tractor specifications data
    try:
        with open('tractordata_all_tractors.json', 'r', encoding='utf-8') as f:
            tractor_specs = json.load(f)
        logging.info(f"Read tractordata_all_tractors.json: {len(tractor_specs)} models")
    except Exception as e:
        logging.error(f"Error reading tractordata_all_tractors.json: {str(e)}")
        return
    
    # Convert to DataFrames
    auction_df = pd.DataFrame(auction_data)
    specs_df = pd.DataFrame(tractor_specs)
    
    # Clean and standardize model names in both datasets
    auction_df['clean_model'] = auction_df.apply(lambda x: clean_model_name(f"{x['brand']} {x['model']}"), axis=1)
    specs_df['clean_model'] = specs_df.apply(lambda x: clean_model_name(f"{x['brand']} {x['model']}"), axis=1)
    
    # Clean horsepower values
    specs_df['horsepower'] = specs_df['horsepower'].apply(clean_horsepower)
    
    # Create a mapping of clean model names to horsepower
    hp_mapping = specs_df.set_index('clean_model')['horsepower'].to_dict()
    
    # Add horsepower to auction data using exact matching first
    auction_df['horsepower'] = auction_df['clean_model'].map(hp_mapping)
    
    # Try fuzzy matching for unmatched models
    unmatched_mask = auction_df['horsepower'].isna()
    if unmatched_mask.any():
        logging.info("\nAttempting fuzzy matching for unmatched models...")
        for idx in auction_df[unmatched_mask].index:
            model = auction_df.loc[idx, 'clean_model']
            best_match = find_best_match(model, specs_df.set_index('clean_model'))
            if best_match:
                auction_df.loc[idx, 'horsepower'] = hp_mapping.get(best_match)
    
    # Analyze unmatched models
    logging.info("\nAnalyzing unmatched models:")
    
    # Get unmatched models
    unmatched_df = auction_df[auction_df['horsepower'].isna()].copy()
    
    # Count by brand
    brand_counts = unmatched_df['brand'].value_counts()
    logging.info("\nTop 10 brands with unmatched models:")
    for brand, count in brand_counts.head(10).items():
        total_brand = len(auction_df[auction_df['brand'] == brand])
        match_rate = (1 - count/total_brand) * 100
        logging.info(f"{brand}: {count} unmatched out of {total_brand} ({match_rate:.1f}% match rate)")
    
    # Sample of unmatched models for top brands
    logging.info("\nSample of unmatched models for top 5 brands:")
    for brand in brand_counts.head(5).index:
        sample_models = unmatched_df[unmatched_df['brand'] == brand]['model'].sample(min(5, len(unmatched_df[unmatched_df['brand'] == brand]))).tolist()
        logging.info(f"\n{brand} unmatched models (sample):")
        for model in sample_models:
            clean_name = clean_model_name(f"{brand} {model}")
            logging.info(f"  Original: {model}")
            logging.info(f"  Cleaned: {clean_name}")
            # Show closest matches in specs data
            similar_models = specs_df[specs_df['clean_model'].str.contains(brand.lower(), case=False, na=False)]['clean_model'].tolist()
            if similar_models:
                logging.info("  Available models in specs data:")
                for spec_model in similar_models[:3]:
                    logging.info(f"    {spec_model}")
    
    # Drop the temporary clean_model column
    auction_df = auction_df.drop('clean_model', axis=1)
    
    # Sort by sold date (newest first) and price
    auction_df['sold_date'] = pd.to_datetime(auction_df['sold_date'], errors='coerce')
    auction_df = auction_df.sort_values(['sold_date', 'price'], ascending=[False, False])
    
    # Convert timestamps back to strings before JSON serialization
    auction_df['sold_date'] = auction_df['sold_date'].dt.strftime('%Y-%m-%d')
    
    # Convert back to list of dictionaries
    merged_data = auction_df.to_dict('records')
    
    # Save merged results
    output_file = 'merged_auction_results.json'
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(merged_data, f, indent=2)
    logging.info(f"\nSaved merged results to {output_file}")
    
    # Also save as CSV for convenience
    auction_df.to_csv('merged_auction_results.csv', index=False)
    logging.info("Also saved results as CSV")
    
    # Print summary statistics
    logging.info("\nSummary:")
    logging.info(f"Total listings: {len(merged_data)}")
    logging.info(f"Date range: {auction_df['sold_date'].min()} to {auction_df['sold_date'].max()}")
    logging.info(f"Number of unique brands: {auction_df['brand'].nunique()}")
    logging.info(f"Average price: ${auction_df['price'].mean():,.2f}")
    logging.info(f"Listings with horsepower data: {auction_df['horsepower'].notna().sum()}")
    
    # Calculate average horsepower only for non-null values
    avg_hp = auction_df['horsepower'].dropna().mean()
    if pd.notna(avg_hp):
        logging.info(f"Average horsepower: {avg_hp:.1f}")
    
    # Check for missing values
    missing_values = auction_df.isnull().sum()
    logging.info("\nMissing values by column:")
    for column, count in missing_values.items():
        if count > 0:
            logging.info(f"{column}: {count} missing values")

if __name__ == "__main__":
    merge_auction_data() 