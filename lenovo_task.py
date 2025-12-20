import pandas as pd
import numpy as np
import requests
import os
from datetime import datetime
from pathlib import Path


# CONFIGURATION SETTINGS

# API Settings
API_BASE_URL = "https://date.nager.at/api/v3/PublicHolidays"
API_TIMEOUT = 10

# Countries and Years
COUNTRIES = ['AT', 'DE', 'SK', 'CZ']
YEARS = list(range(2020, 2026))  # 2020-2025 inclusive

# File paths (can be overridden by environment variables)
HOLIDAY_CSV_PATH = 'input_file_holidays.csv'

OUTPUT_HOLIDAYS_FILE = 'results/holidays_data.csv'
OUTPUT_COUNTS_FILE = 'results/holiday_counts.csv'
OUTPUT_RESULTS_FILE = 'results/repair_analysis_results.csv'
OUTPUT_AGGREGATION_FILE = 'results/country_aggregation.csv'

# Repair Excel input path
REPAIR_EXCEL_PATH = 'input_file_task3.xlsx'

# Country name mapping (repair data uses full names, holiday data uses codes)
COUNTRY_MAPPING = {
    'Slovakia': 'SK',
    'Czechia': 'CZ',
    'Austria': 'AT',
    'Germany': 'DE'
}


# TASK 1: HOLIDAY DATA FETCHER CLASS
class HolidayDataFetcher:
    
    # Holiday data fetcher. Implements Task 1a and 1b requirements.
    
    def __init__(self):
        # Initialize with configuration
        self.data = None
    
    def fetch_from_api(self):
        # Fetch holiday data from Nager.Date API
        print("Attempting to fetch holiday data from API...")

        def is_public(holiday):
            return 'Public' in holiday.get('types', [])

        collected = []

        for country in COUNTRIES:
            for year in YEARS:
                try:
                    url = f"{API_BASE_URL}/{year}/{country}"
                    response = requests.get(url, timeout=API_TIMEOUT)

                    if response.status_code == 200:
                        holidays = response.json()
                        for holiday in holidays:
                            # keep only public holidays
                            if is_public(holiday):
                                collected.append({
                                    'date': holiday.get('date'),
                                    'localName': holiday.get('localName'),
                                    'name': holiday.get('name'),
                                    'countryCode': holiday.get('countryCode'),
                                    'types': holiday.get('types')
                                })
                    else:
                        print(f"API request failed for {country} {year}: HTTP {response.status_code}")
                        return None

                except requests.exceptions.Timeout:
                    print(f"Timeout occurred for {country} {year}")
                    return None
                except requests.exceptions.RequestException as e:
                    print(f"Network error for {country} {year}: {e}")
                    return None
                except Exception as e:
                    print(f"Unexpected error for {country} {year}: {e}")
                    return None

        if not collected:
            print("No holidays fetched from API")
            return None

        df = pd.DataFrame(collected)
        df['date'] = pd.to_datetime(df['date'])
        print(f"Successfully fetched {len(df)} total holidays from API")
        return df
    
    def fetch_from_csv(self):
        # Fetch holiday data from CSV file
        
        print("Attempting to read holiday data from file...")
        
        if os.path.exists(HOLIDAY_CSV_PATH):
            try:
                df = pd.read_csv(HOLIDAY_CSV_PATH)
                # Convert date column to datetime
                df['date'] = pd.to_datetime(df['date'])
                print(f"Loaded {len(df)} holidays from '{HOLIDAY_CSV_PATH}'")
                return df
            except Exception as e:
                print(f"Error reading '{HOLIDAY_CSV_PATH}': {e}")
        
    
    def fetch_holidays(self):
      
        ### Main method to fetch holidays with fallback strategy

        print("STRATEGY: API First (preferred)")
        
        api_data = self.fetch_from_api()
        if api_data is not None:
            self.data = api_data
            return self.data
        
        print("API failed, trying fallback to CSV...")
        csv_data = self.fetch_from_csv()
        if csv_data is not None:
            self.data = csv_data
            return self.data
      
        raise RuntimeError("Unable to fetch holiday data from any source. Please check network connection and ensure holiday data file is available.")
    
    def save_holidays(self):
        # Save holiday data to CSV file (Task 1b)
        if self.data is None:
            raise ValueError("No holiday data to save. Call fetch_holidays() first.")
        
        self.data.to_csv(OUTPUT_HOLIDAYS_FILE, index=False)
        print(f"Task 1b: Saved holiday data to '{OUTPUT_HOLIDAYS_FILE}'")
        return


# TASK 2: HOLIDAY COUNTS AGGREGATOR
class HolidayCountsAggregator:
    # Handles Task 2a and 2b: Count holidays per year per country
    
    def __init__(self, holidays_df):
        # Initialize with holiday DataFrame
        self.holidays_df = holidays_df.copy()
        self.counts_df = None
    
    def calculate_counts(self):
        # Calculate holiday counts per country per year (Task 2a)
        
        self.holidays_df['Year'] = self.holidays_df['date'].dt.year
        
        # Group by country and year, count holidays
        self.counts_df = self.holidays_df.groupby(['Year', 'countryCode']).size().reset_index(name='HolidayCount')
        
        # Rename columns for clarity
        self.counts_df.columns = ['Year', 'Country', 'HolidayCount']
        
        print("Task 2a: Calculated holiday counts per year per country")
        return self.counts_df
    
    def save_counts(self):
        # Save holiday counts to CSV file (Task 2b)
        
        self.counts_df.to_csv(OUTPUT_COUNTS_FILE, index=False)
        print(f"Task 2b: Saved holiday counts to '{OUTPUT_COUNTS_FILE}'")
        return OUTPUT_COUNTS_FILE


# TASK 3: REPAIR ANALYSIS ENGINE
class RepairAnalysisEngine:
    # Handles Task 3: Business days calculation, lead time analysis, and aggregation

    def __init__(self, holidays_df):
        # Initialize with holiday data and configuration
        self.holidays_df = holidays_df.copy()
        self.repair_data = None
        self.results_df = None
        self.aggregation_df = None
        
        # Prepare holiday data lookup
        self._prepare_holiday_lookup()
    
    def _prepare_holiday_lookup(self):
        # Create efficient holiday lookup structure
        
        # Group holidays by country code
        self.holiday_set_by_country = {}

        for country_code in COUNTRIES:
            country_holidays = self.holidays_df[
                self.holidays_df['countryCode'] == country_code
            ]['date'].dt.date.tolist()
            self.holiday_set_by_country[country_code] = set(country_holidays)
    
    def load_repair_data(self):
        # Load repair data from the provided Excel structure (Task 3a)
        print("TASK 3: REPAIR ANALYSIS")

        if not REPAIR_EXCEL_PATH:
            print(f"No repair Excel path configured {REPAIR_EXCEL_PATH}")
            raise ValueError(f"No repair Excel path configured {REPAIR_EXCEL_PATH}")

        if not os.path.exists(REPAIR_EXCEL_PATH):
            print(f"Repair Excel file not found: {REPAIR_EXCEL_PATH}")
            raise FileNotFoundError(f"Repair Excel file not found: {REPAIR_EXCEL_PATH}")

        try:
            df = pd.read_excel(REPAIR_EXCEL_PATH, sheet_name='RawData')

            self.repair_data = df.copy()
            
            print(f"Task 3a: Loaded repair data from '{REPAIR_EXCEL_PATH}' ({len(self.repair_data)} records)")
            return self.repair_data
        except Exception as e:
            print(f"Failed to read repair data from Excel: {e}")
            raise
    
    def _calculate_business_days(self, start_date, end_date, country_name):
        # Calculate business days excluding weekends and holidays (Task 3b)
        # Map country name to country code
        country_code = COUNTRY_MAPPING.get(country_name)
        
        # Get holidays for this country
        holidays = self.holiday_set_by_country.get(country_code, set())

        # Generate all dates in the interval
        all_days = pd.date_range(start=start_date, end=end_date, freq='D')

        business_days = 0
        for dt in all_days:
            date_obj = dt.date()
            # Weekdays only (Mon-Fri)
            if dt.weekday() < 5:
                if date_obj not in holidays:
                    business_days += 1

        return business_days
    
    def calculate_all_business_days(self):
        # Calculate business days for all repair records (Task 3b)

        print("Calculating business days for each repair...")
        
        # Apply business day calculation to each row
        self.repair_data['BusinessDays'] = self.repair_data.apply(
            lambda row: self._calculate_business_days(
                row['Start Date'], 
                row['End Date'], 
                row['Country']
            ), 
            axis=1
        )
        
        print(f"Task 3b: Calculated business days for {len(self.repair_data)} records")
        return
    
    def merge_lead_times(self):
        #Merge lead times from LeadTimes sheet (Task 3c)
        
        # Try to read lead times from the repair Excel file (sheet: 'LeadTimes')
        lead_times_df = None

        if REPAIR_EXCEL_PATH and os.path.exists(REPAIR_EXCEL_PATH):

            try:
                lead_times_df = pd.read_excel(REPAIR_EXCEL_PATH, sheet_name='LeadTimes')
            except Exception as e:
                print(f"Could not read LeadTimes sheet: {e}")

            # Map possible column names
            col_map = {}
            for column in lead_times_df.columns:
                lower_column = column.lower()
                if 'country' in lower_column:
                    col_map[column] = 'Country'
                if 'lead' in lower_column and 'time' in lower_column:
                    col_map[column] = 'LeadTime'

            if col_map:
                lead_times_df = lead_times_df.rename(columns=col_map)


        # Merge with repair data
        self.results_df = pd.merge(
            self.repair_data,
            lead_times_df,
            on='Country',
            how='left'
        )

        print("Task 3c: Merged lead times with repair data")
        return 
    
    def determine_hit_miss(self):
        # Determine Hit/Miss status for each repair (Task 3d)
        
        # Determine status: Hit if BusinessDays <= LeadTime, else Miss
        self.results_df['Status'] = self.results_df.apply(
            lambda row: 'Hit' if row['BusinessDays'] <= row['LeadTime'] else 'Miss', 
            axis=1
        )
        
        print("Task 3d: Determined Hit/Miss status for all records")
        return 
    
    def aggregate_by_country(self):
        # Aggregate data on country level (Task 3e)
        
        # Prepare list for aggregation results
        agg_data = []
        
        for country in sorted(self.results_df['Country'].unique()):
            country_df = self.results_df[self.results_df['Country'] == country]
            
            hit_count = (country_df['Status'] == 'Hit').sum()
            miss_count = (country_df['Status'] == 'Miss').sum()
            total_count = hit_count + miss_count
            
            # Calculate hit rate with percentage formatting
            hit_rate = (hit_count / total_count * 100) if total_count > 0 else 0
            
            agg_data.append({
                'Country': country,
                'Hit count': hit_count,
                'Miss count': miss_count,
                'Hit rate (%)': round(hit_rate, 2)
            })
        
        # Create aggregation DataFrame
        self.aggregation_df = pd.DataFrame(agg_data)
        
        print("Task 3e: Aggregated data at country level")
        return self.aggregation_df
    
    def save_results(self):
        # Save all results to CSV files (Task 3f)
        
        # Save detailed results
        self.results_df.to_csv(
            OUTPUT_RESULTS_FILE, 
            index=False
        )
        
        # Save aggregation
        self.aggregation_df.to_csv(
            OUTPUT_AGGREGATION_FILE, 
            index=False
        )
        
        print(f"Task 3f: Saved results to '{OUTPUT_RESULTS_FILE}'")
        print(f"Task 3f: Saved aggregation to '{OUTPUT_AGGREGATION_FILE}'")
        
        return
    
# MAIN EXECUTION AND REPORTING

def main():
    #Main execution function

    # TASK 1: HOLIDAY DATA
    print("TASK 1: FETCHING HOLIDAY DATA")
    
    # Create holiday fetcher
    holiday_fetcher = HolidayDataFetcher()
    
    # Fetch holidays (try API first, fall back to CSV)
    try:
        holidays_df = holiday_fetcher.fetch_holidays()
        holiday_fetcher.save_holidays()
    except Exception as e:
        print(f"Failed to fetch holiday data: {e}")
        
    print(f"Total holidays loaded: {len(holidays_df)}")
 
    # TASK 2: HOLIDAY COUNTS
    print("TASK 2: HOLIDAY COUNTS AGGREGATION")
    
    # Create aggregator
    counts_aggregator = HolidayCountsAggregator(holidays_df)
    
    # Calculate and display counts
    try:
        counts_aggregator.calculate_counts()
        
        # Save counts
        counts_aggregator.save_counts()
    except Exception as e:
        print(f"Failed to calculate holiday counts: {e}")
        
    # TASK 3: REPAIR ANALYSIS
    print("TASK 3: REPAIR PERFORMANCE ANALYSIS")
    
    # Create repair analysis engine
    repair_engine = RepairAnalysisEngine(holidays_df)
    
    # Execute all Task 3 steps
    try:
        repair_engine.load_repair_data()
        repair_engine.calculate_all_business_days()
        repair_engine.merge_lead_times()
        repair_engine.determine_hit_miss()
        repair_engine.aggregate_by_country()
        
        # Save results
        repair_engine.save_results()
        
        print("\nANALYSIS COMPLETE\n")
        
    except Exception as e:
        print(f"Failed to complete repair analysis: {e}")
        print("Some tasks may not have been completed fully")
        
    # List generated files
    print("Generated Files:")
    print(f"  1. {OUTPUT_HOLIDAYS_FILE} - Complete holiday data")
    print(f"  2. {OUTPUT_COUNTS_FILE} - Holiday counts by country/year")
    print(f"  3. {OUTPUT_RESULTS_FILE} - Detailed repair analysis")
    print(f"  4. {OUTPUT_AGGREGATION_FILE} - Country-level aggregation")

main()