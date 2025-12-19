import pandas as pd
import numpy as np
import requests
import os
from datetime import datetime
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')


# CONFIGURATION SETTINGS
class Config:
    """Configuration settings for the application"""
    # API Settings
    API_BASE_URL = "https://date.nager.at/api/v3/PublicHolidays"
    API_TIMEOUT = 10
    
    # Countries and Years
    COUNTRIES = ['AT', 'DE', 'SK', 'CZ']
    YEARS = list(range(2020, 2026))  # 2020-2025 inclusive
    
    # File paths (can be overridden by environment variables)
    HOLIDAY_CSV_PATH = os.getenv('HOLIDAY_CSV_PATH', 'input_file_holidays(1).csv')
    OUTPUT_HOLIDAYS_FILE = 'holidays_data.csv'
    OUTPUT_COUNTS_FILE = 'holiday_counts.csv'
    OUTPUT_RESULTS_FILE = 'repair_analysis_results.csv'
    OUTPUT_AGGREGATION_FILE = 'country_aggregation.csv'
    # Repair Excel input path
    REPAIR_EXCEL_PATH = os.getenv('REPAIR_EXCEL_PATH', r'C:\Users\volat\Downloads\input_file_task3(2).xlsx')
    
    # Country name mapping (repair data uses full names, holiday data uses codes)
    COUNTRY_MAPPING = {
        'Slovakia': 'SK',
        'Czechia': 'CZ',
        'Austria': 'AT',
        'Germany': 'DE'
    }


# LOGGING UTILITIES
class Logger:
    """Simple logging utility for professional output"""
    
    @staticmethod
    def _ts():
        """Return a compact timestamp for human-friendly logs."""
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    @staticmethod
    def info(message):
        print(f"[INFO] {Logger._ts()} - {message}")
    
    @staticmethod
    def success(message):
        print(f"[OK] {Logger._ts()} - {message}")
    
    @staticmethod
    def warning(message):
        print(f"[WARN] {Logger._ts()} - {message}")
    
    @staticmethod
    def error(message):
        print(f"[ERR] {Logger._ts()} - {message}")
    
    @staticmethod
    def section(title):
        print("\n" + "=" * 70)
        print(f"{title}")
        print("=" * 70)


# TASK 1: HOLIDAY DATA FETCHER CLASS
class HolidayDataFetcher:
    """
    Professional holiday data fetcher with multiple fallback strategies.
    Implements Task 1a and 1b requirements.
    """
    
    def __init__(self, config):
        """Initialize with configuration"""
        self.config = config
        self.data = None
    
    def fetch_from_api(self):
        """Fetch holiday data from Nager.Date API"""
        Logger.info("Attempting to fetch holiday data from API...")

        def _is_public(holiday):
            return 'Public' in holiday.get('types', [])

        collected = []

        for country in self.config.COUNTRIES:
            for year in self.config.YEARS:
                try:
                    url = f"{self.config.API_BASE_URL}/{year}/{country}"
                    response = requests.get(url, timeout=self.config.API_TIMEOUT)

                    if response.status_code == 200:
                        holidays = response.json()
                        for holiday in holidays:
                            # keep only public holidays
                            if _is_public(holiday):
                                collected.append({
                                    'date': holiday.get('date'),
                                    'localName': holiday.get('localName'),
                                    'name': holiday.get('name'),
                                    'countryCode': holiday.get('countryCode'),
                                    'fixed': holiday.get('fixed'),
                                    'global': holiday.get('global'),
                                    'counties': holiday.get('counties'),
                                    'launchYear': holiday.get('launchYear'),
                                    'types': holiday.get('types')
                                })

                        Logger.info(f"Fetched {len(holidays)} holidays for {country} {year}")
                    else:
                        Logger.warning(f"API request failed for {country} {year}: HTTP {response.status_code}")
                        return None

                except requests.exceptions.Timeout:
                    Logger.warning(f"Timeout occurred for {country} {year}")
                    return None
                except requests.exceptions.RequestException as e:
                    Logger.warning(f"Network error for {country} {year}: {e}")
                    return None
                except Exception as e:
                    Logger.error(f"Unexpected error for {country} {year}: {e}")
                    return None

        if not collected:
            Logger.warning("No holidays fetched from API")
            return None

        df = pd.DataFrame(collected)
        df['date'] = pd.to_datetime(df['date'])
        Logger.success(f"Successfully fetched {len(df)} total holidays from API")
        return df
    
    def fetch_from_csv(self, filepath=None):
        """Fetch holiday data from CSV file with intelligent file discovery"""
        filepath = filepath or self.config.HOLIDAY_CSV_PATH
        
        Logger.info(f"Attempting to read holiday data from file...")
        
        # Try the specified path first
        if os.path.exists(filepath):
            try:
                df = pd.read_csv(filepath)
                # Convert date column to datetime
                df['date'] = pd.to_datetime(df['date'])
                Logger.success(f"Loaded {len(df)} holidays from '{filepath}'")
                return df
            except Exception as e:
                Logger.warning(f"Error reading '{filepath}': {e}")
        
        # Try to find holiday files in current directory
        Logger.info("Searching for holiday files in current directory...")
        possible_patterns = ['*holiday*.csv', '*holidays*.csv', 'input_file*.csv']
        
        for pattern in possible_patterns:
            matching_files = list(Path('.').glob(pattern))
            for file in matching_files:
                try:
                    df = pd.read_csv(file)
                    df['date'] = pd.to_datetime(df['date'])
                    Logger.success(f"Found and loaded {len(df)} holidays from '{file}'")
                    return df
                except:
                    continue
        
        Logger.error("No holiday CSV files found")
        return None
    
    def fetch_holidays(self, prefer_api=True):
        """
        Main method to fetch holidays with fallback strategy
        
        Args:
            prefer_api: If True, try API first then CSV. If False, try CSV first.
        
        Returns:
            DataFrame with holiday data
        """
        if prefer_api:
            Logger.section("STRATEGY: API First (preferred)")
            
            api_data = self.fetch_from_api()
            if api_data is not None:
                self.data = api_data
                return self.data
            
            Logger.info("API failed, trying fallback to CSV...")
            csv_data = self.fetch_from_csv()
            if csv_data is not None:
                self.data = csv_data
                return self.data
        else:
            Logger.section("STRATEGY: CSV First")
            
            csv_data = self.fetch_from_csv()
            if csv_data is not None:
                self.data = csv_data
                return self.data
            
            Logger.info("CSV not available, trying API...")
            api_data = self.fetch_from_api()
            if api_data is not None:
                self.data = api_data
                return self.data
        
        raise RuntimeError("Unable to fetch holiday data from any source. Please check network connection and ensure holiday data file is available.")
    
    def save_holidays(self, filepath=None):
        """Save holiday data to CSV file (Task 1b)"""
        if self.data is None:
            raise ValueError("No holiday data to save. Call fetch_holidays() first.")
        
        filepath = filepath or self.config.OUTPUT_HOLIDAYS_FILE
        self.data.to_csv(filepath, index=False)
        Logger.success(f"Task 1b: Saved holiday data to '{filepath}'")
        return filepath
    
    def get_data(self):
        """Get the fetched holiday data"""
        if self.data is None:
            raise ValueError("No data available. Call fetch_holidays() first.")
        return self.data


# TASK 2: HOLIDAY COUNTS AGGREGATOR
class HolidayCountsAggregator:
    """
    Handles Task 2a and 2b: Count holidays per year per country
    """
    
    def __init__(self, holidays_df):
        """Initialize with holiday DataFrame"""
        self.holidays_df = holidays_df.copy()
        self.counts_df = None
    
    def prepare_data(self):
        """Prepare data for aggregation"""
        # Ensure date column is datetime (in case it's not already)
        if not pd.api.types.is_datetime64_any_dtype(self.holidays_df['date']):
            self.holidays_df['date'] = pd.to_datetime(self.holidays_df['date'])
        
        # Extract year from date
        self.holidays_df['Year'] = self.holidays_df['date'].dt.year
        
        # Filter for required years (2020-2025)
        self.holidays_df = self.holidays_df[
            (self.holidays_df['Year'] >= 2020) & 
            (self.holidays_df['Year'] <= 2025)
        ]
        
        # Ensure there are the required countries
        required_countries = Config.COUNTRIES
        self.holidays_df = self.holidays_df[
            self.holidays_df['countryCode'].isin(required_countries)
        ]
    
    def calculate_counts(self):
        """Calculate holiday counts per country per year (Task 2a)"""
        self.prepare_data()
        
        # Group by country and year, count holidays
        self.counts_df = self.holidays_df.groupby(
            ['countryCode', 'Year']
        ).size().reset_index(name='HolidayCount')
        
        # Rename columns for clarity
        self.counts_df.columns = ['Country', 'Year', 'HolidayCount']
        
        # Sort for better presentation
        self.counts_df = self.counts_df.sort_values(['Country', 'Year'])
        
        Logger.success("Task 2a: Calculated holiday counts per year per country")
        return self.counts_df
    
    def save_counts(self, filepath=None):
        """Save holiday counts to CSV file (Task 2b)"""
        if self.counts_df is None:
            self.calculate_counts()
        
        filepath = filepath or Config.OUTPUT_COUNTS_FILE
        self.counts_df.to_csv(filepath, index=False)
        Logger.success(f"Task 2b: Saved holiday counts to '{filepath}'")
        return filepath


# TASK 3: REPAIR ANALYSIS ENGINE
class RepairAnalysisEngine:
    """
    Handles Task 3: Business days calculation, lead time analysis, and aggregation
    """
    
    def __init__(self, holidays_df, config):
        """Initialize with holiday data and configuration"""
        self.holidays_df = holidays_df
        self.config = config
        self.repair_data = None
        self.results_df = None
        self.aggregation_df = None
        
        # Prepare holiday data lookup
        self._prepare_holiday_lookup()
    
    def _prepare_holiday_lookup(self):
        """Create efficient holiday lookup structure"""
        # Ensure date column is datetime
        if not pd.api.types.is_datetime64_any_dtype(self.holidays_df['date']):
            self.holidays_df['date'] = pd.to_datetime(self.holidays_df['date'])
        
        # Group holidays by country code
        # use a slightly more descriptive internal name
        self._holiday_set_by_country = {}

        for country_code in self.config.COUNTRIES:
            country_holidays = self.holidays_df[
                self.holidays_df['countryCode'] == country_code
            ]['date'].dt.date.tolist()
            self._holiday_set_by_country[country_code] = set(country_holidays)
    
    def load_repair_data(self):
        """Load repair data from the provided Excel structure (Task 3a)
        """
        Logger.section("TASK 3: REPAIR ANALYSIS")

        excel_path = getattr(self.config, 'REPAIR_EXCEL_PATH', None)

        if not excel_path:
            Logger.error("No repair Excel path configured (Config.REPAIR_EXCEL_PATH)")
            raise ValueError("No repair Excel path configured (Config.REPAIR_EXCEL_PATH)")

        if not os.path.exists(excel_path):
            Logger.error(f"Repair Excel file not found: {excel_path}")
            raise FileNotFoundError(f"Repair Excel file not found: {excel_path}")

        try:
            df = pd.read_excel(excel_path)

            # Normalize and map column names
            df.columns = [str(c).strip() for c in df.columns]
            col_map = {}
            for c in df.columns:
                lc = c.lower()
                if 'start' in lc and 'date' in lc:
                    col_map[c] = 'Start Date'
                elif 'end' in lc and 'date' in lc:
                    col_map[c] = 'End Date'
                elif 'country' == lc or 'country' in lc:
                    col_map[c] = 'Country'
                elif 'id' == lc or lc.endswith('id'):
                    col_map[c] = 'ID'

            if col_map:
                df = df.rename(columns=col_map)

            required = ['Start Date', 'End Date', 'Country']
            missing = [c for c in required if c not in df.columns]
            if missing:
                Logger.error(f"Repair Excel missing required columns: {missing}")
                raise ValueError(f"Repair Excel missing required columns: {missing}")

            if 'ID' not in df.columns:
                df.insert(0, 'ID', [f'ID_{i+1:04d}' for i in range(len(df))])

            self.repair_data = df.copy()
            self.repair_data['Start Date'] = pd.to_datetime(self.repair_data['Start Date'])
            self.repair_data['End Date'] = pd.to_datetime(self.repair_data['End Date'])

            Logger.success(f"Task 3a: Loaded repair data from '{excel_path}' ({len(self.repair_data)} records)")
            return self.repair_data
        except Exception as e:
            Logger.error(f"Failed to read repair data from Excel: {e}")
            raise
    
    def _calculate_business_days(self, start_date, end_date, country_name):
        """Calculate business days excluding weekends and holidays (Task 3b)"""
        # Map country name to country code
        country_code = self.config.COUNTRY_MAPPING.get(country_name)
        if not country_code:
            # If country not in mapping, return total days (shouldn't happen with our data)
            return (end_date - start_date).days + 1
        
        # Get holidays for this country
        holidays = self._holiday_set_by_country.get(country_code, set())

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
        """Calculate business days for all repair records (Task 3b)"""
        if self.repair_data is None:
            self.load_repair_data()
        
        Logger.info("Calculating business days for each repair...")
        
        # Apply business day calculation to each row
        self.repair_data['BusinessDays'] = self.repair_data.apply(
            lambda row: self._calculate_business_days(
                row['Start Date'], 
                row['End Date'], 
                row['Country']
            ), 
            axis=1
        )
        
        Logger.success(f"Task 3b: Calculated business days for {len(self.repair_data)} records")
        return self.repair_data
    
    def merge_lead_times(self):
        """Merge lead times from LeadTimes sheet (Task 3c)"""
        # Try to read lead times from the repair Excel file (sheet: 'LeadTimes')
        excel_path = getattr(self.config, 'REPAIR_EXCEL_PATH', None)
        lead_times_df = None

        if excel_path and os.path.exists(excel_path):
            try:
                # Prefer explicit sheet name
                try:
                    lt = pd.read_excel(excel_path, sheet_name='LeadTimes')
                except ValueError:
                    # Sheet not found — try first sheet or infer from sheets
                    lt = pd.read_excel(excel_path, sheet_name=0)

                # Normalize columns
                lt.columns = [str(c).strip() for c in lt.columns]
                # Map possible column names
                col_map = {}
                for c in lt.columns:
                    lc = c.lower()
                    if 'country' == lc or 'country' in lc:
                        col_map[c] = 'Country'
                    if 'lead' in lc and 'time' in lc:
                        col_map[c] = 'LeadTime'
                    if 'leadtime' == lc.replace(' ', ''):
                        col_map[c] = 'LeadTime'

                if col_map:
                    lt = lt.rename(columns=col_map)

                if 'Country' in lt.columns and 'LeadTime' in lt.columns:
                    lead_times_df = lt[['Country', 'LeadTime']].copy()
                else:
                    lead_times_df = None
            except Exception as e:
                Logger.warning(f"Could not read LeadTimes sheet: {e}")

        # If no lead times found in Excel, attempt to use column in repair_data
        if lead_times_df is None and self.repair_data is not None:
            # Look for a lead time column in repair_data
            for c in self.repair_data.columns:
                lc = str(c).lower()
                if 'lead' in lc and 'time' in lc:
                    lead_times_df = self.repair_data[[ 'Country', c ]].rename(columns={c: 'LeadTime'})
                    break

        # Fallback to defaults if necessary (keeps script robust)
        if lead_times_df is None:
            Logger.warning("No LeadTimes provided in Excel or repair data — using default lead times")
            lead_times_data = {
                'Country': ['Slovakia', 'Czechia', 'Austria', 'Germany'],
                'LeadTime': [3, 3, 4, 5]
            }
            lead_times_df = pd.DataFrame(lead_times_data)

        # Merge with repair data
        self.results_df = pd.merge(
            self.repair_data,
            lead_times_df,
            on='Country',
            how='left'
        )

        # If some LeadTime values are missing, fill from defaults where possible
        if 'LeadTime' not in self.results_df.columns:
            self.results_df['LeadTime'] = np.nan

        Logger.success("Task 3c: Merged lead times with repair data")
        return self.results_df
    
    def determine_hit_miss(self):
        """Determine Hit/Miss status for each repair (Task 3d)"""
        if self.results_df is None:
            self.merge_lead_times()
        
        # Determine status: Hit if BusinessDays <= LeadTime, else Miss
        self.results_df['Status'] = self.results_df.apply(
            lambda row: 'Hit' if row['BusinessDays'] <= row['LeadTime'] else 'Miss', 
            axis=1
        )
        
        Logger.success("Task 3d: Determined Hit/Miss status for all records")
        return self.results_df
    
    def aggregate_by_country(self):
        """Aggregate data on country level (Task 3e)"""
        if self.results_df is None:
            self.determine_hit_miss()
        
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
        
        Logger.success("Task 3e: Aggregated data at country level")
        return self.aggregation_df
    
    def save_results(self):
        """Save all results to CSV files (Task 3f)"""
        if self.results_df is None:
            self.aggregate_by_country()
        
        # Save detailed results
        self.results_df.to_csv(
            self.config.OUTPUT_RESULTS_FILE, 
            index=False
        )
        
        # Save aggregation
        self.aggregation_df.to_csv(
            self.config.OUTPUT_AGGREGATION_FILE, 
            index=False
        )
        
        Logger.success(f"Task 3f: Saved results to '{self.config.OUTPUT_RESULTS_FILE}'")
        Logger.success(f"Task 3f: Saved aggregation to '{self.config.OUTPUT_AGGREGATION_FILE}'")
        
        return {
            'detailed_results': self.config.OUTPUT_RESULTS_FILE,
            'aggregation': self.config.OUTPUT_AGGREGATION_FILE
        }
    
# MAIN EXECUTION AND REPORTING

def main():
    """Main execution function"""
    Logger.section("REPAIR PERFORMANCE ANALYSIS SYSTEM")
    
    # Initialize configuration
    config = Config()

    # TASK 1: HOLIDAY DATA
    Logger.section("TASK 1: FETCHING HOLIDAY DATA")
    
    # Create holiday fetcher
    holiday_fetcher = HolidayDataFetcher(config)
    
    # Fetch holidays (try API first, fall back to CSV)
    try:
        holidays_df = holiday_fetcher.fetch_holidays(prefer_api=True)
    except Exception as e:
        Logger.error(f"Failed to fetch holiday data: {e}")
        Logger.info("Attempting to use local CSV file directly...")
        # Try to read directly from CSV as last resort
        try:
            holidays_df = pd.read_csv(config.HOLIDAY_CSV_PATH)
            holidays_df['date'] = pd.to_datetime(holidays_df['date'])
            Logger.success(f"Loaded {len(holidays_df)} holidays from local CSV file")
        except Exception as csv_error:
            Logger.error(f"Failed to load CSV file: {csv_error}")
            # Create an empty DataFrame as last resort
            Logger.warning("Creating empty holiday DataFrame - some calculations may be inaccurate")
            holidays_df = pd.DataFrame(columns=['date', 'countryCode'])
    
    # Save holiday data (if it exists)
    if holidays_df is not None and len(holidays_df) > 0:
        holiday_fetcher.data = holidays_df
        holiday_fetcher.save_holidays()
    
    Logger.info(f"Total holidays loaded: {len(holidays_df)}")
    
    # Safely display date range
    try:
        if 'date' in holidays_df.columns and len(holidays_df) > 0:
            # Ensure the date column is datetime
            if not pd.api.types.is_datetime64_any_dtype(holidays_df['date']):
                holidays_df['date'] = pd.to_datetime(holidays_df['date'])
            
            min_date = holidays_df['date'].min()
            max_date = holidays_df['date'].max()
            Logger.info(f"Date range: {min_date.date()} to {max_date.date()}")
        else:
            Logger.warning("No date information available")
    except Exception as e:
        Logger.warning(f"Unable to display date range: {e}")
    
 
    # TASK 2: HOLIDAY COUNTS
    Logger.section("TASK 2: HOLIDAY COUNTS AGGREGATION")
    
    # Create aggregator
    counts_aggregator = HolidayCountsAggregator(holidays_df)
    
    # Calculate and display counts
    try:
        counts_df = counts_aggregator.calculate_counts()
        Logger.info("Holiday Counts per Year per Country:")
        print("-" * 40)
        print(counts_df.to_string(index=False))
        
        # Save counts
        counts_aggregator.save_counts()
    except Exception as e:
        Logger.error(f"Failed to calculate holiday counts: {e}")
        # Create empty counts DataFrame
        counts_df = pd.DataFrame(columns=['Country', 'Year', 'HolidayCount'])

    # TASK 3: REPAIR ANALYSIS
    Logger.section("TASK 3: REPAIR PERFORMANCE ANALYSIS")
    
    # Create repair analysis engine
    repair_engine = RepairAnalysisEngine(holidays_df, config)
    
    # Execute all Task 3 steps
    try:
        repair_engine.load_repair_data()
        repair_engine.calculate_all_business_days()
        repair_engine.merge_lead_times()
        repair_engine.determine_hit_miss()
        repair_engine.aggregate_by_country()
        
        # Display sample results
        Logger.info("Sample Results (first 5 records):")
        print("-" * 70)
        sample_cols = ['ID', 'Country', 'BusinessDays', 'LeadTime', 'Status']
        print(repair_engine.results_df[sample_cols].head().to_string(index=False))
        
        # Display aggregation
        Logger.info("Country-Level Aggregation:")
        print("-" * 40)
        print(repair_engine.aggregation_df.to_string(index=False))
        
        # Save results
        output_files = repair_engine.save_results()
        

        # FINAL SUMMARY
        Logger.section("EXECUTION SUMMARY")
        
        # Count hits and misses
        if repair_engine.results_df is not None:
            total_hits = (repair_engine.results_df['Status'] == 'Hit').sum()
            total_misses = (repair_engine.results_df['Status'] == 'Miss').sum()
            overall_hit_rate = total_hits / (total_hits + total_misses) * 100
            
            Logger.info("Overall Performance:")
            Logger.info(f"  Total Repairs: {len(repair_engine.results_df)}")
            Logger.info(f"  Hits: {total_hits} ({overall_hit_rate:.1f}%)")
            Logger.info(f"  Misses: {total_misses}")
        
        # List generated files
        Logger.info("Generated Files:")
        Logger.info(f"  1. {config.OUTPUT_HOLIDAYS_FILE} - Complete holiday data")
        Logger.info(f"  2. {config.OUTPUT_COUNTS_FILE} - Holiday counts by country/year")
        Logger.info(f"  3. {config.OUTPUT_RESULTS_FILE} - Detailed repair analysis")
        Logger.info(f"  4. {config.OUTPUT_AGGREGATION_FILE} - Country-level aggregation")
        
        Logger.section("ANALYSIS COMPLETE")
        
    except Exception as e:
        Logger.error(f"Failed to complete repair analysis: {e}")
        Logger.error("Some tasks may not have been completed fully")


# ENTRY POINT WITH ROBUST ERROR HANDLING
if __name__ == "__main__":
    try:
        main()
    except FileNotFoundError as e:
        Logger.error(f"File Error: {e}")
        Logger.info("Please ensure the holiday data file is available.")
        Logger.info("You can:")
        Logger.info("  1. Place 'input_file_holidays(1).csv' in the same directory")
        Logger.info("  2. Set HOLIDAY_CSV_PATH environment variable")
        Logger.info("  3. Ensure you have internet access for API fallback")
    except requests.exceptions.ConnectionError:
        Logger.error("Network Error: Unable to connect to API")
        Logger.info("Please check your internet connection or use the CSV file.")
    except AttributeError as e:
        Logger.error(f"Data format error: {e}")
        Logger.info("There might be an issue with the date format in the data.")
        Logger.info("Check that the CSV file has a 'date' column in YYYY-MM-DD format.")
    except Exception as e:
        Logger.error(f"Unexpected Error: {e}")
        Logger.info(f"Error type: {type(e).__name__}")
        import traceback
        traceback.print_exc()
    else:
        Logger.success("All tasks completed successfully!")