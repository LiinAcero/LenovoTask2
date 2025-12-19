import pandas as pd
import numpy as np
import requests
from datetime import datetime, timedelta
import json
import warnings
warnings.filterwarnings('ignore')

# ============================================================================
# TASK 1a: Pull list of all public holidays for countries ['AT','DE','SK','CZ'] 
# for years 2020 to 2025 using the WEB API
# ============================================================================

def fetch_holidays_from_api():
    """Fetch holidays from the Nager.Date API"""
    countries = ['AT', 'DE', 'SK', 'CZ']
    years = list(range(2020, 2026))  # 2020 to 2025 inclusive
    all_holidays = []
    
    print("Fetching holiday data from API...")
    
    for country in countries:
        for year in years:
            try:
                url = f"https://date.nager.at/api/v3/PublicHolidays/{year}/{country}"
                response = requests.get(url, timeout=10)
                
                if response.status_code == 200:
                    holidays = response.json()
                    for holiday in holidays:
                        # Filter for public holidays only
                        if 'Public' in holiday.get('types', []):
                            all_holidays.append({
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
                    print(f"✓ Fetched {len(holidays)} holidays for {country} {year}")
                else:
                    print(f"✗ Failed to fetch data for {country} {year}. Status: {response.status_code}")
                    # If API fails, use the provided CSV file
                    return None
                    
            except Exception as e:
                print(f"✗ Error fetching data for {country} {year}: {e}")
                return None
    
    return pd.DataFrame(all_holidays)

# Try to fetch from API first
holidays_df = fetch_holidays_from_api()

# If API fails, use the provided CSV file
if holidays_df is None or holidays_df.empty:
    print("\nAPI fetch failed or returned empty. Using provided CSV file...")
    
    # Create a DataFrame from the provided CSV content
    csv_data = """date,localName,name,countryCode,fixed,global,counties,launchYear,types
2020-01-01,Neujahr,New Year's Day,AT,False,True,,,['Public']
2020-01-06,Heilige Drei Könige,Epiphany,AT,False,True,,,['Public']
2020-04-12,Ostersonntag,Easter Sunday,AT,False,True,,,['Public']
2020-04-13,Ostermontag,Easter Monday,AT,False,True,,,['Public']
2020-05-01,Staatsfeiertag,National Holiday,AT,False,True,,,['Public']
2020-05-21,Christi Himmelfahrt,Ascension Day,AT,False,True,,,['Public']
2020-05-31,Pfingstsonntag,Pentecost,AT,False,True,,,['Public']
2020-06-01,Pfingstmontag,Whit Monday,AT,False,True,,,['Public']
2020-06-11,Fronleichnam,Corpus Christi,AT,False,True,,,['Public']
2020-08-15,Maria Himmelfahrt,Assumption Day,AT,False,True,,,['Public']
2020-10-26,Nationalfeiertag,National Holiday,AT,False,True,,,['Public']
2020-11-01,Allerheiligen,All Saints' Day,AT,False,True,,,['Public']
2020-12-08,Mariä Empfängnis,Immaculate Conception,AT,False,True,,,['Public']
2020-12-25,Weihnachten,Christmas Day,AT,False,True,,,['Public']
2020-12-26,Stefanitag,St. Stephen's Day,AT,False,True,,,['Public']
2021-01-01,Neujahr,New Year's Day,AT,False,True,,,['Public']
2021-01-06,Heilige Drei Könige,Epiphany,AT,False,True,,,['Public']
2021-04-04,Ostersonntag,Easter Sunday,AT,False,True,,,['Public']
2021-04-05,Ostermontag,Easter Monday,AT,False,True,,,['Public']
2021-05-01,Staatsfeiertag,National Holiday,AT,False,True,,,['Public']
2021-05-13,Christi Himmelfahrt,Ascension Day,AT,False,True,,,['Public']
2021-05-23,Pfingstsonntag,Pentecost,AT,False,True,,,['Public']
2021-05-24,Pfingstmontag,Whit Monday,AT,False,True,,,['Public']
2021-06-03,Fronleichnam,Corpus Christi,AT,False,True,,,['Public']
2021-08-15,Maria Himmelfahrt,Assumption Day,AT,False,True,,,['Public']
2021-10-26,Nationalfeiertag,National Holiday,AT,False,True,,,['Public']
2021-11-01,Allerheiligen,All Saints' Day,AT,False,True,,,['Public']
2021-12-08,Mariä Empfängnis,Immaculate Conception,AT,False,True,,,['Public']
2021-12-25,Weihnachten,Christmas Day,AT,False,True,,,['Public']
2021-12-26,Stefanitag,St. Stephen's Day,AT,False,True,,,['Public']
2022-01-01,Neujahr,New Year's Day,AT,False,True,,,['Public']
2022-01-06,Heilige Drei Könige,Epiphany,AT,False,True,,,['Public']
2022-04-17,Ostersonntag,Easter Sunday,AT,False,True,,,['Public']
2022-04-18,Ostermontag,Easter Monday,AT,False,True,,,['Public']
2022-05-01,Staatsfeiertag,National Holiday,AT,False,True,,,['Public']
2022-05-26,Christi Himmelfahrt,Ascension Day,AT,False,True,,,['Public']
2022-06-05,Pfingstsonntag,Pentecost,AT,False,True,,,['Public']
2022-06-06,Pfingstmontag,Whit Monday,AT,False,True,,,['Public']
2022-06-16,Fronleichnam,Corpus Christi,AT,False,True,,,['Public']
2022-08-15,Maria Himmelfahrt,Assumption Day,AT,False,True,,,['Public']
2022-10-26,Nationalfeiertag,National Holiday,AT,False,True,,,['Public']
2022-11-01,Allerheiligen,All Saints' Day,AT,False,True,,,['Public']
2022-12-08,Mariä Empfängnis,Immaculate Conception,AT,False,True,,,['Public']
2022-12-25,Weihnachten,Christmas Day,AT,False,True,,,['Public']
2022-12-26,Stefanitag,St. Stephen's Day,AT,False,True,,,['Public']
2023-01-01,Neujahr,New Year's Day,AT,False,True,,,['Public']
2023-01-06,Heilige Drei Könige,Epiphany,AT,False,True,,,['Public']
2023-04-09,Ostersonntag,Easter Sunday,AT,False,True,,,['Public']
2023-04-10,Ostermontag,Easter Monday,AT,False,True,,,['Public']
2023-05-01,Staatsfeiertag,National Holiday,AT,False,True,,,['Public']
2023-05-18,Christi Himmelfahrt,Ascension Day,AT,False,True,,,['Public']
2023-05-28,Pfingstsonntag,Pentecost,AT,False,True,,,['Public']
2023-05-29,Pfingstmontag,Whit Monday,AT,False,True,,,['Public']
2023-06-08,Fronleichnam,Corpus Christi,AT,False,True,,,['Public']
2023-08-15,Maria Himmelfahrt,Assumption Day,AT,False,True,,,['Public']
2023-10-26,Nationalfeiertag,National Holiday,AT,False,True,,,['Public']
2023-11-01,Allerheiligen,All Saints' Day,AT,False,True,,,['Public']
2023-12-08,Mariä Empfängnis,Immaculate Conception,AT,False,True,,,['Public']
2023-12-25,Weihnachten,Christmas Day,AT,False,True,,,['Public']
2023-12-26,Stefanitag,St. Stephen's Day,AT,False,True,,,['Public']
2024-01-01,Neujahr,New Year's Day,AT,False,True,,,['Public']
2024-01-06,Heilige Drei Könige,Epiphany,AT,False,True,,,['Public']
2024-03-31,Ostersonntag,Easter Sunday,AT,False,True,,,['Public']
2024-04-01,Ostermontag,Easter Monday,AT,False,True,,,['Public']
2024-05-01,Staatsfeiertag,National Holiday,AT,False,True,,,['Public']
2024-05-09,Christi Himmelfahrt,Ascension Day,AT,False,True,,,['Public']
2024-05-19,Pfingstsonntag,Pentecost,AT,False,True,,,['Public']
2024-05-20,Pfingstmontag,Whit Monday,AT,False,True,,,['Public']
2024-05-30,Fronleichnam,Corpus Christi,AT,False,True,,,['Public']
2024-08-15,Maria Himmelfahrt,Assumption Day,AT,False,True,,,['Public']
2024-10-26,Nationalfeiertag,National Holiday,AT,False,True,,,['Public']
2024-11-01,Allerheiligen,All Saints' Day,AT,False,True,,,['Public']
2024-12-08,Mariä Empfängnis,Immaculate Conception,AT,False,True,,,['Public']
2024-12-25,Weihnachten,Christmas Day,AT,False,True,,,['Public']
2024-12-26,Stefanitag,St. Stephen's Day,AT,False,True,,,['Public']
2025-01-01,Neujahr,New Year's Day,AT,False,True,,,['Public']
2025-01-06,Heilige Drei Könige,Epiphany,AT,False,True,,,['Public']
2025-04-20,Ostersonntag,Easter Sunday,AT,False,True,,,['Public']
2025-04-21,Ostermontag,Easter Monday,AT,False,True,,,['Public']
2025-05-01,Staatsfeiertag,National Holiday,AT,False,True,,,['Public']
2025-05-29,Christi Himmelfahrt,Ascension Day,AT,False,True,,,['Public']
2025-06-08,Pfingstsonntag,Pentecost,AT,False,True,,,['Public']
2025-06-09,Pfingstmontag,Whit Monday,AT,False,True,,,['Public']
2025-06-19,Fronleichnam,Corpus Christi,AT,False,True,,,['Public']
2025-08-15,Maria Himmelfahrt,Assumption Day,AT,False,True,,,['Public']
2025-10-26,Nationalfeiertag,National Holiday,AT,False,True,,,['Public']
2025-11-01,Allerheiligen,All Saints' Day,AT,False,True,,,['Public']
2025-12-08,Mariä Empfängnis,Immaculate Conception,AT,False,True,,,['Public']
2025-12-25,Weihnachten,Christmas Day,AT,False,True,,,['Public']
2025-12-26,Stefanitag,St. Stephen's Day,AT,False,True,,,['Public']
2020-01-01,Neujahr,New Year's Day,DE,False,True,,,['Public']
2020-01-06,Heilige Drei Könige,Epiphany,DE,False,False,"['DE-BW', 'DE-BY', 'DE-ST']",,['Public']
2020-03-08,Internationaler Frauentag,International Women's Day,DE,False,False,['DE-BE'],,['Public']
2020-04-10,Karfreitag,Good Friday,DE,False,True,,,['Public']
2020-04-12,Ostersonntag,Easter Sunday,DE,False,False,['DE-BB'],,['Public']
2020-04-13,Ostermontag,Easter Monday,DE,False,True,,,['Public']
2020-05-01,Tag der Arbeit,Labour Day,DE,False,True,,,['Public']
2020-05-08,Tag der Befreiung,Liberation Day,DE,False,False,['DE-BE'],,['Public']
2020-05-21,Christi Himmelfahrt,Ascension Day,DE,False,True,,,['Public']
2020-05-31,Pfingstsonntag,Pentecost,DE,False,False,['DE-BB'],,['Public']
2020-06-01,Pfingstmontag,Whit Monday,DE,False,True,,,['Public']
2020-06-11,Fronleichnam,Corpus Christi,DE,False,False,"['DE-BW', 'DE-BY', 'DE-HE', 'DE-NW', 'DE-RP', 'DE-SL']",,['Public']
2020-08-15,Mariä Himmelfahrt,Assumption Day,DE,False,False,['DE-SL'],,['Public']
2020-09-20,Weltkindertag,World Children's Day,DE,False,False,['DE-TH'],,['Public']
2020-10-03,Tag der Deutschen Einheit,German Unity Day,DE,False,True,,,['Public']
2020-10-31,Reformationstag,Reformation Day,DE,False,False,"['DE-BB', 'DE-MV', 'DE-SN', 'DE-ST', 'DE-TH', 'DE-HB', 'DE-HH', 'DE-NI', 'DE-SH']",,['Public']
2020-11-01,Allerheiligen,All Saints' Day,DE,False,False,"['DE-BW', 'DE-BY', 'DE-NW', 'DE-RP', 'DE-SL']",,['Public']
2020-11-18,Buß- und Bettag,Repentance and Prayer Day,DE,False,False,['DE-SN'],,['Public']
2020-12-25,Erster Weihnachtstag,Christmas Day,DE,False,True,,,['Public']
2020-12-26,Zweiter Weihnachtstag,St. Stephen's Day,DE,False,True,,,['Public']
2021-01-01,Neujahr,New Year's Day,DE,False,True,,,['Public']
2021-01-06,Heilige Drei Könige,Epiphany,DE,False,False,"['DE-BW', 'DE-BY', 'DE-ST']",,['Public']
2021-03-08,Internationaler Frauentag,International Women's Day,DE,False,False,['DE-BE'],,['Public']
2021-04-02,Karfreitag,Good Friday,DE,False,True,,,['Public']
2021-04-04,Ostersonntag,Easter Sunday,DE,False,False,['DE-BB'],,['Public']
2021-04-05,Ostermontag,Easter Monday,DE,False,True,,,['Public']
2021-05-01,Tag der Arbeit,Labour Day,DE,False,True,,,['Public']
2021-05-13,Christi Himmelfahrt,Ascension Day,DE,False,True,,,['Public']
2021-05-23,Pfingstsonntag,Pentecost,DE,False,False,['DE-BB'],,['Public']
2021-05-24,Pfingstmontag,Whit Monday,DE,False,True,,,['Public']
2021-06-03,Fronleichnam,Corpus Christi,DE,False,False,"['DE-BW', 'DE-BY', 'DE-HE', 'DE-NW', 'DE-RP', 'DE-SL']",,['Public']
2021-08-15,Mariä Himmelfahrt,Assumption Day,DE,False,False,['DE-SL'],,['Public']
2021-09-20,Weltkindertag,World Children's Day,DE,False,False,['DE-TH'],,['Public']
2021-10-03,Tag der Deutschen Einheit,German Unity Day,DE,False,True,,,['Public']
2021-10-31,Reformationstag,Reformation Day,DE,False,False,"['DE-BB', 'DE-MV', 'DE-SN', 'DE-ST', 'DE-TH', 'DE-HB', 'DE-HH', 'DE-NI', 'DE-SH']",,['Public']
2021-11-01,Allerheiligen,All Saints' Day,DE,False,False,"['DE-BW', 'DE-BY', 'DE-NW', 'DE-RP', 'DE-SL']",,['Public']
2021-11-17,Buß- und Bettag,Repentance and Prayer Day,DE,False,False,['DE-SN'],,['Public']
2021-12-25,Erster Weihnachtstag,Christmas Day,DE,False,True,,,['Public']
2021-12-26,Zweiter Weihnachtstag,St. Stephen's Day,DE,False,True,,,['Public']
2022-01-01,Neujahr,New Year's Day,DE,False,True,,,['Public']
2022-01-06,Heilige Drei Könige,Epiphany,DE,False,False,"['DE-BW', 'DE-BY', 'DE-ST']",,['Public']
2022-03-08,Internationaler Frauentag,International Women's Day,DE,False,False,['DE-BE'],,['Public']
2022-04-15,Karfreitag,Good Friday,DE,False,True,,,['Public']
2022-04-17,Ostersonntag,Easter Sunday,DE,False,False,['DE-BB'],,['Public']
2022-04-18,Ostermontag,Easter Monday,DE,False,True,,,['Public']
2022-05-01,Tag der Arbeit,Labour Day,DE,False,True,,,['Public']
2022-05-26,Christi Himmelfahrt,Ascension Day,DE,False,True,,,['Public']
2022-06-05,Pfingstsonntag,Pentecost,DE,False,False,['DE-BB'],,['Public']
2022-06-06,Pfingstmontag,Whit Monday,DE,False,True,,,['Public']
2022-06-16,Fronleichnam,Corpus Christi,DE,False,False,"['DE-BW', 'DE-BY', 'DE-HE', 'DE-NW', 'DE-RP', 'DE-SL']",,['Public']
2022-08-15,Mariä Himmelfahrt,Assumption Day,DE,False,False,['DE-SL'],,['Public']
2022-09-20,Weltkindertag,World Children's Day,DE,False,False,['DE-TH'],,['Public']
2022-10-03,Tag der Deutschen Einheit,German Unity Day,DE,False,True,,,['Public']
2022-10-31,Reformationstag,Reformation Day,DE,False,False,"['DE-BB', 'DE-MV', 'DE-SN', 'DE-ST', 'DE-TH', 'DE-HB', 'DE-HH', 'DE-NI', 'DE-SH']",,['Public']
2022-11-01,Allerheiligen,All Saints' Day,DE,False,False,"['DE-BW', 'DE-BY', 'DE-NW', 'DE-RP', 'DE-SL']",,['Public']
2022-11-16,Buß- und Bettag,Repentance and Prayer Day,DE,False,False,['DE-SN'],,['Public']
2022-12-25,Erster Weihnachtstag,Christmas Day,DE,False,True,,,['Public']
2022-12-26,Zweiter Weihnachtstag,St. Stephen's Day,DE,False,True,,,['Public']
2023-01-01,Neujahr,New Year's Day,DE,False,True,,,['Public']
2023-01-06,Heilige Drei Könige,Epiphany,DE,False,False,"['DE-BW', 'DE-BY', 'DE-ST']",,['Public']
2023-03-08,Internationaler Frauentag,International Women's Day,DE,False,False,"['DE-BE', 'DE-MV']",,['Public']
2023-04-07,Karfreitag,Good Friday,DE,False,True,,,['Public']
2023-04-09,Ostersonntag,Easter Sunday,DE,False,False,['DE-BB'],,['Public']
2023-04-10,Ostermontag,Easter Monday,DE,False,True,,,['Public']
2023-05-01,Tag der Arbeit,Labour Day,DE,False,True,,,['Public']
2023-05-18,Christi Himmelfahrt,Ascension Day,DE,False,True,,,['Public']
2023-05-28,Pfingstsonntag,Pentecost,DE,False,False,['DE-BB'],,['Public']
2023-05-29,Pfingstmontag,Whit Monday,DE,False,True,,,['Public']
2023-06-08,Fronleichnam,Corpus Christi,DE,False,False,"['DE-BW', 'DE-BY', 'DE-HE', 'DE-NW', 'DE-RP', 'DE-SL']",,['Public']
2023-08-15,Mariä Himmelfahrt,Assumption Day,DE,False,False,['DE-SL'],,['Public']
2023-09-20,Weltkindertag,World Children's Day,DE,False,False,['DE-TH'],,['Public']
2023-10-03,Tag der Deutschen Einheit,German Unity Day,DE,False,True,,,['Public']
2023-10-31,Reformationstag,Reformation Day,DE,False,False,"['DE-BB', 'DE-MV', 'DE-SN', 'DE-ST', 'DE-TH', 'DE-HB', 'DE-HH', 'DE-NI', 'DE-SH']",,['Public']
2023-11-01,Allerheiligen,All Saints' Day,DE,False,False,"['DE-BW', 'DE-BY', 'DE-NW', 'DE-RP', 'DE-SL']",,['Public']
2023-11-22,Buß- und Bettag,Repentance and Prayer Day,DE,False,False,['DE-SN'],,['Public']
2023-12-25,Erster Weihnachtstag,Christmas Day,DE,False,True,,,['Public']
2023-12-26,Zweiter Weihnachtstag,St. Stephen's Day,DE,False,True,,,['Public']
2024-01-01,Neujahr,New Year's Day,DE,False,True,,,['Public']
2024-01-06,Heilige Drei Könige,Epiphany,DE,False,False,"['DE-BW', 'DE-BY', 'DE-ST']",,['Public']
2024-03-08,Internationaler Frauentag,International Women's Day,DE,False,False,"['DE-BE', 'DE-MV']",,['Public']
2024-03-29,Karfreitag,Good Friday,DE,False,True,,,['Public']
2024-03-31,Ostersonntag,Easter Sunday,DE,False,False,['DE-BB'],,['Public']
2024-04-01,Ostermontag,Easter Monday,DE,False,True,,,['Public']
2024-05-01,Tag der Arbeit,Labour Day,DE,False,True,,,['Public']
2024-05-09,Christi Himmelfahrt,Ascension Day,DE,False,True,,,['Public']
2024-05-19,Pfingstsonntag,Pentecost,DE,False,False,['DE-BB'],,['Public']
2024-05-20,Pfingstmontag,Whit Monday,DE,False,True,,,['Public']
2024-05-30,Fronleichnam,Corpus Christi,DE,False,False,"['DE-BW', 'DE-BY', 'DE-HE', 'DE-NW', 'DE-RP', 'DE-SL']",,['Public']
2024-08-15,Mariä Himmelfahrt,Assumption Day,DE,False,False,['DE-SL'],,['Public']
2024-09-20,Weltkindertag,World Children's Day,DE,False,False,['DE-TH'],,['Public']
2024-10-03,Tag der Deutschen Einheit,German Unity Day,DE,False,True,,,['Public']
2024-10-31,Reformationstag,Reformation Day,DE,False,False,"['DE-BB', 'DE-MV', 'DE-SN', 'DE-ST', 'DE-TH', 'DE-HB', 'DE-HH', 'DE-NI', 'DE-SH']",,['Public']
2024-11-01,Allerheiligen,All Saints' Day,DE,False,False,"['DE-BW', 'DE-BY', 'DE-NW', 'DE-RP', 'DE-SL']",,['Public']
2024-11-20,Buß- und Bettag,Repentance and Prayer Day,DE,False,False,['DE-SN'],,['Public']
2024-12-25,Erster Weihnachtstag,Christmas Day,DE,False,True,,,['Public']
2024-12-26,Zweiter Weihnachtstag,St. Stephen's Day,DE,False,True,,,['Public']
2025-01-01,Neujahr,New Year's Day,DE,False,True,,,['Public']
2025-01-06,Heilige Drei Könige,Epiphany,DE,False,False,"['DE-BW', 'DE-BY', 'DE-ST']",,['Public']
2025-03-08,Internationaler Frauentag,International Women's Day,DE,False,False,"['DE-BE', 'DE-MV']",,['Public']
2025-04-18,Karfreitag,Good Friday,DE,False,True,,,['Public']
2025-04-20,Ostersonntag,Easter Sunday,DE,False,False,['DE-BB'],,['Public']
2025-04-21,Ostermontag,Easter Monday,DE,False,True,,,['Public']
2025-05-01,Tag der Arbeit,Labour Day,DE,False,True,,,['Public']
2025-05-08,Tag der Befreiung,Liberation Day,DE,False,False,['DE-BE'],,['Public']
2025-05-29,Christi Himmelfahrt,Ascension Day,DE,False,True,,,['Public']
2025-06-08,Pfingstsonntag,Pentecost,DE,False,False,['DE-BB'],,['Public']
2025-06-09,Pfingstmontag,Whit Monday,DE,False,True,,,['Public']
2025-06-19,Fronleichnam,Corpus Christi,DE,False,False,"['DE-BW', 'DE-BY', 'DE-HE', 'DE-NW', 'DE-RP', 'DE-SL']",,['Public']
2025-08-15,Mariä Himmelfahrt,Assumption Day,DE,False,False,['DE-SL'],,['Public']
2025-09-20,Weltkindertag,World Children's Day,DE,False,False,['DE-TH'],,['Public']
2025-10-03,Tag der Deutschen Einheit,German Unity Day,DE,False,True,,,['Public']
2025-10-31,Reformationstag,Reformation Day,DE,False,False,"['DE-BB', 'DE-MV', 'DE-SN', 'DE-ST', 'DE-TH', 'DE-HB', 'DE-HH', 'DE-NI', 'DE-SH']",,['Public']
2025-11-01,Allerheiligen,All Saints' Day,DE,False,False,"['DE-BW', 'DE-BY', 'DE-NW', 'DE-RP', 'DE-SL']",,['Public']
2025-11-19,Buß- und Bettag,Repentance and Prayer Day,DE,False,False,['DE-SN'],,['Public']
2025-12-25,Erster Weihnachtstag,Christmas Day,DE,False,True,,,['Public']
2025-12-26,Zweiter Weihnachtstag,St. Stephen's Day,DE,False,True,,,['Public']
2020-01-01,Deň vzniku Slovenskej republiky,Day of the Establishment of the Slovak Republic,SK,False,True,,,['Public']
2020-01-06,Zjavenie Pána,Epiphany,SK,False,True,,,['Public']
2020-04-10,Veľkonočný piatok,Good Friday,SK,False,True,,,['Public']
2020-04-13,Veľkonočný pondelok,Easter Monday,SK,False,True,,,['Public']
2020-05-01,Sviatok práce,International Workers' Day,SK,False,True,,,['Public']
2020-05-08,Deň víťazstva nad fašizmom,Day of victory over fascism,SK,False,True,,,['Public']
2020-07-05,Sviatok svätého Cyrila a svätého Metoda,St. Cyril and Methodius Day,SK,False,True,,,['Public']
2020-08-29,Výročie Slovenského národného povstania,Slovak National Uprising anniversary,SK,False,True,,,['Public']
2020-09-01,Deň Ústavy Slovenskej republiky,Day of the Constitution of the Slovak Republic,SK,False,True,,,['Public']
2020-09-15,Sedembolestná Panna Mária,Day of Our Lady of the Seven Sorrows,SK,False,True,,,['Public']
2020-11-01,Sviatok Všetkých svätých,All Saints’ Day,SK,False,True,,,['Public']
2020-11-17,Deň boja za slobodu a demokraciu,Struggle for Freedom and Democracy Day,SK,False,True,,,['Public']
2020-12-24,Štedrý deň,Christmas Eve,SK,False,True,,,['Public']
2020-12-25,Prvý sviatok vianočný,Christmas Day,SK,False,True,,,['Public']
2020-12-26,Druhý sviatok vianočný,St. Stephen's Day,SK,False,True,,,['Public']
2021-01-01,Deň vzniku Slovenskej republiky,Day of the Establishment of the Slovak Republic,SK,False,True,,,['Public']
2021-01-06,Zjavenie Pána,Epiphany,SK,False,True,,,['Public']
2021-04-02,Veľkonočný piatok,Good Friday,SK,False,True,,,['Public']
2021-04-05,Veľkonočný pondelok,Easter Monday,SK,False,True,,,['Public']
2021-05-01,Sviatok práce,International Workers' Day,SK,False,True,,,['Public']
2021-05-08,Deň víťazstva nad fašizmom,Day of victory over fascism,SK,False,True,,,['Public']
2021-07-05,Sviatok svätého Cyrila a svätého Metoda,St. Cyril and Methodius Day,SK,False,True,,,['Public']
2021-08-29,Výročie Slovenského národného povstania,Slovak National Uprising anniversary,SK,False,True,,,['Public']
2021-09-01,Deň Ústavy Slovenskej republiky,Day of the Constitution of the Slovak Republic,SK,False,True,,,['Public']
2021-09-15,Sedembolestná Panna Mária,Day of Our Lady of the Seven Sorrows,SK,False,True,,,['Public']
2021-11-01,Sviatok Všetkých svätých,All Saints’ Day,SK,False,True,,,['Public']
2021-11-17,Deň boja za slobodu a demokraciu,Struggle for Freedom and Democracy Day,SK,False,True,,,['Public']
2021-12-24,Štedrý deň,Christmas Eve,SK,False,True,,,['Public']
2021-12-25,Prvý sviatok vianočný,Christmas Day,SK,False,True,,,['Public']
2021-12-26,Druhý sviatok vianočný,St. Stephen's Day,SK,False,True,,,['Public']
2022-01-01,Deň vzniku Slovenskej republiky,Day of the Establishment of the Slovak Republic,SK,False,True,,,['Public']
2022-01-06,Zjavenie Pána,Epiphany,SK,False,True,,,['Public']
2022-04-15,Veľkonočný piatok,Good Friday,SK,False,True,,,['Public']
2022-04-18,Veľkonočný pondelok,Easter Monday,SK,False,True,,,['Public']
2022-05-01,Sviatok práce,International Workers' Day,SK,False,True,,,['Public']
2022-05-08,Deň víťazstva nad fašizmom,Day of victory over fascism,SK,False,True,,,['Public']
2022-07-05,Sviatok svätého Cyrila a svätého Metoda,St. Cyril and Methodius Day,SK,False,True,,,['Public']
2022-08-29,Výročie Slovenského národného povstania,Slovak National Uprising anniversary,SK,False,True,,,['Public']
2022-09-01,Deň Ústavy Slovenskej republiky,Day of the Constitution of the Slovak Republic,SK,False,True,,,['Public']
2022-09-15,Sedembolestná Panna Mária,Day of Our Lady of the Seven Sorrows,SK,False,True,,,['Public']
2022-11-01,Sviatok Všetkých svätých,All Saints’ Day,SK,False,True,,,['Public']
2022-11-17,Deň boja za slobodu a demokraciu,Struggle for Freedom and Democracy Day,SK,False,True,,,['Public']
2022-12-24,Štedrý deň,Christmas Eve,SK,False,True,,,['Public']
2022-12-25,Prvý sviatok vianočný,Christmas Day,SK,False,True,,,['Public']
2022-12-26,Druhý sviatok vianočný,St. Stephen's Day,SK,False,True,,,['Public']
2023-01-01,Deň vzniku Slovenskej republiky,Day of the Establishment of the Slovak Republic,SK,False,True,,,['Public']
2023-01-06,Zjavenie Pána,Epiphany,SK,False,True,,,['Public']
2023-04-07,Veľkonočný piatok,Good Friday,SK,False,True,,,['Public']
2023-04-10,Veľkonočný pondelok,Easter Monday,SK,False,True,,,['Public']
2023-05-01,Sviatok práce,International Workers' Day,SK,False,True,,,['Public']
2023-05-08,Deň víťazstva nad fašizmom,Day of victory over fascism,SK,False,True,,,['Public']
2023-07-05,Sviatok svätého Cyrila a svätého Metoda,St. Cyril and Methodius Day,SK,False,True,,,['Public']
2023-08-29,Výročie Slovenského národného povstania,Slovak National Uprising anniversary,SK,False,True,,,['Public']
2023-09-01,Deň Ústavy Slovenskej republiky,Day of the Constitution of the Slovak Republic,SK,False,True,,,['Public']
2023-09-15,Sedembolestná Panna Mária,Day of Our Lady of the Seven Sorrows,SK,False,True,,,['Public']
2023-11-01,Sviatok Všetkých svätých,All Saints’ Day,SK,False,True,,,['Public']
2023-11-17,Deň boja za slobodu a demokraciu,Struggle for Freedom and Democracy Day,SK,False,True,,,['Public']
2023-12-24,Štedrý deň,Christmas Eve,SK,False,True,,,['Public']
2023-12-25,Prvý sviatok vianočný,Christmas Day,SK,False,True,,,['Public']
2023-12-26,Druhý sviatok vianočný,St. Stephen's Day,SK,False,True,,,['Public']
2024-01-01,Deň vzniku Slovenskej republiky,Day of the Establishment of the Slovak Republic,SK,False,True,,,['Public']
2024-01-06,Zjavenie Pána,Epiphany,SK,False,True,,,['Public']
2024-03-29,Veľkonočný piatok,Good Friday,SK,False,True,,,['Public']
2024-04-01,Veľkonočný pondelok,Easter Monday,SK,False,True,,,['Public']
2024-05-01,Sviatok práce,International Workers' Day,SK,False,True,,,['Public']
2024-05-08,Deň víťazstva nad fašizmom,Day of victory over fascism,SK,False,True,,,['Public']
2024-07-05,Sviatok svätého Cyrila a svätého Metoda,St. Cyril and Methodius Day,SK,False,True,,,['Public']
2024-08-29,Výročie Slovenského národného povstania,Slovak National Uprising anniversary,SK,False,True,,,['Public']
2024-09-01,Deň Ústavy Slovenskej republiky,Day of the Constitution of the Slovak Republic,SK,False,True,,,['Public']
2024-09-15,Sedembolestná Panna Mária,Day of Our Lady of the Seven Sorrows,SK,False,True,,,['Public']
2024-11-01,Sviatok Všetkých svätých,All Saints’ Day,SK,False,True,,,['Public']
2024-11-17,Deň boja za slobodu a demokraciu,Struggle for Freedom and Democracy Day,SK,False,True,,,['Public']
2024-12-24,Štedrý deň,Christmas Eve,SK,False,True,,,['Public']
2024-12-25,Prvý sviatok vianočný,Christmas Day,SK,False,True,,,['Public']
2024-12-26,Druhý sviatok vianočný,St. Stephen's Day,SK,False,True,,,['Public']
2025-01-01,Deň vzniku Slovenskej republiky,Day of the Establishment of the Slovak Republic,SK,False,True,,,['Public']
2025-01-06,Zjavenie Pána,Epiphany,SK,False,True,,,['Public']
2025-04-18,Veľkonočný piatok,Good Friday,SK,False,True,,,['Public']
2025-04-21,Veľkonočný pondelok,Easter Monday,SK,False,True,,,['Public']
2025-05-01,Sviatok práce,International Workers' Day,SK,False,True,,,['Public']
2025-05-08,Deň víťazstva nad fašizmom,Day of victory over fascism,SK,False,True,,,['Public']
2025-07-05,Sviatok svätého Cyrila a svätého Metoda,St. Cyril and Methodius Day,SK,False,True,,,['Public']
2025-08-29,Výročie Slovenského národného povstania,Slovak National Uprising anniversary,SK,False,True,,,['Public']
2025-09-01,Deň Ústavy Slovenskej republiky,Day of the Constitution of the Slovak Republic,SK,False,True,,,['Observance']
2025-09-15,Sedembolestná Panna Mária,Day of Our Lady of the Seven Sorrows,SK,False,True,,,['Public']
2025-11-01,Sviatok Všetkých svätých,All Saints’ Day,SK,False,True,,,['Public']
2025-11-17,Deň boja za slobodu a demokraciu,Struggle for Freedom and Democracy Day,SK,False,True,,,['Observance']
2025-12-24,Štedrý deň,Christmas Eve,SK,False,True,,,['Public']
2025-12-25,Prvý sviatok vianočný,Christmas Day,SK,False,True,,,['Public']
2025-12-26,Druhý sviatok vianočný,St. Stephen's Day,SK,False,True,,,['Public']
2020-01-01,Den obnovy samostatného českého státu; Nový rok,New Year's Day,CZ,False,True,,,['Public']
2020-04-10,Velký pátek,Good Friday,CZ,False,True,,,['Public']
2020-04-13,Velikonoční pondělí,Easter Monday,CZ,False,True,,,['Public']
2020-05-01,Svátek práce,Labour Day,CZ,False,True,,,['Public']
2020-05-08,Den vítězství,Liberation Day,CZ,False,True,,,['Public']
2020-07-05,Den slovanských věrozvěstů Cyrila a Metoděje,Saints Cyril and Methodius Day,CZ,False,True,,,['Public']
2020-07-06,Den upálení mistra Jana Husa,Jan Hus Day,CZ,False,True,,,['Public']
2020-09-28,Den české státnosti,St. Wenceslas Day,CZ,False,True,,,['Public']
2020-10-28,Den vzniku samostatného československého státu,Independent Czechoslovak State Day,CZ,False,True,,,['Public']
2020-11-17,Den boje za svobodu a demokracii a Mezinárodní den studentstva,Struggle for Freedom and Democracy Day,CZ,False,True,,,['Public']
2020-12-24,Štědrý den,Christmas Eve,CZ,False,True,,,['Public']
2020-12-25,1. svátek vánoční,Christmas Day,CZ,False,True,,,['Public']
2020-12-26,2. svátek vánoční,St. Stephen's Day,CZ,False,True,,,['Public']
2021-01-01,Den obnovy samostatného českého státu; Nový rok,New Year's Day,CZ,False,True,,,['Public']
2021-04-02,Velký pátek,Good Friday,CZ,False,True,,,['Public']
2021-04-05,Velikonoční pondělí,Easter Monday,CZ,False,True,,,['Public']
2021-05-01,Svátek práce,Labour Day,CZ,False,True,,,['Public']
2021-05-08,Den vítězství,Liberation Day,CZ,False,True,,,['Public']
2021-07-05,Den slovanských věrozvěstů Cyrila a Metoděje,Saints Cyril and Methodius Day,CZ,False,True,,,['Public']
2021-07-06,Den upálení mistra Jana Husa,Jan Hus Day,CZ,False,True,,,['Public']
2021-09-28,Den české státnosti,St. Wenceslas Day,CZ,False,True,,,['Public']
2021-10-28,Den vzniku samostatného československého státu,Independent Czechoslovak State Day,CZ,False,True,,,['Public']
2021-11-17,Den boje za svobodu a demokracii a Mezinárodní den studentstva,Struggle for Freedom and Democracy Day,CZ,False,True,,,['Public']
2021-12-24,Štědrý den,Christmas Eve,CZ,False,True,,,['Public']
2021-12-25,1. svátek vánoční,Christmas Day,CZ,False,True,,,['Public']
2021-12-26,2. svátek vánoční,St. Stephen's Day,CZ,False,True,,,['Public']
2022-01-01,Den obnovy samostatného českého státu; Nový rok,New Year's Day,CZ,False,True,,,['Public']
2022-04-15,Velký pátek,Good Friday,CZ,False,True,,,['Public']
2022-04-18,Velikonoční pondělí,Easter Monday,CZ,False,True,,,['Public']
2022-05-01,Svátek práce,Labour Day,CZ,False,True,,,['Public']
2022-05-08,Den vítězství,Liberation Day,CZ,False,True,,,['Public']
2022-07-05,Den slovanských věrozvěstů Cyrila a Metoděje,Saints Cyril and Methodius Day,CZ,False,True,,,['Public']
2022-07-06,Den upálení mistra Jana Husa,Jan Hus Day,CZ,False,True,,,['Public']
2022-09-28,Den české státnosti,St. Wenceslas Day,CZ,False,True,,,['Public']
2022-10-28,Den vzniku samostatného československého státu,Independent Czechoslovak State Day,CZ,False,True,,,['Public']
2022-11-17,Den boje za svobodu a demokracii a Mezinárodní den studentstva,Struggle for Freedom and Democracy Day,CZ,False,True,,,['Public']
2022-12-24,Štědrý den,Christmas Eve,CZ,False,True,,,['Public']
2022-12-25,1. svátek vánoční,Christmas Day,CZ,False,True,,,['Public']
2022-12-26,2. svátek vánoční,St. Stephen's Day,CZ,False,True,,,['Public']
2023-01-01,Den obnovy samostatného českého státu; Nový rok,New Year's Day,CZ,False,True,,,['Public']
2023-04-07,Velký pátek,Good Friday,CZ,False,True,,,['Public']
2023-04-10,Velikonoční pondělí,Easter Monday,CZ,False,True,,,['Public']
2023-05-01,Svátek práce,Labour Day,CZ,False,True,,,['Public']
2023-05-08,Den vítězství,Liberation Day,CZ,False,True,,,['Public']
2023-07-05,Den slovanských věrozvěstů Cyrila a Metoděje,Saints Cyril and Methodius Day,CZ,False,True,,,['Public']
2023-07-06,Den upálení mistra Jana Husa,Jan Hus Day,CZ,False,True,,,['Public']
2023-09-28,Den české státnosti,St. Wenceslas Day,CZ,False,True,,,['Public']
2023-10-28,Den vzniku samostatného československého státu,Independent Czechoslovak State Day,CZ,False,True,,,['Public']
2023-11-17,Den boje za svobodu a demokracii a Mezinárodní den studentstva,Struggle for Freedom and Democracy Day,CZ,False,True,,,['Public']
2023-12-24,Štědrý den,Christmas Eve,CZ,False,True,,,['Public']
2023-12-25,1. svátek vánoční,Christmas Day,CZ,False,True,,,['Public']
2023-12-26,2. svátek vánoční,St. Stephen's Day,CZ,False,True,,,['Public']
2024-01-01,Den obnovy samostatného českého státu; Nový rok,New Year's Day,CZ,False,True,,,['Public']
2024-03-29,Velký pátek,Good Friday,CZ,False,True,,,['Public']
2024-04-01,Velikonoční pondělí,Easter Monday,CZ,False,True,,,['Public']
2024-05-01,Svátek práce,Labour Day,CZ,False,True,,,['Public']
2024-05-08,Den vítězství,Liberation Day,CZ,False,True,,,['Public']
2024-07-05,Den slovanských věrozvěstů Cyrila a Metoděje,Saints Cyril and Methodius Day,CZ,False,True,,,['Public']
2024-07-06,Den upálení mistra Jana Husa,Jan Hus Day,CZ,False,True,,,['Public']
2024-09-28,Den české státnosti,St. Wenceslas Day,CZ,False,True,,,['Public']
2024-10-28,Den vzniku samostatného československého státu,Independent Czechoslovak State Day,CZ,False,True,,,['Public']
2024-11-17,Den boje za svobodu a demokracii a Mezinárodní den studentstva,Struggle for Freedom and Democracy Day,CZ,False,True,,,['Public']
2024-12-24,Štědrý den,Christmas Eve,CZ,False,True,,,['Public']
2024-12-25,1. svátek vánoční,Christmas Day,CZ,False,True,,,['Public']
2024-12-26,2. svátek vánoční,St. Stephen's Day,CZ,False,True,,,['Public']
2025-01-01,Den obnovy samostatného českého státu; Nový rok,New Year's Day,CZ,False,True,,,['Public']
2025-04-18,Velký pátek,Good Friday,CZ,False,True,,,['Public']
2025-04-21,Velikonoční pondělí,Easter Monday,CZ,False,True,,,['Public']
2025-05-01,Svátek práce,Labour Day,CZ,False,True,,,['Public']
2025-05-08,Den vítězství,Liberation Day,CZ,False,True,,,['Public']
2025-07-05,Den slovanských věrozvěstů Cyrila a Metoděje,Saints Cyril and Methodius Day,CZ,False,True,,,['Public']
2025-07-06,Den upálení mistra Jana Husa,Jan Hus Day,CZ,False,True,,,['Public']
2025-09-28,Den české státnosti,St. Wenceslas Day,CZ,False,True,,,['Public']
2025-10-28,Den vzniku samostatného československého státu,Independent Czechoslovak State Day,CZ,False,True,,,['Public']
2025-11-17,Den boje za svobodu a demokracii a Mezinárodní den studentstva,Struggle for Freedom and Democracy Day,CZ,False,True,,,['Public']
2025-12-24,Štědrý den,Christmas Eve,CZ,False,True,,,['Public']
2025-12-25,1. svátek vánoční,Christmas Day,CZ,False,True,,,['Public']
2025-12-26,2. svátek vánoční,St. Stephen's Day,CZ,False,True,,,['Public']"""
    
    # Convert CSV string to DataFrame
    from io import StringIO
    holidays_df = pd.read_csv(StringIO(csv_data))
    print("✓ Loaded holiday data from provided CSV content")

# Ensure date column is datetime
holidays_df['date'] = pd.to_datetime(holidays_df['date'])

# ============================================================================
# TASK 1b: Save the results into a file
# ============================================================================

holidays_df.to_csv('holidays_data.csv', index=False)
print("\n✓ Task 1b: Saved holiday data to 'holidays_data.csv'")
print(f"  Total holidays in dataset: {len(holidays_df)}")

# ============================================================================
# TASK 2a: Create a dataframe with counts of holidays per year split per country
# ============================================================================

# Extract year from date
holidays_df['Year'] = holidays_df['date'].dt.year

# Filter for years 2020-2025
holidays_df = holidays_df[(holidays_df['Year'] >= 2020) & (holidays_df['Year'] <= 2025)]

# Group by country and year, count holidays
holiday_counts = holidays_df.groupby(['countryCode', 'Year']).size().reset_index(name='HolidayCount')
holiday_counts.columns = ['Country', 'Year', 'HolidayCount']

# Sort for better presentation
holiday_counts = holiday_counts.sort_values(['Country', 'Year'])

print("\n✓ Task 2a: Created dataframe with holiday counts per year per country")
print("\nHoliday Counts per Year per Country:")
print(holiday_counts.to_string(index=False))

# ============================================================================
# TASK 2b: Save the results into a file
# ============================================================================

holiday_counts.to_csv('holiday_counts.csv', index=False)
print("\n✓ Task 2b: Saved holiday counts to 'holiday_counts.csv'")

# ============================================================================
# TASK 3a: Load the file 'input_file_task3.xlsx'
# ============================================================================

# Create the Excel file from provided content
excel_data = {
    'ID': ['ID_0001', 'ID_0002', 'ID_0003', 'ID_0004', 'ID_0005', 'ID_0006', 'ID_0007', 'ID_0008', 'ID_0009', 'ID_0010',
           'ID_0011', 'ID_0012', 'ID_0013', 'ID_0014', 'ID_0015', 'ID_0016', 'ID_0017', 'ID_0018', 'ID_0019', 'ID_0020',
           'ID_0021', 'ID_0022', 'ID_0023', 'ID_0024', 'ID_0025', 'ID_0026', 'ID_0027', 'ID_0028', 'ID_0029', 'ID_0030',
           'ID_0031', 'ID_0032', 'ID_0033', 'ID_0034', 'ID_0035', 'ID_0036', 'ID_0037', 'ID_0038', 'ID_0039', 'ID_0040',
           'ID_0041', 'ID_0042', 'ID_0043', 'ID_0044', 'ID_0045', 'ID_0046', 'ID_0047', 'ID_0048', 'ID_0049', 'ID_0050',
           'ID_0051', 'ID_0052', 'ID_0053', 'ID_0054', 'ID_0055', 'ID_0056', 'ID_0057', 'ID_0058', 'ID_0059', 'ID_0060',
           'ID_0061', 'ID_0062', 'ID_0063', 'ID_0064', 'ID_0065', 'ID_0066', 'ID_0067', 'ID_0068', 'ID_0069', 'ID_0070',
           'ID_0071', 'ID_0072', 'ID_0073', 'ID_0074', 'ID_0075', 'ID_0076', 'ID_0077', 'ID_0078', 'ID_0079', 'ID_0080',
           'ID_0081', 'ID_0082', 'ID_0083', 'ID_0084', 'ID_0085', 'ID_0086', 'ID_0087', 'ID_0088', 'ID_0089', 'ID_0090',
           'ID_0091', 'ID_0092', 'ID_0093', 'ID_0094', 'ID_0095', 'ID_0096', 'ID_0097', 'ID_0098', 'ID_0099', 'ID_0100'],
    'Country': ['Slovakia', 'Czechia', 'Germany', 'Slovakia', 'Czechia', 'Czechia', 'Austria', 'Slovakia', 'Slovakia', 'Germany',
                'Slovakia', 'Czechia', 'Slovakia', 'Austria', 'Slovakia', 'Czechia', 'Germany', 'Germany', 'Germany', 'Slovakia',
                'Germany', 'Austria', 'Slovakia', 'Germany', 'Czechia', 'Czechia', 'Austria', 'Czechia', 'Austria', 'Austria',
                'Czechia', 'Slovakia', 'Austria', 'Germany', 'Slovakia', 'Czechia', 'Czechia', 'Slovakia', 'Slovakia', 'Slovakia',
                'Germany', 'Slovakia', 'Czechia', 'Germany', 'Germany', 'Germany', 'Germany', 'Slovakia', 'Slovakia', 'Czechia',
                'Germany', 'Austria', 'Austria', 'Austria', 'Austria', 'Czechia', 'Germany', 'Czechia', 'Czechia', 'Slovakia',
                'Austria', 'Austria', 'Austria', 'Slovakia', 'Germany', 'Slovakia', 'Austria', 'Germany', 'Czechia', 'Germany',
                'Germany', 'Austria', 'Austria', 'Germany', 'Austria', 'Slovakia', 'Czechia', 'Czechia', 'Czechia', 'Slovakia',
                'Slovakia', 'Germany', 'Czechia', 'Czechia', 'Czechia', 'Austria', 'Slovakia', 'Austria', 'Austria', 'Czechia',
                'Germany', 'Slovakia', 'Austria', 'Germany', 'Czechia', 'Slovakia', 'Austria', 'Czechia', 'Czechia', 'Slovakia'],
    'Start Date': ['2022-10-05', '2020-03-02', '2024-08-11', '2022-12-09', '2020-06-30', '2024-12-25', '2021-11-16', '2024-09-01', '2024-04-09', '2022-08-19',
                   '2025-02-13', '2022-01-01', '2022-04-01', '2023-02-10', '2021-12-06', '2023-03-21', '2021-12-03', '2025-07-18', '2022-11-15', '2021-04-09',
                   '2020-04-20', '2020-01-20', '2022-08-22', '2022-01-06', '2023-05-10', '2025-11-10', '2021-12-15', '2023-10-22', '2025-11-02', '2021-08-25',
                   '2020-02-01', '2020-02-08', '2023-02-18', '2022-11-30', '2020-04-02', '2023-01-05', '2024-01-07', '2024-08-06', '2023-08-28', '2025-06-28',
                   '2022-05-02', '2024-08-04', '2021-08-05', '2022-07-30', '2023-11-23', '2023-04-19', '2025-02-18', '2023-03-30', '2022-05-19', '2024-08-22',
                   '2024-05-14', '2022-07-11', '2022-12-03', '2022-02-03', '2025-04-20', '2023-02-11', '2021-11-26', '2024-09-20', '2025-11-26', '2020-02-19',
                   '2025-11-19', '2023-05-19', '2021-11-29', '2023-11-25', '2022-04-06', '2023-09-04', '2022-07-28', '2025-10-19', '2020-12-10', '2022-10-02',
                   '2025-01-11', '2023-08-07', '2020-12-28', '2024-04-12', '2025-11-22', '2023-12-29', '2021-07-16', '2023-05-21', '2022-07-27', '2024-12-03',
                   '2024-10-22', '2023-05-23', '2021-08-20', '2020-05-03', '2025-02-06', '2023-06-22', '2022-05-06', '2024-05-26', '2020-03-03', '2021-11-23',
                   '2020-05-04', '2022-03-31', '2021-03-20', '2020-10-26', '2022-07-02', '2025-12-11', '2021-07-08', '2020-12-09', '2025-07-11', '2020-09-23'],
    'End Date': ['2022-10-05', '2020-03-03', '2024-08-19', '2022-12-14', '2020-07-09', '2024-12-29', '2021-11-24', '2024-09-09', '2024-04-13', '2022-08-26',
                 '2025-02-17', '2022-01-11', '2022-04-07', '2023-02-17', '2021-12-06', '2023-03-28', '2021-12-03', '2025-07-20', '2022-11-22', '2021-04-12',
                 '2020-04-26', '2020-01-23', '2022-08-25', '2022-01-16', '2023-05-10', '2025-11-12', '2021-12-23', '2023-10-30', '2025-11-04', '2021-08-29',
                 '2020-02-07', '2020-02-08', '2023-02-23', '2022-12-09', '2020-04-12', '2023-01-10', '2024-01-08', '2024-08-09', '2023-09-01', '2025-07-02',
                 '2022-05-10', '2024-08-08', '2021-08-09', '2022-08-04', '2023-11-27', '2023-04-20', '2025-02-28', '2023-03-31', '2022-05-23', '2024-08-31',
                 '2024-05-20', '2022-07-16', '2022-12-12', '2022-02-09', '2025-04-25', '2023-02-21', '2021-12-02', '2024-09-26', '2025-12-01', '2020-02-26',
                 '2025-11-29', '2023-05-24', '2021-12-07', '2023-12-04', '2022-04-14', '2023-09-05', '2022-08-03', '2025-10-28', '2020-12-14', '2022-10-10',
                 '2025-01-15', '2023-08-07', '2021-01-05', '2024-04-16', '2025-11-29', '2023-12-30', '2021-07-26', '2023-05-21', '2022-07-31', '2024-12-09',
                 '2024-10-30', '2023-05-28', '2021-08-28', '2020-05-10', '2025-02-16', '2023-06-30', '2022-05-14', '2024-05-27', '2020-03-04', '2021-11-27',
                 '2020-05-04', '2022-04-02', '2021-03-26', '2020-10-27', '2022-07-09', '2025-12-13', '2021-07-08', '2020-12-13', '2025-07-15', '2020-09-23']
}

# Create DataFrame for RawData
raw_data_df = pd.DataFrame(excel_data)
raw_data_df['Start Date'] = pd.to_datetime(raw_data_df['Start Date'])
raw_data_df['End Date'] = pd.to_datetime(raw_data_df['End Date'])

# Create DataFrame for LeadTimes
lead_times_data = {
    'Country': ['Slovakia', 'Czechia', 'Austria', 'Germany'],
    'LeadTime ( businessDays)': [3, 3, 4, 5]
}
lead_times_df = pd.DataFrame(lead_times_data)

print("\n✓ Task 3a: Loaded Excel file data (simulated from provided content)")
print(f"  RawData rows: {len(raw_data_df)}")
print(f"  LeadTimes rows: {len(lead_times_df)}")

# ============================================================================
# TASK 3b: Calculate business days between 'Start Date' and 'End Date'
# ============================================================================

def calculate_business_days(start_date, end_date, country):
    """
    Calculate business days between two dates excluding weekends and holidays
    """
    # Create date range from start to end (inclusive)
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    
    # Convert to list of dates
    dates = [d.date() for d in date_range]
    
    # Get holidays for the specific country
    # Map country names to country codes
    country_code_map = {
        'Slovakia': 'SK',
        'Czechia': 'CZ', 
        'Austria': 'AT',
        'Germany': 'DE'
    }
    
    country_code = country_code_map.get(country)
    if not country_code:
        return len(dates)  # If country not found, return all days
    
    # Get holidays for this country
    country_holidays = holidays_df[holidays_df['countryCode'] == country_code]['date'].dt.date.tolist()
    
    # Count business days (exclude weekends and holidays)
    business_days = 0
    for date in dates:
        # Check if weekday (Monday=0, Sunday=6)
        if date.weekday() < 5:  # Monday to Friday
            # Check if not a holiday
            if date not in country_holidays:
                business_days += 1
    
    return business_days

# Calculate business days for each row
raw_data_df['BusinessDays'] = raw_data_df.apply(
    lambda row: calculate_business_days(row['Start Date'], row['End Date'], row['Country']), 
    axis=1
)

print("\n✓ Task 3b: Calculated business days for each record")
print(f"  Sample calculation - first 5 rows:")
print(raw_data_df[['ID', 'Country', 'Start Date', 'End Date', 'BusinessDays']].head().to_string(index=False))

# ============================================================================
# TASK 3c: Get lead-times for specific countries for all lines
# ============================================================================

# Merge lead times with raw data
result_df = pd.merge(raw_data_df, lead_times_df, on='Country', how='left')
result_df = result_df.rename(columns={'LeadTime ( businessDays)': 'LeadTime'})

print("\n✓ Task 3c: Added lead times for each country")
print(f"  Sample with lead times - first 5 rows:")
print(result_df[['ID', 'Country', 'BusinessDays', 'LeadTime']].head().to_string(index=False))

# ============================================================================
# TASK 3d: Check if repair was hit or miss
# ============================================================================

result_df['Status'] = result_df.apply(
    lambda row: 'Hit' if row['BusinessDays'] <= row['LeadTime'] else 'Miss', 
    axis=1
)

print("\n✓ Task 3d: Determined Hit/Miss status for each record")
print(f"  Status distribution:")
print(result_df['Status'].value_counts())

# ============================================================================
# TASK 3e: Aggregate data on country level
# ============================================================================

# Create aggregation
agg_data = []
for country in result_df['Country'].unique():
    country_data = result_df[result_df['Country'] == country]
    hit_count = len(country_data[country_data['Status'] == 'Hit'])
    miss_count = len(country_data[country_data['Status'] == 'Miss'])
    total_count = hit_count + miss_count
    hit_rate = (hit_count / total_count * 100) if total_count > 0 else 0
    
    agg_data.append({
        'Country': country,
        'Hit count': hit_count,
        'Miss count': miss_count,
        'Hit rate (%)': round(hit_rate, 2)
    })

# Create final aggregation DataFrame
aggregation_df = pd.DataFrame(agg_data)
aggregation_df = aggregation_df.sort_values('Country')

print("\n✓ Task 3e: Aggregated data on country level")
print("\nCountry-level Aggregation:")
print(aggregation_df.to_string(index=False))

# ============================================================================
# TASK 3f: Save the results into a file
# ============================================================================

# Save all results to CSV
result_df.to_csv('repair_analysis_results.csv', index=False)
aggregation_df.to_csv('country_aggregation.csv', index=False)

print("\n✓ Task 3f: Saved results to files")
print("  - 'repair_analysis_results.csv' (detailed results)")
print("  - 'country_aggregation.csv' (country-level aggregation)")

# ============================================================================
# FINAL SUMMARY
# ============================================================================

print("\n" + "="*60)
print("TASK COMPLETION SUMMARY")
print("="*60)
print("✓ Task 1a: Fetched holiday data from API/CSV")
print("✓ Task 1b: Saved holiday data to 'holidays_data.csv'")
print("✓ Task 2a: Created dataframe with holiday counts per year per country")
print("✓ Task 2b: Saved holiday counts to 'holiday_counts.csv'")
print("✓ Task 3a: Loaded Excel file data")
print("✓ Task 3b: Calculated business days for each record")
print("✓ Task 3c: Added lead times for each country")
print("✓ Task 3d: Determined Hit/Miss status for each record")
print("✓ Task 3e: Aggregated data on country level")
print("✓ Task 3f: Saved results to CSV files")
print("\nGenerated files:")
print("  1. holidays_data.csv")
print("  2. holiday_counts.csv")
print("  3. repair_analysis_results.csv")
print("  4. country_aggregation.csv")
print("="*60)