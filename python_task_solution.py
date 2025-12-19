import requests
import pandas as pd
import numpy as np
from pathlib import Path

# --------------------------------------------------
# CONFIGURATION
# --------------------------------------------------

COUNTRIES = ['AT', 'DE', 'SK', 'CZ']
YEARS = range(2020, 2026)

OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(exist_ok=True)

# --------------------------------------------------
# TASK 1a – DOWNLOAD PUBLIC HOLIDAYS FROM API
# --------------------------------------------------

all_holidays = []

for country in COUNTRIES:
    for year in YEARS:
        url = f"https://date.nager.at/api/v3/PublicHolidays/{year}/{country}"
        response = requests.get(url)

        if response.status_code != 200:
            raise RuntimeError(f"API error for {country}, {year}")

        for h in response.json():
            all_holidays.append({
                "CountryCode": country,
                "Country": h["countryCode"],
                "Date": h["date"],
                "LocalName": h["localName"],
                "Name": h["name"],
                "Year": year
            })

holidays_df = pd.DataFrame(all_holidays)
holidays_df["Date"] = pd.to_datetime(holidays_df["Date"])

# --------------------------------------------------
# TASK 1b – SAVE HOLIDAY LIST
# --------------------------------------------------

holidays_df.to_csv(
    OUTPUT_DIR / "task1_holidays.csv",
    index=False
)

# --------------------------------------------------
# TASK 2a – COUNT HOLIDAYS PER COUNTRY / YEAR
# --------------------------------------------------

holiday_counts = (
    holidays_df
    .groupby(["CountryCode", "Year"])
    .size()
    .reset_index(name="HolidayCount")
)

# --------------------------------------------------
# TASK 2b – SAVE COUNTS
# --------------------------------------------------

holiday_counts.to_csv(
    OUTPUT_DIR / "task2_holiday_counts.csv",
    index=False
)

# --------------------------------------------------
# TASK 3a – LOAD INPUT EXCEL
# --------------------------------------------------

raw_data = pd.read_excel(
    "input_file_task3.xlsx",
    sheet_name="RawData"
)

lead_times = pd.read_excel(
    "input_file_task3.xlsx",
    sheet_name="LeadTimes"
)

# --------------------------------------------------
# TASK 3b – BUSINESS DAYS CALCULATION
# --------------------------------------------------

holiday_calendar = holidays_df[["CountryCode", "Date"]].drop_duplicates()

def business_days(start_date, end_date, country):
    country_holidays = holiday_calendar[
        holiday_calendar["CountryCode"] == country
    ]["Date"].values.astype("datetime64[D]")

    return np.busday_count(
        start_date.date(),
        (end_date + pd.Timedelta(days=1)).date(),
        holidays=country_holidays
    )

raw_data["BusinessDays"] = raw_data.apply(
    lambda r: business_days(
        r["Start Date"],
        r["End Date"],
        r["CountryCode"]
    ),
    axis=1
)

# --------------------------------------------------
# TASK 3c – MERGE LEAD TIMES
# --------------------------------------------------

merged = raw_data.merge(
    lead_times,
    on="CountryCode",
    how="left"
)

# --------------------------------------------------
# TASK 3d – HIT / MISS LOGIC
# --------------------------------------------------

merged["Result"] = np.where(
    merged["BusinessDays"] <= merged["LeadTime"],
    "Hit",
    "Miss"
)

# --------------------------------------------------
# TASK 3e – AGGREGATE BY COUNTRY
# --------------------------------------------------

summary = (
    merged
    .groupby("Country")
    .agg(
        Hit_count=("Result", lambda x: (x == "Hit").sum()),
        Miss_count=("Result", lambda x: (x == "Miss").sum())
    )
    .reset_index()
)

summary["Hit_rate (%)"] = (
    summary["Hit_count"] /
    (summary["Hit_count"] + summary["Miss_count"]) * 100
).round(2)

# --------------------------------------------------
# TASK 3f – SAVE FINAL RESULTS
# --------------------------------------------------

summary.to_csv(
    OUTPUT_DIR / "task3_country_summary.csv",
    index=False
)
