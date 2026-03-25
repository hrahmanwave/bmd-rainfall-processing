"""
BMD 3-Hourly Rainfall Data Processing Pipeline
================================================
Processes Bangladesh Meteorological Department (BMD) 3-hourly rainfall data
from multiple stations into tidy long-format output with temporal analysis.

Author: hrahmanwave
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import warnings
import json

warnings.filterwarnings('ignore')


class BMDRainfallProcessor:
    """
    Processes raw BMD 3-hourly rainfall CSV/Excel files into tidy long-format.
    
    Input format expected:
        - Columns: Station, Date, and 8 three-hourly slots (00-03, 03-06, ..., 21-24)
        - Multiple stations across multiple years
    
    Output:
        - Tidy long-format Excel: one row per station-datetime-observation
        - Summary statistics per station
        - Monthly/seasonal aggregations
    """

    # BMD 3-hourly time slots (UTC+6)
    TIME_SLOTS = [
        ('00:00', '03:00'),
        ('03:00', '06:00'),
        ('06:00', '09:00'),
        ('09:00', '12:00'),
        ('12:00', '15:00'),
        ('15:00', '18:00'),
        ('18:00', '21:00'),
        ('21:00', '24:00'),
    ]

    SLOT_LABELS = [
        '00-03', '03-06', '06-09', '09-12',
        '12-15', '15-18', '18-21', '21-24'
    ]

    # Bangladesh seasons
    SEASONS = {
        'Pre-monsoon': [3, 4, 5],
        'Monsoon': [6, 7, 8, 9],
        'Post-monsoon': [10, 11],
        'Winter': [12, 1, 2],
    }

    # Key coastal stations for 3-hourly analysis
    KEY_STATIONS = [
        'Khepupara', 'Patuakhali', 'Bhola',
        'Satkhira', 'Mongla', 'Khulna'
    ]

    def __init__(self, input_path=None):
        self.input_path = input_path
        self.raw_df = None
        self.long_df = None
        self.station_summary = None

    def load_data(self, input_path=None):
        """Load raw BMD data from CSV or Excel."""
        path = input_path or self.input_path
        if path is None:
            raise ValueError("No input path provided")

        path = Path(path)
        if path.suffix in ['.xlsx', '.xls']:
            self.raw_df = pd.read_excel(path)
        elif path.suffix == '.csv':
            self.raw_df = pd.read_csv(path)
        else:
            raise ValueError(f"Unsupported format: {path.suffix}")

        print(f"Loaded {len(self.raw_df)} rows from {path.name}")
        print(f"Columns: {list(self.raw_df.columns)}")
        print(f"Stations: {self.raw_df['Station'].nunique()}")
        return self

    def load_from_dataframe(self, df):
        """Load data directly from a DataFrame."""
        self.raw_df = df.copy()
        return self

    def to_long_format(self):
        """
        Convert wide-format BMD data to tidy long-format.
        
        Wide: Station | Date | 00-03 | 03-06 | ... | 21-24
        Long: Station | Date | Datetime | Slot | Rainfall_mm
        """
        if self.raw_df is None:
            raise ValueError("No data loaded. Call load_data() first.")

        records = []
        slot_cols = [c for c in self.raw_df.columns if '-' in str(c) and any(
            s in str(c) for s in ['00', '03', '06', '09', '12', '15', '18', '21']
        )]

        # If slot columns not auto-detected, use positional
        if len(slot_cols) < 8:
            # Assume first col = Station, second = Date, next 8 = slots
            slot_cols = list(self.raw_df.columns[2:10])

        for _, row in self.raw_df.iterrows():
            station = row.iloc[0]  # Station name
            date_val = row.iloc[1]  # Date

            if pd.isna(date_val):
                continue

            # Parse date
            if isinstance(date_val, str):
                try:
                    date = pd.to_datetime(date_val)
                except:
                    continue
            else:
                date = pd.to_datetime(date_val)

            # Extract each 3-hourly slot
            for i, slot_label in enumerate(self.SLOT_LABELS):
                col_idx = i + 2  # offset past Station, Date
                if col_idx >= len(row):
                    break

                rainfall = row.iloc[col_idx]

                # Handle missing/trace values
                if pd.isna(rainfall) or str(rainfall).strip().upper() in ['T', 'TR', 'TRACE', '-', '']:
                    rainfall_mm = 0.0
                    is_trace = str(rainfall).strip().upper() in ['T', 'TR', 'TRACE']
                else:
                    try:
                        rainfall_mm = float(rainfall)
                    except (ValueError, TypeError):
                        rainfall_mm = np.nan
                        is_trace = False
                        continue

                    is_trace = False

                # Build datetime for this slot
                start_hour = int(slot_label.split('-')[0])
                slot_datetime = date + timedelta(hours=start_hour)

                records.append({
                    'Station': str(station).strip(),
                    'Date': date.date(),
                    'Year': date.year,
                    'Month': date.month,
                    'Day': date.day,
                    'Slot': slot_label,
                    'Slot_Start_Hour': start_hour,
                    'Datetime': slot_datetime,
                    'Rainfall_mm': rainfall_mm,
                    'Is_Trace': is_trace if 'is_trace' in dir() else False,
                })

        self.long_df = pd.DataFrame(records)

        # Add season
        self.long_df['Season'] = self.long_df['Month'].apply(self._get_season)

        # Sort
        self.long_df.sort_values(['Station', 'Datetime'], inplace=True)
        self.long_df.reset_index(drop=True, inplace=True)

        print(f"Long format: {len(self.long_df)} rows")
        print(f"Date range: {self.long_df['Date'].min()} to {self.long_df['Date'].max()}")
        print(f"Stations: {self.long_df['Station'].nunique()}")
        return self

    def compute_station_summary(self):
        """Compute summary statistics per station."""
        if self.long_df is None:
            raise ValueError("No long-format data. Call to_long_format() first.")

        summaries = []
        for station, grp in self.long_df.groupby('Station'):
            daily = grp.groupby('Date')['Rainfall_mm'].sum()
            summaries.append({
                'Station': station,
                'Total_Records': len(grp),
                'Date_Start': grp['Date'].min(),
                'Date_End': grp['Date'].max(),
                'Total_Days': daily.shape[0],
                'Total_Rainfall_mm': daily.sum(),
                'Mean_Daily_mm': daily.mean(),
                'Max_Daily_mm': daily.max(),
                'Rainy_Days': (daily > 0.1).sum(),
                'Rainy_Day_Pct': (daily > 0.1).mean() * 100,
                'Max_3hr_mm': grp['Rainfall_mm'].max(),
                'Mean_3hr_mm': grp.loc[grp['Rainfall_mm'] > 0, 'Rainfall_mm'].mean(),
            })

        self.station_summary = pd.DataFrame(summaries)
        return self

    def compute_monthly_stats(self):
        """Compute monthly rainfall statistics per station."""
        if self.long_df is None:
            raise ValueError("No long-format data.")

        monthly = self.long_df.groupby(['Station', 'Year', 'Month']).agg(
            Total_mm=('Rainfall_mm', 'sum'),
            Max_3hr_mm=('Rainfall_mm', 'max'),
            Count=('Rainfall_mm', 'count'),
            Rainy_Slots=('Rainfall_mm', lambda x: (x > 0.1).sum()),
        ).reset_index()

        month_names = {1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun',
                       7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'}
        monthly['Month_Name'] = monthly['Month'].map(month_names)
        monthly['Season'] = monthly['Month'].apply(self._get_season)
        return monthly

    def compute_diurnal_pattern(self):
        """Compute average diurnal rainfall pattern per station."""
        if self.long_df is None:
            raise ValueError("No long-format data.")

        diurnal = self.long_df.groupby(['Station', 'Slot']).agg(
            Mean_mm=('Rainfall_mm', 'mean'),
            Max_mm=('Rainfall_mm', 'max'),
            Frequency=('Rainfall_mm', lambda x: (x > 0.1).mean() * 100),
        ).reset_index()
        return diurnal

    def compute_huff_quartile(self):
        """
        Classify storm events by Huff quartile method.
        First-Quartile: peak intensity in first 25% of storm duration.
        Validated as best fit for Bay of Bengal front-loaded rainfall.
        """
        if self.long_df is None:
            raise ValueError("No long-format data.")

        results = []
        for station, grp in self.long_df.groupby('Station'):
            for date, day_grp in grp.groupby('Date'):
                rain = day_grp.sort_values('Slot_Start_Hour')['Rainfall_mm'].values
                total = rain.sum()
                if total < 1.0:  # skip dry days
                    continue

                n = len(rain)
                q_size = max(1, n // 4)
                quartile_sums = [
                    rain[:q_size].sum(),
                    rain[q_size:2*q_size].sum(),
                    rain[2*q_size:3*q_size].sum(),
                    rain[3*q_size:].sum(),
                ]
                peak_quartile = np.argmax(quartile_sums) + 1

                results.append({
                    'Station': station,
                    'Date': date,
                    'Total_mm': total,
                    'Peak_Quartile': peak_quartile,
                    'Q1_pct': quartile_sums[0] / total * 100,
                    'Q2_pct': quartile_sums[1] / total * 100,
                    'Q3_pct': quartile_sums[2] / total * 100,
                    'Q4_pct': quartile_sums[3] / total * 100,
                })

        huff_df = pd.DataFrame(results)
        return huff_df

    def export_excel(self, output_path='bmd_rainfall_processed.xlsx'):
        """Export all processed data to a multi-sheet Excel workbook."""
        if self.long_df is None:
            raise ValueError("No processed data to export.")

        if self.station_summary is None:
            self.compute_station_summary()

        monthly = self.compute_monthly_stats()
        diurnal = self.compute_diurnal_pattern()
        huff = self.compute_huff_quartile()

        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            self.long_df.to_excel(writer, sheet_name='Long_Format', index=False)
            self.station_summary.to_excel(writer, sheet_name='Station_Summary', index=False)
            monthly.to_excel(writer, sheet_name='Monthly_Stats', index=False)
            diurnal.to_excel(writer, sheet_name='Diurnal_Pattern', index=False)
            huff.to_excel(writer, sheet_name='Huff_Quartile', index=False)

        print(f"Exported to {output_path}")
        print(f"  - Long_Format: {len(self.long_df)} rows")
        print(f"  - Station_Summary: {len(self.station_summary)} rows")
        print(f"  - Monthly_Stats: {len(monthly)} rows")
        print(f"  - Diurnal_Pattern: {len(diurnal)} rows")
        print(f"  - Huff_Quartile: {len(huff)} rows")
        return output_path

    def _get_season(self, month):
        for season, months in self.SEASONS.items():
            if month in months:
                return season
        return 'Unknown'


# =============================================================================
# Demo Data Generator
# =============================================================================

def generate_demo_data(n_stations=6, start_date='2019-01-01', end_date='2024-12-31'):
    """
    Generate realistic demo BMD 3-hourly rainfall data.
    NOT real BMD data — for demonstration purposes only.
    """
    stations = [
        'Khepupara', 'Patuakhali', 'Bhola',
        'Satkhira', 'Mongla', 'Khulna'
    ][:n_stations]

    # Monthly mean rainfall (mm/day) — approximate Bay of Bengal pattern
    monthly_means = {
        1: 5, 2: 10, 3: 20, 4: 55, 5: 100,
        6: 250, 7: 300, 8: 280, 9: 220, 10: 120,
        11: 30, 12: 8
    }

    dates = pd.date_range(start_date, end_date, freq='D')
    np.random.seed(42)

    records = []
    for station in stations:
        station_factor = np.random.uniform(0.7, 1.3)
        for date in dates:
            month_mean = monthly_means[date.month] * station_factor
            daily_total = max(0, np.random.exponential(month_mean / 30))

            if daily_total < 0.5:
                slots = [0.0] * 8
            else:
                # Front-loaded distribution (Huff First-Quartile pattern)
                weights = np.array([0.25, 0.20, 0.15, 0.12, 0.10, 0.08, 0.06, 0.04])
                weights = weights + np.random.dirichlet(np.ones(8)) * 0.3
                weights = weights / weights.sum()
                slots = (daily_total * weights).tolist()
                slots = [round(max(0, s), 1) for s in slots]

            records.append([station, date.strftime('%Y-%m-%d')] + slots)

    cols = ['Station', 'Date'] + [
        '00-03', '03-06', '06-09', '09-12',
        '12-15', '15-18', '18-21', '21-24'
    ]
    df = pd.DataFrame(records, columns=cols)
    return df


# =============================================================================
# CLI Entry Point
# =============================================================================

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='BMD 3-Hourly Rainfall Processor')
    parser.add_argument('input', nargs='?', help='Input CSV/Excel file path')
    parser.add_argument('-o', '--output', default='bmd_rainfall_processed.xlsx',
                        help='Output Excel file path')
    parser.add_argument('--demo', action='store_true',
                        help='Generate demo data instead of reading input')
    args = parser.parse_args()

    processor = BMDRainfallProcessor()

    if args.demo or args.input is None:
        print("Generating demo data (6 stations, 2019-2024)...")
        demo_df = generate_demo_data()
        processor.load_from_dataframe(demo_df)
    else:
        processor.load_data(args.input)

    processor.to_long_format()
    processor.compute_station_summary()
    processor.export_excel(args.output)
    print("\nDone!")
