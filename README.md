# BMD 3-Hourly Rainfall Data Processing

Python pipeline and interactive dashboard for processing Bangladesh Meteorological Department (BMD) 3-hourly rainfall gauge data into tidy long-format output with temporal and spatial analysis.

## Overview

BMD provides 3-hourly rainfall observations from stations across Bangladesh in wide-format tables. This toolkit converts that raw data into analysis-ready long format, computes station-level statistics, and provides an interactive HTML dashboard for visualization — all without external CDN dependencies.

### Pipeline Features

- **Wide-to-long conversion**: Transforms BMD's 8-column slot format (00-03, 03-06, ..., 21-24) into tidy long-format with one row per station-datetime-observation
- **Trace value handling**: Automatically detects and handles BMD's trace rainfall markers (T, TR, TRACE)
- **Temporal enrichment**: Adds year, month, day, season (Pre-monsoon, Monsoon, Post-monsoon, Winter), and datetime columns
- **Station summary statistics**: Total rainfall, mean/max daily, rainy day percentage, max 3-hourly intensity
- **Monthly aggregation**: Monthly totals, max intensity, and rainy slot counts per station
- **Diurnal pattern analysis**: Mean and max rainfall by 3-hourly slot per station
- **Huff quartile classification**: Classifies storm events by temporal distribution pattern — First-Quartile validated as best fit for Bay of Bengal front-loaded rainfall
- **Multi-sheet Excel export**: All outputs in a single workbook with 5 sheets

### Dashboard Features

- **Fully self-contained HTML**: No CDN dependencies, runs offline
- **Canvas API charts**: Monthly distribution, diurnal pattern, Huff quartile, seasonal breakdown, annual totals
- **Interactive filters**: Filter by station and year
- **Summary statistics**: Total records, station count, date range, total rainfall, max 3-hr intensity
- **CSV export**: Download filtered long-format data

## Files

| File | Description |
|---|---|
| `bmd_rainfall_processor.py` | Main processing pipeline (Python) |
| `dashboard.html` | Self-contained interactive dashboard |
| `demo_output.xlsx` | Demo output (synthetic data, not real BMD data) |

## Quick Start

### Using the Python Pipeline

```bash
# Process a real BMD data file
python bmd_rainfall_processor.py input_data.csv -o processed_output.xlsx

# Generate demo data to explore the pipeline
python bmd_rainfall_processor.py --demo
```

### Using as a Library

```python
from bmd_rainfall_processor import BMDRainfallProcessor

processor = BMDRainfallProcessor()
processor.load_data('bmd_raw_data.csv')
processor.to_long_format()
processor.compute_station_summary()
processor.export_excel('output.xlsx')

# Access DataFrames directly
long_df = processor.long_df              # 105k+ rows tidy format
summary = processor.station_summary      # per-station stats
monthly = processor.compute_monthly_stats()
diurnal = processor.compute_diurnal_pattern()
huff = processor.compute_huff_quartile()
```

### Using the Dashboard

Open `dashboard.html` in any browser. Click **Demo Data** to explore with synthetic data, or **Load CSV/Excel** to load your own BMD file.

## Input Format

The pipeline expects BMD data in this wide format (CSV or Excel):

```
Station,Date,00-03,03-06,06-09,09-12,12-15,15-18,18-21,21-24
Khepupara,2023-05-14,12.5,8.3,45.2,32.1,18.7,5.4,2.1,0.0
Patuakhali,2023-05-14,8.0,15.6,38.4,28.9,12.3,4.8,1.2,0.0
```

## Output Sheets

The Excel output contains 5 sheets:

1. **Long_Format** — One row per station-datetime-observation (Station, Date, Year, Month, Day, Slot, Datetime, Rainfall_mm, Season)
2. **Station_Summary** — Per-station statistics (total days, total/mean/max rainfall, rainy day percentage)
3. **Monthly_Stats** — Monthly aggregations per station per year
4. **Diurnal_Pattern** — Mean/max rainfall and frequency by 3-hourly slot per station
5. **Huff_Quartile** — Storm event classification with quartile percentages

## Key Stations

Six coastal stations used for primary 3-hourly analysis:
Khepupara, Patuakhali, Bhola, Satkhira, Mongla, Khulna

## Methodology Notes

- **Huff First-Quartile**: Validated as best fit for observed front-loaded rainfall patterns in the Bay of Bengal region. Most storm events show peak intensity in the first 25% of duration.
- **Bangladesh Seasons**: Pre-monsoon (Mar–May), Monsoon (Jun–Sep), Post-monsoon (Oct–Nov), Winter (Dec–Feb)
- **Trace handling**: BMD uses "T" or "TR" for trace rainfall (< 0.1 mm); treated as 0.0 mm in processing

## Dependencies

```
pandas
numpy
openpyxl
```

Install: `pip install pandas numpy openpyxl`

## Demo Data Disclaimer

The `demo_output.xlsx` file contains **synthetic data generated for demonstration purposes**. It is NOT real BMD observational data. The synthetic data follows realistic seasonal patterns (monsoon peak, winter minimum, front-loaded diurnal distribution) but should not be used for any scientific analysis.

## Context

Developed for cyclone rainfall forcing analysis as part of coastal hydrodynamic modeling research (Delft3D storm surge simulation) at the **Institute of Water and Flood Management (IWFM), BUET**, Bangladesh.

## License

MIT License — see [LICENSE](LICENSE).

### Requirements

`Python 3.x` · `pandas` · `numpy` · `scipy` · `openpyxl` · `Jupyter Notebook`


## Author

[hrahmanwave](https://github.com/hrahmanwave) 

[![ResearchGate](https://img.shields.io/badge/ResearchGate-00CCBB?style=flat&logo=researchgate&logoColor=white)](https://www.researchgate.net/profile/Md-Rahman-1059)
[![Google Scholar](https://img.shields.io/badge/Google_Scholar-4285F4?style=flat&logo=googlescholar&logoColor=white)](https://scholar.google.com/citations?user=Kc736lkAAAAJ&hl)

