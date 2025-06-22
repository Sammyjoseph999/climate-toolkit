# Climate Toolkit

[![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)](https://www.python.org/)
[![License](https://img.shields.io/github/license/Sammyjoseph999/climate-toolkit)](./LICENSE)
[![cdsapi](https://img.shields.io/badge/cdsapi-0.7.6-yellow)](https://pypi.org/project/cdsapi/)
[![earthaccess](https://img.shields.io/badge/earthaccess-0.14.0-brightgreen)](https://pypi.org/project/earthaccess/)
[![pydantic](https://img.shields.io/badge/pydantic-2.11.7-blue)](https://pypi.org/project/pydantic/)
[![python-dotenv](https://img.shields.io/badge/python--dotenv-1.1.0-lightgrey)](https://pypi.org/project/python-dotenv/)
[![requests](https://img.shields.io/badge/requests-2.32.4-red)](https://pypi.org/project/requests/)
[![PyYAML](https://img.shields.io/badge/PyYAML-6.0.2-blueviolet)](https://pypi.org/project/PyYAML/)

A modular Python library for downloading and analyzing climate data from public datasets like CHIRPS, IMERG, ERA5, AGERA5, and TerraClimate.

---

## About

The Climate Toolkit offers a unified, programmatic interface to:

- Retrieve climate data from CHIRPS, IMERG, ERA5, AGERA5, and TerraClimate
- Compute rainfall statistics, anomalies, and hazard indicators
- Compare climate trends over historical and seasonal periods

---

## Project Structure

```
climate_toolkit/
├── calculate_hazards/       # Hazard metrics like SPI
├── climate_statistics/      # Stats and anomalies
├── compare_periods/         # Compare historic trends
├── fetch_data/              # Modular data downloaders
└── season_analysis/         # Onset/cessation detection
```

---

## Getting Started

1. **Clone the repository**

   ```bash
   git clone https://github.com/Sammyjoseph999/climate-toolkit.git
   cd climate-toolkit
   ```

2. **Set up a virtual environment**

   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**

   ```bash
   pip install -r requirements.txt
   ```

4. **Create and configure your `.env`**

   ```bash
   cp .env.example .env
   ```

---

## How to Use

To download climate data from the terminal, run:

```bash
python climate_toolkit/fetch_data/source_data/source_data.py
```

This will trigger the configured download process based on the parameters defined in the `SourceData` class within the script.

---

## Development

### Setting Up

- All configuration values (e.g., API keys) are managed via `.env` using `python-dotenv`.
- Modular dataset handlers are found in `fetch_data/`, each with `DownloadData` classes.
- Common utilities like enums and settings are stored in `fetch_data/sources/utils/`.

### Solution Architecture

- Each dataset (e.g., CHIRPS, IMERG) follows a consistent interface: `DownloadData` class
- The `SourceData` class in `source_data.py` dynamically routes to the appropriate source client
- All modules extend a shared `DataDownloadBase` for consistency

### Solution Modules

| Module                | Description                            |
|-----------------------|----------------------------------------|
| `calculate_hazards`   | Climate hazard indices (e.g., SPI)     |
| `climate_statistics`  | Mean, anomaly, and seasonal summaries  |
| `compare_periods`     | Analyze historical changes             |
| `fetch_data`          | Dataset-specific download logic        |
| `season_analysis`     | Detect onset, cessation, dry spells    |

---

## Best Practices

- ✅ Environment variables for secrets (`.env`)
- ✅ Pydantic for settings validation
- ✅ Follow separation of concerns: each module does one thing well
- ✅ Dataset wrappers are intentionally minimal and extensible
- ✅ Consistent naming, date handling, and logging across modules

---

## Contributing

We welcome PRs and suggestions!

1. Fork the repo
2. Work in a feature branch
3. Follow module layout and formatting
4. Submit a pull request with a clear description

---

## License

This project is licensed under the [MIT License](./LICENSE).
