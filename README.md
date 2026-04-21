# ZAP-SCRAPING

Web scraping pipeline to collect, geocode, and export real estate listings from [ZAP Imóveis](https://www.zapimoveis.com.br), Brazil's leading real estate platform.

## Pipeline

The pipeline runs three steps in sequence:

1. **Scrape** — collects listings from ZAP Imóveis using Selenium with `undetected-chromedriver`. Uses a price-band strategy to work around the platform's 500-listing-per-search cap. Saves checkpoints after each band so no data is lost on crash.

2. **Geocode** — resolves listing addresses to coordinates via Nominatim (OpenStreetMap). Results are cached locally in `data/.geocache.json` to avoid redundant requests. Each row is also assigned an [H3](https://h3geo.org/) hexagonal cell index at resolution 9.

3. **Export** — converts the geocoded Parquet file to a GeoPackage (`.gpkg`) ready to open in QGIS or any GIS tool.

## Configuration

Copy `config.example.json` to `config.json` and fill in your target location:

```json
{
  "city": "porto-alegre",
  "state": "rs",
  "label": "Porto Alegre",
  "transacao": "aluguel",
  "max_listings": 1000
}
```

| Field | Description |
|---|---|
| `city` | City slug as it appears in ZAP URLs |
| `state` | Two-letter state code (lowercase) |
| `label` | Human-readable city name used in logs |
| `transacao` | `"aluguel"` (rent), `"venda"` (sale), or a list of both |
| `max_listings` | Optional cap on listings collected per run |

## Running

```bash
python run.py
```

## Data collected

Each listing record contains:

| Field | Description |
|---|---|
| `id` | ZAP listing ID |
| `url` | Listing URL |
| `transacao` | Transaction type (`aluguel` / `venda`) |
| `descricao` | Listing title |
| `bairro` | Neighborhood |
| `cidade` | City |
| `endereco` | Street address |
| `area` | Area in m² |
| `quartos` | Number of bedrooms |
| `banheiros` | Number of bathrooms |
| `garagens` | Number of parking spaces |
| `preco` | Price (BRL) |
| `periodo_aluguel` | Rental period (`mensal` / `diario`) |
| `condominio` | Condo fee (BRL) |
| `iptu` | Property tax (BRL) |
| `scraped_at` | Timestamp of collection |
| `latitude` / `longitude` | Geocoded coordinates |
| `h3_id` | H3 cell index at resolution 9 |

## Repository structure

```
zap-scraping/
├── data/
│   ├── scrape/          # Raw scraped Parquet files + band checkpoints
│   ├── geocode/         # Geocoded Parquet files
│   ├── gpkg/            # GeoPackage files for GIS
│   └── .geocache.json   # Nominatim geocoding cache
├── src/
│   ├── scraper.py       # ZAP Imóveis scraper (Selenium)
│   ├── geocode.py       # Nominatim geocoder + H3 indexing
│   └── export.py        # GeoPackage exporter (GeoPandas)
├── run.py               # Main pipeline script
├── config.json          # Local configuration (gitignored)
└── config.example.json  # Configuration template
```

## Requirements

- Python 3.11+
- Google Chrome (macOS; ARM64 chromedriver is downloaded automatically)
- Dependencies: `selenium`, `undetected-chromedriver`, `beautifulsoup4`, `pandas`, `geopy`, `h3`, `geopandas`, `shapely`, `pytz`
