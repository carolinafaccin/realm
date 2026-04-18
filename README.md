![Status](https://img.shields.io/badge/status-planned-yellow.svg)

# REALM
**REALM** - *Real Estate Data Location Mining* - is a web scraping project designed to extract and analyze real estate data from leading Brazilian platforms.

This project is a practical application of data collection and processing for urban analysis, focusing on the Brazilian real estate market. 

The goal is to build a robust dataset for future projects, including predictive modeling of property prices and spatial analysis of market trends.

## Project Goals

- **Data Collection**: Develop a flexible web scraper to extract real estate listings, including key attributes like price, location, size, number of rooms, and building features.

- **Data Structuring**: Organize the scraped data into a clean, structured format (e.g., CSV or GeoJSON) suitable for analysis.

- **Initial Analysis**: Conduct a preliminary analysis to identify key market trends and prepare a dataset for future predictive models.


## Outputs

A sample of the collected data, including the first few rows of the scraped dataset:

|Price|Bedrooms|Size (m²)|Location|
|---|---|---|---|
|R$ 500,000|2|70|Porto Alegre, RS|
|R$ 850,000|3|120|São Paulo, SP|
|R$ 320,000|1|45|Santa Cruz do Sul, RS|

## Running the Project

Clone the repository and run the main script to start scraping: `python3 src/main.py`

## Repository Structure

```
ZAP/
├── data/             # Raw and processed data files (add to .gitignore)
│   ├── raw/
│   └── processed/
├── notebooks/        # Jupyter notebooks for data analysis
├── src/              # Source code for the web scraper
├── .gitignore
└── README.md
```

## Future Work

- [ ] Implement a function to handle dynamic content (e.g., listings loaded via JavaScript).
    
- [ ] Add support for other real estate platforms (e.g., QuintoAndar, OLX).
    
- [ ] Develop a machine learning model to predict property prices.
    
- [ ] Create an interactive dashboard with **Streamlit** to visualize urban real estate trends.
