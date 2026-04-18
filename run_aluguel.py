import warnings
warnings.filterwarnings("ignore")

from src.scraper import main as run_scraper
from src.geocode import main as run_geocode


def main():
    scrape_path = run_scraper("aluguel")
    if not scrape_path:
        print("\n[!] Scraping produced no data — stopping.")
        return

    run_geocode(str(scrape_path))


if __name__ == "__main__":
    main()
