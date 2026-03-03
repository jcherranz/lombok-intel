"""Main orchestrator — runs the full Lombok Intel pipeline locally.

Usage:
    python main.py              # Run full pipeline (scrape + analyze)
    python main.py --scrape     # Scrape only
    python main.py --analyze    # Analyze only (skip scraping)
    python main.py --dashboard  # Launch the Streamlit dashboard
"""

import argparse
import sys
import subprocess
from pathlib import Path

from src.db.init_db import init_database, DB_PATH
from src.utils import setup_logger

logger = setup_logger("main")


def run_scrapers():
    """Run both scrapers sequentially."""
    logger.info("=== Starting data collection ===")

    try:
        logger.info("Running Airbnb scraper...")
        from src.scrapers.airbnb_scraper import AirbnbScraper
        airbnb = AirbnbScraper()
        airbnb.run()
        logger.info("Airbnb scraper completed.")
    except Exception as e:
        logger.error(f"Airbnb scraper failed: {e}")

    try:
        logger.info("Running Booking.com scraper...")
        from src.scrapers.booking_scraper import BookingScraper
        booking = BookingScraper()
        booking.run()
        logger.info("Booking.com scraper completed.")
    except Exception as e:
        logger.error(f"Booking.com scraper failed: {e}")


def run_analysis():
    """Run occupancy inference and ADR calculations."""
    logger.info("=== Starting analysis pipeline ===")

    try:
        logger.info("Running occupancy inference...")
        from src.pipeline.occupancy_engine import OccupancyEngine
        engine = OccupancyEngine()
        engine.run()
        logger.info("Occupancy inference completed.")
    except Exception as e:
        logger.error(f"Occupancy inference failed: {e}")

    try:
        logger.info("Running ADR calculator...")
        from src.pipeline.adr_calculator import ADRCalculator
        calc = ADRCalculator()
        calc.run()
        logger.info("ADR calculation completed.")
    except Exception as e:
        logger.error(f"ADR calculator failed: {e}")


def launch_dashboard():
    """Launch the Streamlit dashboard."""
    app_path = Path(__file__).parent / "src" / "dashboard" / "app.py"
    logger.info(f"Launching dashboard: streamlit run {app_path}")
    subprocess.run(["streamlit", "run", str(app_path)], check=True)


def main():
    parser = argparse.ArgumentParser(description="Lombok Market Intelligence Pipeline")
    parser.add_argument("--scrape", action="store_true", help="Run scrapers only")
    parser.add_argument("--analyze", action="store_true", help="Run analysis only")
    parser.add_argument("--dashboard", action="store_true", help="Launch dashboard")
    args = parser.parse_args()

    # Initialize database
    logger.info(f"Database: {DB_PATH}")
    init_database()

    if args.dashboard:
        launch_dashboard()
        return

    if args.scrape:
        run_scrapers()
    elif args.analyze:
        run_analysis()
    else:
        # Full pipeline
        run_scrapers()
        run_analysis()

    # Export Excel snapshot
    try:
        from src.export_excel import export
        logger.info("Exporting Excel snapshot...")
        export()
    except Exception as e:
        logger.error(f"Excel export failed: {e}")

    logger.info("=== Pipeline complete ===")


if __name__ == "__main__":
    main()
