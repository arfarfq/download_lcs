import logging
import os
import pandas as pd
import ast
import time
from tqdm import tqdm
import lightkurve as lk
import sqlite3
import multiprocessing

# Set up logging
logging.basicConfig(
    filename='processing.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

TEST_MODE = False  # Set to False for full processing
TEST_LIMIT = 10 if TEST_MODE else None
SSD_CACHE_DIR = "/Users/samirhaffegee/TCEs_LCs"  # Replace with your SSD path
MAX_WORKERS = 8

# Configure lightkurve cache directory to SSD
lk.conf.cache_dir = SSD_CACHE_DIR
if not os.path.exists(SSD_CACHE_DIR):
    os.makedirs(SSD_CACHE_DIR)
logging.info(f"Lightkurve cache set to {SSD_CACHE_DIR}")

def get_exo_tic_sectors():
    """Load TIC IDs and sectors of exoplanet hosts from CSV file."""
    try:
        df = pd.read_csv("tces.csv")
        tic_sectors = list(zip(df['tic_id'], df['Sectors']))
        tic_sectors = [(tic, sectors) for tic, sectors in tic_sectors]
        return tic_sectors
    except Exception as e:
        logging.error(f"Error loading exo TIC sectors: {e}", exc_info=True)
        return []

def download_tess_data(tic, sector, max_retries=3):
    for attempt in range(max_retries):
        try:
            search = lk.search_lightcurve(f"TIC {tic}", sector=sector)
            if len(search) == 0:
                logging.info(f"No data found for TIC {tic} sector {sector}")
                return None
            lc = search.download()
            logging.info(f"Downloaded TIC {tic} sector {sector} to cache")
            return lc
        except (ConnectionError, TimeoutError) as e:
            logging.warning(f"Attempt {attempt+1}/{max_retries} failed for TIC {tic} sector {sector}: {e}")
            if attempt < max_retries - 1:
                sleep_time = 3 * (attempt + 1)
                time.sleep(sleep_time)
        except Exception as e:
            logging.error(f"Permanent error for TIC {tic} sector {sector}: {e}")
            return None
    logging.error(f"All download attempts failed for TIC {tic} sector {sector}")
    return None

def worker(task):
    """Worker function to download light curve and return result."""
    tic, sector = task
    try:
        lc = download_tess_data(tic, sector)
        if lc is not None:
            file_path = lc.filename
            return (tic, sector, file_path)
        return None
    except Exception as e:
        logging.error(f"Error in worker for TIC {tic} sector {sector}: {e}")
        return None

def main():
    try:
        # Set up SQLite database
        conn = sqlite3.connect('tce_database.db')
        cursor = conn.cursor()

        # Create LightCurves table with unique constraint on (TIC, sector)
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS LightCurves (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            TIC INTEGER,
            sector INTEGER,
            path_to_fits TEXT,
            UNIQUE (TIC, sector)
        )
        ''')
        conn.commit()

        # Load TOI features from "tois.csv" and populate TOIs table
        df_tois = pd.read_csv("tces.csv")
        features = [col for col in df_tois.columns if col != 'Sectors']
        df_tois = df_tois[features]
        df_tois.to_sql('TOIs', conn, if_exists='replace', index=False)

        # Load TIC and sector data
        if TEST_MODE:
            test_exo = get_exo_tic_sectors()[:TEST_LIMIT]  # Process only 10 in test mode
        else:
            test_exo = get_exo_tic_sectors()  # Full dataset

        # Prepare tasks for parallel processing
        tasks = [(tic, sectors) for tic, sectors in test_exo if sectors is not None]

        logging.info(f"Starting parallel processing of {len(tasks)} tasks with {MAX_WORKERS} workers...")

        # Use multiprocessing Pool with 8 workers
        with multiprocessing.Pool(processes=MAX_WORKERS) as pool:
            cursor.execute("BEGIN TRANSACTION")
            processed_count = 0

            # Process tasks in parallel, inserting results as they arrive
            for result in tqdm(pool.imap_unordered(worker, tasks), total=len(tasks), desc="Downloading Light Curves"):
                if result is not None:
                    tic, sector, file_path = result
                    cursor.execute("""
                        INSERT INTO LightCurves (TIC, sector, path_to_fits) VALUES (?, ?, ?)
                        ON CONFLICT(TIC, sector) DO UPDATE SET path_to_fits = excluded.path_to_fits
                    """, (tic, sector, file_path))
                    processed_count += 1

                    # Commit every 100 inserts to balance speed and data safety
                    if processed_count % 100 == 0:
                        conn.commit()
                        cursor.execute("BEGIN TRANSACTION")
                        logging.info(f"Processed {processed_count} light curves")

            # Commit any remaining inserts
            conn.commit()
            logging.info(f"Completed processing. Total processed: {processed_count}")

    except KeyboardInterrupt:
        logging.info("Keyboard interrupt detected. Progress saved in cache and partial database commits.")
    except Exception as e:
        logging.error(f"Unexpected error in main: {e}", exc_info=True)
    finally:
        conn.close()

if __name__ == "__main__":
    main()