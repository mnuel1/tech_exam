import os
import time
import pandas as pd
import shutil
import logging
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from pymongo import errors
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

from config.db import uri, DB_NAME, COLLECTION_NAME

# Configure logging
logging.basicConfig(filename="file_watcher.log", level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Directory Paths
WATCH_DIR = "storage/app/medalists/"
ARCHIVE_DIR = "storage/app/medalists/archive/"
os.makedirs(ARCHIVE_DIR, exist_ok=True) # make sure folder exist

class CSVHandler(FileSystemEventHandler):
        
    def __init__(self):

        """ initiate database """
        self.client = MongoClient(uri, server_api=ServerApi('1'))        
        self.db = self.client[DB_NAME]
        self.collection = self.db[COLLECTION_NAME]

        """ create indexes for duplicates medal_code """
        self.ensure_indexes()

        """ check ping for checking database connection """
        self.ping() 

    """ ping connection """
    def ping(self):        
        try:                        
            print("Connection successfully established.")
        except Exception as e:            
            print(f"❌ Failed to connect to MongoDB Atlas: {str(e)}")
        
    def ensure_indexes(self):
        """Create an index on code_athlete to prevent duplicates."""
        self.collection.create_index([("code_athlete", 1)], unique=True)

    def on_created(self, event):
        """Triggered when a new file is created in the watch directory."""
        if event.is_directory:
            return
        
        file_path = event.src_path
        if file_path.endswith(".csv"):
            logging.info(f"New CSV file detected: {file_path}")
            self.process_csv(file_path)

    def process_csv(self, file_path):
        """Parses and inserts CSV data into MongoDB."""
        try:
            df = pd.read_csv(file_path)

            """ 
                Validate required columns 
                assuming these fields (team, team_gender, url_event, code_team)
                are not required because some of fields in the medalist.csv 
                are empty.
            """
            
            required_columns = {"medal_date","medal_type","medal_code","name","gender","country_code",
                                "country","country_long",
                                "nationality","discipline","event",
                                "event_type","birth_date","code_athlete",}
            
            if not required_columns.issubset(df.columns):
                logging.error(f"Missing required columns in {file_path}")
                return

            records = df.to_dict(orient="records")

            """ 
                Get only the unique records based on the code_athlete 
                assuming that the code_athlete is an unique identifier
            """
            
            unique_records = {record["code_athlete"]: record for record in records}.values()
                        
            """ Prepare bulk insert operations with the unique records """
            bulk_operations = []
            for record in unique_records:
                bulk_operations.append(record)

            if bulk_operations:
                try:
                    self.collection.insert_many(bulk_operations, ordered=False)                    
                except errors.BulkWriteError as e:
                    logging.warning(f"Error during bulk insert for {file_path}: {str(e)}")
            else : 
                logging.info(f"Not added {len(bulk_operations)} records from {file_path}")

            logging.info(f"Successfully processed {len(bulk_operations)} records from {file_path}")

            """ Move file to archive after successful processing """
            shutil.move(file_path, os.path.join(ARCHIVE_DIR, os.path.basename(file_path)))
            logging.info(f"Moved {file_path} to archive.")

        except Exception as e:
            logging.error(f"Error processing {file_path}: {str(e)}")


def run_service():      
    """ Observer to watch any changes in the WATCH_DIR"""
    event_handler = CSVHandler()
    observer = Observer()
    observer.schedule(event_handler, WATCH_DIR, recursive=False)
    observer.start()
    
    try:
        """ Keeps the service alive """
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        observer.stop()
    
    observer.join()

if __name__ == "__main__":
    run_service()
