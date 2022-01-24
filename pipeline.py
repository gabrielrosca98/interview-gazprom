import os
import datetime
from pathlib import Path
import sqlite3
import pandas as pd
import logging
import re
import traceback

logging.basicConfig(format='%(asctime)s [%(levelname)s] (%(name)s) %(msg)s', level=logging.INFO)
APP_ROOT = Path(__file__).parent.absolute()
RECORD_IDENTIFIERS = ["HEADR", "CONSU", "TRAIL"]


class Pipeline:
    def __init__(self):
        self.db_connection = sqlite3.connect("smrt_db.db", isolation_level=None, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
        self.db_connection.row_factory = sqlite3.Row
        self.db_cursor = self.db_connection.cursor()
        self.logger = logging.getLogger(__name__)

    def submit_metadata(self, file_name: str, updated_time: datetime, status: str, _metadata: dict):
        self.db_cursor.execute("INSERT INTO _metadata VALUES (?, ?, ?, ?, strftime('%Y-%m-%d %H:%M:%S', datetime('now')), ?)", (file_name,
                                                                                   _metadata['generation_number'],
                                                                                   _metadata['creation_datetime'],
                                                                                   updated_time,
                                                                                   status)
                               )

    @staticmethod
    def check_file(file_data: pd.DataFrame) -> pd.DataFrame:
        # Check RECORD IDENTIFIERS
        if not set(RECORD_IDENTIFIERS) == set(file_data[0]):
            raise ValueError(f"Record identifier mismatch. Please check file.")

        # Verify RECORD IDENTIFIER position in file
        if file_data[0].tail(1).item() != "TRAIL" or file_data[0].head(1).item() != "HEADR":
            raise ValueError(f"Format mismatch. Please check file.")

        # Verify file generation number format
        re_pattern = re.compile(r"\b(PN|DV)[0-9]{6}\b")
        result = re_pattern.search(file_data.iloc[0][5])
        if not result:
            raise ValueError(f"Wrong file generation number format.")

        # Remove Record identifiers, Header, Trail
        file_data = file_data.iloc[1:-1, 1:-1]

        # Add column names
        file_data.columns = ["METER_NUMBER", "MEASUREMENT_DATE", "MEASUREMENT_TIME", "CONSUMPTION"]

        # Check date and time columns
        try:
            [datetime.datetime.strptime(_, '%Y%m%d') for _ in file_data.MEASUREMENT_DATE]
            [datetime.datetime.strptime(_, '%H%M') for _ in file_data.MEASUREMENT_TIME]
        except ValueError as ex:
            raise ValueError(ex)

        # Generate new datetime column
        x = [datetime.datetime.strptime(f"{_['MEASUREMENT_DATE']} {_['MEASUREMENT_TIME']}", "%Y%m%d %H%M") for index, _ in file_data.iterrows()]
        file_data.insert(loc = 4, column='MEASUREMENT_DATETIME', value=x)

        # Remove any rows with negative readings
        file_data = file_data[file_data['CONSUMPTION'].astype(float) >= 0]

        return file_data

    @staticmethod
    def extract_metadata(file_data) -> dict:
        _metadata = {"generation_number": None, "creation_datetime": None, "company": None}
        try:
            _metadata['generation_number'] = file_data.iloc[0][5]
            _metadata['creation_datetime'] = datetime.datetime.strptime(f"{file_data.iloc[0][3]} {file_data.iloc[0][4]}", "%Y%m%d %H%M%S")
            _metadata['company'] = file_data.iloc[0][2]
        except Exception as ex:
            raise ValueError(f"Error extracting metadata from file.")
        return _metadata

    def process_file(self, file_name: str) -> dict:
        file_data = pd.read_csv(APP_ROOT / "Data" / file_name, header=None, dtype=str)

        _metadata = self.extract_metadata(file_data)

        file_df = self.check_file(file_data)

        file_df.insert(loc = 0, column='COMPANY_ID', value=_metadata['company'])
        file_df.insert(loc = 6, column='FILE_NUMBER', value=_metadata['generation_number'])

        # Clean staging table
        self.logger.info("Cleaning staging table.")
        self.db_cursor.execute("delete from smrt_readings_staging where 1=1;")

        # Insert rows
        self.logger.info("Inserting data into staging table.")
        for _, row in file_df.iterrows():
            row['MEASUREMENT_DATETIME'] = row['MEASUREMENT_DATETIME'].to_pydatetime()
            self.db_cursor.execute("INSERT INTO smrt_readings_staging "
                                   "(company_id, meter_number, measurement_date, "
                                   "measurement_time, consumption, measurement_datetime, "
                                   "generation_number, last_updated)"
                                   "VALUES (?, ?, ?, ?, ?, ?, ?, strftime('%Y-%m-%d %H:%M:%S', datetime('now')))"
                                   "ON CONFLICT(company_id, meter_number, measurement_date, measurement_time) "
                                   "DO UPDATE SET "
                                   "consumption=excluded.consumption, last_updated=excluded.last_updated", row)

        # Merge staging
        self.logger.info("Merging staging table into final table.")
        self.db_cursor.execute("insert into smrt_readings select * from smrt_readings_staging where true on conflict "
                               "(company_id, meter_number, measurement_date, measurement_time) "
                               "do update set consumption=excluded.consumption, last_updated=excluded.last_updated;")

        return _metadata

    def run(self):
        self.logger.info("Starting pipeline.")

        list_files = [(item, os.path.getmtime(APP_ROOT / "Data" / item)) for item in os.listdir(APP_ROOT / "Data") if Path(APP_ROOT / "Data" / item).is_file()]
        list_files.sort(key=lambda val: val[1])
        list_files = [(val[0], datetime.datetime.fromtimestamp(val[1])) for val in list_files]

        for _ in list_files:
            # Check if the file has been processed
            if not self.db_cursor.execute("SELECT EXISTS(SELECT * FROM _metadata WHERE file_name = ?) as processed", [_[0]]).fetchone()['processed']:
                try:
                    self.logger.info(f"Processing file: {_[0]}")
                    _metadata = self.process_file(_[0])
                    status = "SUCCESS"
                except Exception as ex:
                    self.logger.warning(traceback.format_exc())
                    status = "FAILED"
                finally:
                    self.logger.info(f"Submitting metadata entry for file {_[0]} with status: {status}")
                    self.submit_metadata(_[0], _[1], status, _metadata)
            else:
                self.logger.info(f"File {_[0]} already has been already loaded.")
                continue


def main():
    pipeline = Pipeline()
    pipeline.run()


if __name__ == '__main__':
    main()
