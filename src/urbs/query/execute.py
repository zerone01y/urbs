import pathlib
import sqlite3 as sql

from loguru import logger

def export_results(result_dir, queries="urbs/query"):
    import csv

    with sql.connect(str(result_dir / "data.db")) as conn:
        logger.debug(f"Connect to database {str(result_dir / 'data.db')}")
        for file in pathlib.Path(queries).glob("*view_*.sql"):
            command = file.read_text()
            cursor = conn.cursor()
            try:
                cursor.execute(command)
                logger.info(f"Query execution finish: {file.stem}.")

            except sql.OperationalError as e:
                logger.warning(f"Query execution of {file.stem} failed.")
                continue
        for file in pathlib.Path(queries).glob("*table_*.sql"):
            command = file.read_text()
            cursor = conn.cursor()
            try:
                cursor.execute(command)
                logger.info(f"Query execution finish: {file.stem}.")

            except:
                logger.warning(f"Query execution of {file.stem} failed.")
                continue
            header = [head[0] for head in cursor.description]

            data = cursor.fetchall()
            with open(result_dir / (file.stem.replace("table_", "") + ".csv")
                    , "w", newline="") as f_handle:
                writer = csv.writer(f_handle)
                # Add the header/column names
                writer.writerow(header)
                # Iterate over `data`  and  write to the csv file
                for row in data:
                    writer.writerow(row)
