'''
Demonstrates how to use a DuckDB database
'''

import os
import csv
import duckdb

def convert_entries(csv_input):
    '''
    Attempt to convert data from a CSV file into proper type for
    database. Return unaltered input if there's a problem
    '''
    try:
        csv_input[0] = int(csv_input[0])
        csv_input[5] = int(csv_input[5])
        csv_input[6] = int(csv_input[6])
        csv_input[7] = int(csv_input[7])
        csv_input[8] = float(csv_input[8])
        csv_input[9] = float(csv_input[9])
        csv_input[10] = float(csv_input[10])
        csv_input[11] = float(csv_input[11])
        csv_input[12] = float(csv_input[12])
        csv_input[13] = float(csv_input[13])
        csv_input[14] = float(csv_input[14])
        return csv_input
    except Exception as e:
        print(f"There was an error during conversion: {e}")
        return None

def populate_database(database, file_reader, columns):
    '''
    Create and fill a table for the database with data from a CSV file
    '''
    # Create database table
    ingest_data_sql = f'CREATE SEQUENCE id_sequence START 1;' \
                        f'CREATE TABLE IF NOT EXISTS elevation_data (' \
                        f'id INTEGER PRIMARY KEY DEFAULT nextval(\'id_sequence\'), ' \
                        f'{columns[0]} BIGINT, ' \
                        f'{columns[1]} TEXT, ' \
                        f'{columns[2]} TEXT, ' \
                        f'{columns[3]} TEXT, ' \
                        f'{columns[4]} TEXT, ' \
                        f'{columns[5]} INT, ' \
                        f'{columns[6]} INT, ' \
                        f'{columns[7]} INT, ' \
                        f'{columns[8]} REAL, ' \
                        f'{columns[9]} REAL, ' \
                        f'{columns[10]} REAL, ' \
                        f'{columns[11]} REAL, ' \
                        f'{columns[12]} REAL, ' \
                        f'{columns[13]} REAL, ' \
                        f'{columns[14]} REAL, ' \
                    ');'
    database.sql(ingest_data_sql)

    # Fill the table with values from file
    columns_insert = ', '.join(columns)
    line_num = 0
    for line in file_reader:
        if line_num != 0: # Skip header
            formated_data = convert_entries(line)
            if formated_data != None:
                # List out column titles and column values (for current line)
                current_row_sql = f"INSERT INTO elevation_data ({columns_insert}) " \
                                f"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"
                database.sql(current_row_sql, params=formated_data)
            else:
                print("Stopped ingest")
                return
        line_num += 1

def database_exists():
    '''
    Returns True if the elevation_data database already exists
    '''
    return os.path.exists("./elevation_data.db")

def main():
    '''
    Create database and ingest CSV file data
    '''
    if not database_exists():
        try:
            # Create connection to database
            with duckdb.connect("elevation_data.db") as db:
                # Injest data
                elevation_file = open("../data/atchaf_20210912_AirSWOT_L3.csv")
                reader = csv.reader(elevation_file, delimiter=",")

                # Get the column names
                columns = [
                    "flight_line_id",
                    "basin_id",
                    "channel_id",
                    "date",
                    "time",
                    "utc_time",
                    "CoordX",
                    "CoordY",
                    "longitude",
                    "latitude",
                    "channel_dist",
                    "water_surface_elevation_WGS84",
                    "water_surface_elevation_NAVD88",
                    "water_surface_elevation_uncertainty",
                    "water_area"
                ]

                # Fill database with data from file
                populate_database(db, reader, columns)
                print("Ingest complete")
        except Exception as e:
            print(f"There was a problem with the database connection, file, or ingest: {e}")
    else:
        print("Ingest already performed. Database exists")


if __name__ == "__main__":
    main()