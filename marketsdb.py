import csv
import sqlite3
import os

def load_sql_script(script_name: str) ->str:
    script_path = os.path.join("SQL", script_name)
    with open(script_path) as script_file:
        sql = script_file.read()
        return sql


def create_database(fname: str):
    if os.path.isfile(fname):
        os.remove(fname)
    with sqlite3.connect(fname) as connection:
        sql_create_schema = load_sql_script("create_schema.sql")
        connection.executescript(sql_create_schema)


def import_from_csv(csv_fname: str, target_db_fname: str):
    with open(csv_fname) as csv_file, sqlite3.connect(target_db_fname) as connection:
        reader = csv.reader(csv_file)
        header = next(reader)
        categories = header[28:58]
        for category in categories:
            connection.execute("INSERT INTO categories (category) VALUES(?)" , [category.strip()])

        for line in reader:
            raw_row = list(None if x.strip() == "" else x.strip() for x in line)
            markets = [raw_row[0], raw_row[1], raw_row[7], raw_row[8], raw_row[10], raw_row[11], raw_row[20], raw_row[21]]

            category_flags = raw_row[28:58]
            market_categories = list()
            for i in range(len(categories)):
                if category_flags[i] == "Y": market_categories.append(categories[i])

            market_categories_string = "|".join(market_categories)
            markets.append(market_categories_string)
            connection.execute("INSERT INTO imports (id, market_name, street, city, state, zip, x, y, categories) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)", markets)

        connection.commit()

        sql_import = load_sql_script("import.sql")
        connection.executescript(sql_import)

create_database("markets.db")
import_from_csv("Export.csv", "markets.db")