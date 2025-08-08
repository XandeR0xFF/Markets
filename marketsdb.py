import csv
import sqlite3
import os
import math

sql_create_schema = """
    CREATE TABLE imports (
	    id INTEGER NOT NULL,
	    market_name TEXT NOT NULL,
	    street TEXT,
	    city TEXT,
	    state TEXT,
	    zip INTEGER,
	    x REAL,
	    y REAL,
	    categories TEXT,

	    PRIMARY KEY (id)
    );

    CREATE TABLE categories (
	    id INTEGER NOT NULL,
	    category TEXT NOT NULL,
	
	    PRIMARY KEY (id),
	    UNIQUE (category)
    );

    CREATE TABLE states (
	    id INTEGER NOT NULL,
	    state TEXT NOT NULL,
	
	    PRIMARY KEY (id),
	    UNIQUE(state)
    );

    CREATE TABLE cities (
	    id INTEGER NOT NULL,
	    city TEXT NOT NULL,
	    state_id INTEGER NOT NULL,
	
	    PRIMARY KEY (id),
	    UNIQUE(city, state_id),
	    FOREIGN KEY (state_id) REFERENCES states(id) ON UPDATE CASCADE ON DELETE CASCADE
    );

    CREATE TABLE markets (
	    id INTEGER NOT NULL,
	    market_name TEXT NOT NULL,
	    street TEXT,
	    city_id INTEGER,
	    zip INTEGER,
	    x REAL,
	    y REAL,
	
	    PRIMARY KEY (id),
	    FOREIGN KEY (city_id) REFERENCES cities(id) ON UPDATE CASCADE ON DELETE CASCADE
    );

    CREATE TABLE markets_categories (
	    id INTEGER NOT NULL,
	    market_id INTEGER NOT NULL,
	    category_id INTEGER NOT NULL,
	
	    PRIMARY KEY (id),
	    FOREIGN KEY (market_id) REFERENCES markets(id),
	    FOREIGN KEY (category_id) REFERENCES categories(id),
	    UNIQUE (market_id, category_id)
    );

    CREATE TABLE users (
	    id INTEGER NOT NULL,
	    first_name TEXT NOT NULL,
	    last_name TEXT NOT NULL,
	
	    PRIMARY KEY (id)
    );

    CREATE TABLE reviews (
	    id INTEGER NOT NULL,
	    market_id INTEGER NOT NULL,
	    user_id INTEGER NOT NULL,
	    rating INTEGER NOT NULL,
	    content TEXT,
	
	    PRIMARY KEY (id),
	    FOREIGN KEY (market_id) REFERENCES markets(id) ON UPDATE CASCADE ON DELETE CASCADE,
	    FOREIGN KEY (user_id) REFERENCES users(id) ON UPDATE CASCADE ON DELETE CASCADE,
	    CHECK (rating >= 1 AND rating <= 5)
    );"""

sql_import = """
    INSERT INTO states (state) 
	    SELECT DISTINCT state
	    FROM imports
	    WHERE state IS NOT NULL;
	
    INSERT INTO cities (city, state_id)
	    SELECT DISTINCT imp.city, st.id
	    FROM imports imp, states st 
	    WHERE imp.state = st.state AND imp.city IS NOT NULL;

    INSERT INTO markets (id, market_name, street, city_id, zip, x, y)
	    SELECT imp.id, imp.market_name, imp.street, c.id, imp.zip, imp.x, imp.y
	    FROM imports imp, cities c, states s
	    WHERE imp.city = c.city AND imp.state = s.state AND c.state_id = s.id;
	
    INSERT INTO markets_categories (market_id, category_id)
	    WITH RECURSIVE split(id, word, remainder) AS (
		    SELECT 
			    id, 
			    '',
			    imports.categories || '|'
		    FROM imports
		    WHERE categories IS NOT NULL
		    UNION ALL
		    SELECT 
			    id,
			    SUBSTR(remainder, 1, INSTR(remainder, '|') - 1),
			    SUBSTR(remainder, INSTR(remainder, '|') + 1)
		    FROM split
		    WHERE remainder != ''
	    )
	    SELECT 
		    sp.id, 
		    c.id as category_id
	    FROM split sp, categories c
	    WHERE sp.word != '' AND sp.word = c.category;  
	
    DELETE FROM imports;
    VACUUM;"""

def create_database(fname: str):
    if os.path.isfile(fname):
        os.remove(fname)
    with sqlite3.connect(fname) as connection:
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
        connection.executescript(sql_import)


def get_command(context = "") -> str:
    message = ["marketsdb"]
    if context and context.strip():
        message.append(f"[{context.strip()}]")
    message.append(">>>")
    return input("".join(message))


def details_menu(db_file, cursor):
    market_id = input("Введите Id рынка: ")

    cmd = ""
    while cmd != 'b':
        cursor.execute(
                """SELECT m.id, m.market_name, c.city, s.state, m.street, m.zip, m.x, m.y
                FROM markets m, cities c, states s
                WHERE m.city_id = c.id and c.state_id = s.id AND m.id = ?""", [market_id])
        market = cursor.fetchone()

        cursor.execute
        if market:
            reviews = cursor.execute(
                """SELECT r.content, r.rating, u.first_name, u.last_name
                FROM reviews r, users u
                WHERE r.user_id = u.id AND r.market_id = ?""", [market_id]).fetchall()

            categories = cursor.execute(
                """SELECT c.category
                FROM markets_categories mc, categories c 
                WHERE mc.market_id = ? AND mc.category_id = c.id""", [market_id]).fetchall()

            print(f"Название рынка:\t\t{market[1]}")
            print(f"ID:\t\t\t{market[0]}")
            print(f"Штат:\t\t\t{market[3]}")
            print(f"Город:\t\t\t{market[2]}")
            print(f"Адрес:\t\t\t{market[4]}")
            print(f"ZIP:\t\t\t{market[5]}")
            print(f"Координаты (Х, У):\t{market[6]}, {market[7]}")
            print(f"Категории товаров:\t{' '.join([str(item) for category in categories for item in category])}")

            print("--------Обзоры---------")
            for review in reviews:
                print(f"Автор: {review[2]} {review[3]}  Рейтинг: {review[1]}")
                print(f"{review[0]}")

        else:
            print(f"запись с id {market_id} не найдена.")
            input("Нажмите ENTER...")
            return

        print("\n\tr - добавить обзор\n\tb - в предыдущее меню")
        cmd = get_command(f"{db_file}/{market_id}")
        if cmd == "r":
            first_name = input("Имя: ")
            last_name = input("Фамилия: ")
            content = input("Отзыв: ")
            rating = 1
            while True:
                rating = input("Оценка от 1 до 5: ")
                try:
                    rating_num = float(rating)
                    if 1 <= rating_num <= 5:
                        break
                    else:
                        print("Оценка должна быть от 1 до 5!")
                except ValueError:
                    print("Ошибка: введите число!")

            user_id = cursor.execute("SELECT id FROM users WHERE first_name COLLATE NOCASE = ? AND last_name COLLATE NOCASE = ?", [first_name, last_name]).fetchone()

            if user_id is None:
                user_id = cursor.execute("SELECT COALESCE(max(id), 0) + 1 FROM users").fetchone()[0]
                cursor.execute("INSERT INTO users (id, first_name, last_name) VALUES (?, ?, ?)", [user_id, first_name, last_name])
            else:
                user_id = user_id[0]
    
            cursor.execute("INSERT INTO reviews (market_id, user_id, rating, content) VALUES (?, ?, ?, ?)", [market_id, user_id, rating, content] );
            cursor.connection.commit()

def markets_menu(db_file):
    with sqlite3.connect(db_file) as connection:
        page_size = 5
        page_number = 1
        markets_count = connection.execute("SELECT count(*) FROM markets").fetchone()[0];
        page_count = math.ceil(markets_count / page_size)

        cmd = ""
        while cmd != "b":
            print("\n***** Просмотр рынков *****\n")
            print(f"Страница {page_number} из {page_count}")
            offset = (page_number - 1) * page_size
            cursor = connection.cursor()
            cursor.execute(
                """SELECT m.id, m.market_name, c.city, s.state, m.street, m.zip
                FROM markets m, cities c, states s
                WHERE m.city_id = c.id and c.state_id = s.id
                ORDER BY market_name LIMIT ? OFFSET ?""", [page_size, offset])
            line_index = page_size * (page_number - 1);
            for row in cursor:
                line_index = line_index + 1
                print(f"{line_index}. {row[1]}")
                print(f"\tID: {row[0]}")
                print(f"\tАдрес: {row[3]}, {row[2]}, {row[4]} (ZIP: {row[5]})")
                print()
            print("\n\tn - следующая страница\n\tp - предыдущая страница\n\ts - выбрать рынок\n\tf - поиск по городу и штату\n\tfz - посиск по ZIP\n\td - удалить рынок\n\tb - в предыдущее меню")
            cmd = get_command(db_file)
            if cmd == "n" and (page_number + 1) <= page_count: page_number = page_number + 1
            if cmd == "p" and (page_number - 1) >= 1: page_number = page_number - 1
            if cmd == "fz":
                z = input("ZIP: ")
                cursor.execute(
                """SELECT m.id, m.market_name, c.city, s.state, m.street, m.zip
                FROM markets m, cities c, states s
                WHERE m.city_id = c.id AND c.state_id = s.id AND m.zip = ?""", [z])
                for row in cursor:
                    print(f"{row[1]}\tРейтинг: ")
                    print(f"\tID: {row[0]}")
                    print(f"\tАдрес: {row[3]}, {row[2]}, {row[4]} (ZIP: {row[5]})")
                input("Для продолжения нажмите ENTER...")
            if cmd == "f":
                city = input("Введите город: ")
                state = input("Введите штат: ")
                cursor.execute(
                """SELECT m.id, m.market_name, c.city, s.state, m.street, m.zip
                FROM markets m, cities c, states s
                WHERE m.city_id = c.id AND c.state_id = s.id AND c.city COLLATE NOCASE = ? AND s.state COLLATE NOCASE = ?""", [city, state])
                for row in cursor:
                    print(f"{row[1]}\tРейтинг: ")
                    print(f"\tID: {row[0]}")
                    print(f"\tАдрес: {row[3]}, {row[2]}, {row[4]} (ZIP: {row[5]})")
                input("Для продолжения нажмите ENTER...")
            if cmd == "d":
                market_id = input("Введите ID удаляемой записи: ")
                cursor.execute("DELETE FROM markets WHERE id = ?", [market_id])
                connection.commit()
                if cursor.rowcount > 0:
                    print("Запись удалена.")
                else:
                    print(f"запись с id {market_id} не найдена.")
                input("Нажмите ENTER...")
            if cmd == "s": details_menu(db_file, cursor)
        return cmd

def main_menu():
    cmd = ""
    while cmd != "quit":
        print("\n***** Главное меню *****\n")
        print("Введите команду\n\tcreate\t- создать новую БД из файла Export.csv\n\topen\t- открыть БД\n\tquit\t- выход")
        cmd = get_command()
        if cmd == "create":
            csv_file = "Export.csv"
            db_file = input("Введите путь к файлу БД: ")
            create_database(db_file)
            import_from_csv(csv_file, db_file)
            cmd = markets_menu(db_file)
        if cmd == "open":
            db_file = input("Введите путь к файлу БД: ")
            cmd = markets_menu(db_file)


main_menu()