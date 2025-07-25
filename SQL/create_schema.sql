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
);



