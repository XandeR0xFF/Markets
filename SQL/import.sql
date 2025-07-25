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
	
--DELETE FROM imports;
--VACUUM;