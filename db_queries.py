create_city_forecasts = """
CREATE TABLE IF NOT EXISTS city_forecasts
( 
  city TEXT UNIQUE NOT NULL,
  lat REAL NOT NULL,
  lon REAL NOT NULL,
  precipitation REAL NOT NULL,
  temperature BLOB NOT NULL,
  wind_speed BLOB NOT NULL,
  humidity BLOB NOT NULL
)
"""

index_city_forecasts = """
CREATE INDEX IF NOT EXISTS idx_city_forecasts ON city_forecasts (city)
"""

insert_city_forecasts = """
INSERT INTO city_forecasts 
(city, lat, lon, precipitation, temperature, wind_speed, humidity)
 VALUES (?, ?, ?, ?, ?, ?, ?)
"""

select_rows = """
SELECT 
ROWID, lat, lon, precipitation, 
temperature, wind_speed, humidity 
FROM city_forecasts
"""

update_forecasts = """
UPDATE city_forecasts
SET {} = ?
WHERE ROWID = ?
"""

find_city = """
SELECT ROWID FROM city_forecasts
WHERE city = ?
"""

select_cities = """
SELECT city, lat, lon FROM city_forecasts
"""

city_row = """
SELECT city, lat, lon, {} FROM city_forecasts
WHERE city = ?
"""

db_queries = {
    'create_city_forecasts': create_city_forecasts,
    'index_city_forecasts': index_city_forecasts,
    'insert_city_forecasts': insert_city_forecasts,
    'select_rows': select_rows,
    'update_forecasts': update_forecasts,
    'select_cities': select_cities,
    'city_row': city_row,
    'find_city': find_city,
}
