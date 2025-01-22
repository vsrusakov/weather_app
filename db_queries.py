create_city_forecasts = """
CREATE TABLE IF NOT EXISTS city_forecasts
( 
  city TEXT UNIQUE NOT NULL,
  lat REAL NOT NULL,
  lon REAL NOT NULL,
  forecast BLOB
)
"""

insert_city_forecasts = """
INSERT INTO city_forecasts (city, lat, lon, forecast)
 VALUES (?, ?, ?, ?)
"""

select_coords = """
SELECT lat, lon, forecast, ROWID FROM city_forecasts
"""

update_forecasts = """
UPDATE city_forecasts
SET forecast = ?
WHERE ROWID = ?
"""

select_cities = """
SELECT city, lat, lon FROM city_forecasts
"""

city_coords = """
SELECT lat, lon FROM city_forecasts
WHERE city = ?
"""

db_queries = {
    'create_city_forecasts': create_city_forecasts,
    'insert_city_forecasts': insert_city_forecasts,
    'select_coords': select_coords,
    'update_forecasts': update_forecasts,
    'select_cities': select_cities,
    'city_coords': city_coords,
}
