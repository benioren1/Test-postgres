create_table_mission = '''
        CREATE TABLE IF NOT EXISTS mission (
                         mission_id SERIAL PRIMARY KEY,                 -- Mission ID, auto-incremented primary key
                         mission_date DATE,                             -- Mission Date, a date field
                         theater_of_operations VARCHAR(100),            -- Theater of Operations, assuming text data
                         country VARCHAR(100),                          -- Country, assuming text data
                         air_force VARCHAR(100),                        -- Air Force, assuming text data
                         unit_id VARCHAR(100),                          -- Unit ID, assuming text data
                         aircraft_series VARCHAR(100),                  -- Aircraft Series, assuming text data
                         callsign VARCHAR(100),                         -- Callsign, assuming text data
                         mission_type VARCHAR(100),                     -- Mission Type, assuming text data
                         takeoff_base VARCHAR(255),                     -- Takeoff Base, assuming larger text data
                         takeoff_location VARCHAR(255),                 -- Takeoff Location, assuming larger text data
                         takeoff_latitude VARCHAR(15),               -- Takeoff Latitude, assuming GPS latitude
                         takeoff_longitude NUMERIC(10, 6),              -- Takeoff Longitude, assuming GPS longitude
                         target_key VARCHAR(100),   
                         target_id integer REFERENCES targets(target_id), -- Foreign key referencing targets table, 
                         altitude_hundreds_of_feet NUMERIC(7, 2),             -- Altitude in hundreds of feet, assuming numerical data
                         airborne_aircraft NUMERIC(4, 1),                     -- Airborne Aircraft, assuming numerical data
                         attacking_aircraft INTEGER,                    -- Attacking Aircraft, assuming numerical data
                         bombing_aircraft INTEGER,                      -- Bombing Aircraft, assuming numerical data
                         aircraft_returned INTEGER,                     -- Aircraft Returned, assuming numerical data
                         aircraft_failed INTEGER,                       -- Aircraft Failed, assuming numerical data
                         aircraft_damaged INTEGER,                      -- Aircraft Damaged, assuming numerical data
                         aircraft_lost INTEGER,                         -- Aircraft Lost, assuming numerical data
                         high_explosives VARCHAR(255),                  -- High Explosives, assuming text
                         high_explosives_type VARCHAR(255),             -- High Explosives Type, assuming text data
                         high_explosives_weight_pounds VARCHAR(25),  -- High Explosives Weight in Pounds, assuming decimal data
                         high_explosives_weight_tons NUMERIC(10, 2),    -- High Explosives Weight in Tons, assuming decimal data
                         incendiary_devices VARCHAR(255),               -- Incendiary Devices, assuming text data
                         incendiary_devices_type VARCHAR(255),          -- Incendiary Devices Type, assuming text data
                         incendiary_devices_weight_pounds NUMERIC(10, 2), -- Incendiary Devices Weight in Pounds, assuming decimal data
                         incendiary_devices_weight_tons NUMERIC(10, 2),   -- Incendiary Devices Weight in Tons, assuming decimal data
                         fragmentation_devices VARCHAR(255),            -- Fragmentation Devices, assuming text data
                         fragmentation_devices_type VARCHAR(255),       -- Fragmentation Devices Type, assuming text data
                         fragmentation_devices_weight_pounds NUMERIC(10, 2), -- Fragmentation Devices Weight in Pounds, assuming decimal data
                         fragmentation_devices_weight_tons NUMERIC(10, 2),   -- Fragmentation Devices Weight in Tons, assuming decimal data
                         total_weight_pounds NUMERIC(10, 2),            -- Total Weight in Pounds, assuming decimal data
                         total_weight_tons NUMERIC(10, 2),              -- Total Weight in Tons, assuming decimal data
                         time_over_target VARCHAR(8),                         -- Time Over Target, assuming time data
                         bomb_damage_assessment VARCHAR(255),           -- Bomb Damage Assessment, assuming text data
                         source_id VARCHAR(100)                         -- Source ID, assuming text or unique identifier
);




        '''

create_table_country = '''
                   CREATE TABLE country (
                       country_id SERIAL PRIMARY KEY,
                       country_name VARCHAR(100)
                   );
               '''

create_table_city = '''
                   CREATE TABLE city (
                       city_id SERIAL PRIMARY KEY,
                       city_name VARCHAR(100),
                       city_country INTEGER REFERENCES country(country_id)
                   );
               '''

create_table_target_location = '''
                   CREATE TABLE target_location (
                       loc_id SERIAL PRIMARY KEY,
                       target_city INTEGER REFERENCES city(city_id),
                       target_latitude NUMERIC(10, 6),
                       target_longitude NUMERIC(10, 6)
                   );
               '''

create_table_targets = '''
                   CREATE TABLE targets (
                       target_id SERIAL PRIMARY KEY,
                       target_key int,
                       target_type VARCHAR(100),
                       target_industry VARCHAR(255),
                       target_priority VARCHAR(5),
                       loc_id INTEGER REFERENCES target_location(loc_id)
                   );
               '''