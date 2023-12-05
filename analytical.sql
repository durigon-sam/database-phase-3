---------------------
-- Analytical Queries
---------------------

-----------------------
-- 1. rankForestSensors
-----------------------
CREATE OR REPLACE PROCEDURE rankForestSensors()
AS
$$
BEGIN
DROP VIEW IF EXISTS FOREST_RANKS;
CREATE VIEW FOREST_RANKS AS
    SELECT
        forest_id,
        name,
        count as sensor_count,
        RANK() OVER (ORDER BY count DESC) AS rank
    FROM getSensorCountsInForest();
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION getSensorCountsInForest()
RETURNS TABLE (
    forest_id integer,
    name    varchar(30),
    count   integer
              )
AS $$
DECLARE
    rec_forest arbor_db.FOREST%ROWTYPE;
    rec_curr_sensor arbor_db.SENSOR%ROWTYPE;
    curr_sensor_count integer;
BEGIN
    DROP TABLE IF EXISTS SENSOR_COUNTS;
    CREATE TEMPORARY TABLE SENSOR_COUNTS (
        forest_id integer,
        name    varchar(30),
        count   integer
    );

    FOR rec_forest IN SELECT * FROM arbor_db.FOREST LOOP
        curr_sensor_count := 0;
        FOR rec_curr_sensor in SELECT * FROM arbor_db.SENSOR LOOP
            IF ((rec_curr_sensor.x >= rec_forest.mbr_xmin AND rec_curr_sensor.x <= rec_forest.mbr_xmax)
                AND
                (rec_curr_sensor.y >= rec_forest.mbr_ymin AND rec_curr_sensor.y <= rec_forest.mbr_ymax)) THEN
                curr_sensor_count := curr_sensor_count + 1;
            END IF;
        END LOOP;

        INSERT INTO SENSOR_COUNTS VALUES (rec_forest.forest_no, rec_forest.name, curr_sensor_count);

    END LOOP;

    RETURN QUERY SELECT * FROM SENSOR_COUNTS;
END;
$$ LANGUAGE plpgsql;

-- function to loop through forest, then for each forest, loop through sensors and get a count.
--  insert forest, count into new table...O(N^2)

CALL rankForestSensors();

--------------------------
-- 2. habitableEnvironment
-- given (genus, epithet, k) 
-- find forests that have an average temp within +-5 degrees from the species ideal temp
-- for the past k years
----------------------
-- Approach:
-- * create function to get sensors within a given forest (getSensorsInForest(forest_id)) --> forests_sensors
-- * slice REPORTS into years, get one year slice
-- * iterate over forests_sensors, calculate average temp within the year slice
-- * if temp doesn't meet criteria, break and continue to next forest
-- * otherwise, continue to next year slice until kth slice
-- * if temp meets criteria at last slice, insert current forest into result table HABITABLE_FORESTS
INSERT INTO arbor_db.CLOCK VALUES(current_timestamp);

CREATE OR REPLACE FUNCTION habitableEnvironment(g varchar(30), e varchar(30), k integer)
RETURNS TABLE (forest_no integer, name varchar(30)) AS
$$
DECLARE
    ideal_temp real;
    rec_forest arbor_db.FOREST%ROWTYPE;
    time timestamp;
    year_avg_temp real;
    is_habitable boolean := true;
BEGIN
    -- create result table 
    DROP TABLE IF EXISTS HABITABLE_FORESTS;
    CREATE TEMPORARY TABLE HABITABLE_FORESTS (
        forest_no integer,
        name varchar(30)
    );
    -- get delta (ideal_temp)
    SELECT ideal_temperature FROM arbor_db.tree_species
    WHERE genus = $1 AND epithet = $2 -- $ notation is just the position of passed in arguments 
    INTO ideal_temp;

    -- iterate through forests, calculate average temp for past k years
    FOR rec_forest IN SELECT * FROM arbor_db.FOREST LOOP
        -- get current date
        SELECT synthetic_time FROM arbor_db.CLOCK
        INTO time;
        -- k years loop
        FOR counter IN 1..k LOOP
            -- subtract time by 1 year
            time := time - interval '1 year';
            -- RAISE NOTICE 'Year after subtraction of % years: %', counter, time;
            RAISE NOTICE 'Current interval % to %', time, time + interval '1 year';
            -- calculate average temp for this year slice
            SELECT (AVG(temperature)) AS average_temp FROM arbor_db.REPORT
            NATURAL JOIN getSensorsInForest(rec_forest.forest_no)
            WHERE report_time >= time AND report_time <= (time + interval '1 year')
            INTO year_avg_temp;
            IF (year_avg_temp IS NOT NULL) THEN
                RAISE NOTICE '%: (% - % deg.)', counter, rec_forest.forest_no, year_avg_temp;
            ELSE
                RAISE NOTICE '%: (% - % deg.)', counter, rec_forest.forest_no, year_avg_temp;
            end if;

            -- check if average temp doesn't meet habitability criteria
            IF (year_avg_temp IS NULL OR (year_avg_temp < ideal_temp - 5 OR year_avg_temp > ideal_temp + 5)) THEN
               is_habitable := false;
               EXIT WHEN is_habitable = false;
            end if;
        end loop;

        IF (is_habitable) THEN
            -- insert into result table
            INSERT INTO HABITABLE_FORESTS VALUES(rec_forest.forest_no, rec_forest.name);
            RAISE NOTICE 'This forest (%: %) is habitable for % % (ideal: % deg.)', rec_forest.forest_no, rec_forest.name, $1, $2, ideal_temp;
        ELSE
            RAISE NOTICE 'This forest (%: %) is NOT habitable for % % (ideal: % deg.)', rec_forest.forest_no, rec_forest.name, $1, $2, ideal_temp;
        end if;
        -- reset habitability 
        is_habitable = true;
    end loop;

    RETURN QUERY SELECT * FROM HABITABLE_FORESTS;
end;
$$ LANGUAGE plpgsql;

-- Returns a list of sensor_id for sensors located within the given forest
CREATE OR REPLACE FUNCTION getSensorsInForest(f_num integer)
RETURNS TABLE (
    sensor_id integer
              )
AS
$$
DECLARE
    forest arbor_db.FOREST%ROWTYPE;
    rec_sensor arbor_db.SENSOR%ROWTYPE;
BEGIN

    -- create result table
    DROP TABLE IF EXISTS FORESTS_SENSORS;
    CREATE TEMPORARY TABLE FORESTS_SENSORS (
        sensor_id integer
    );
    -- select passed in forest
    SELECT * FROM arbor_db.FOREST F
    WHERE F.forest_no = f_num
    INTO forest;
    -- get all sensors within forest
    FOR rec_sensor IN SELECT * FROM arbor_db.SENSOR LOOP
        IF ((rec_sensor.x >= forest.mbr_xmin AND rec_sensor.x <= forest.mbr_xmax)
            AND
            (rec_sensor.y >= forest.mbr_ymin AND rec_sensor.y <= forest.mbr_ymax)) THEN
            INSERT INTO FORESTS_SENSORS VALUES (rec_sensor.sensor_id);
        end if;
    end loop;
    RETURN QUERY SELECT * FROM FORESTS_SENSORS;
    END;

$$ LANGUAGE plpgsql;

-- Testing Habitable Environments
SELECT * FROM habitableEnvironment('Larix', 'Lyallii', 2);

-------------------------------------------------
-- Interesting cases for 2. habitableEnvironement
-- What if a forest has no sensors? This means there are no reports, meaning no temperature
-- What if a forest does have sensors, but no reports within the time interval? 
--      This means no recorded temp for the interval
-- For these cases, the average temp calculation returns NULL.
-- Should these be treated as A) Not habitable or B) Habitable?
-- In my current implementation, NULL is treated as NOT Habitable...
-------------------------------------------------

--------------------------
-- 3. topSensors
-- input parameters
-- months: number of months
-- k: number of sensors
---------------------------
DROP FUNCTION IF EXISTS topSensors(months integer, k integer);
DROP FUNCTION IF EXISTS retrieveTopSensorIds(x integer, k integer);
CREATE OR REPLACE FUNCTION retrieveTopSensorIds(x integer, k integer)
RETURNS TABLE (sensor_id integer, count bigint )
AS
$$
DECLARE
    time timestamp;
BEGIN
    -- get number of total days to go back in time
    SELECT synthetic_time FROM arbor_db.CLOCK INTO time;
    time := time - (x * interval '30 days');
    RAISE NOTICE '% to %', time, time + (x * interval '30 days');

    -- count reports ordered by sensor id where report_time is within x months
    RETURN QUERY (SELECT R.sensor_id, COUNT(*) FROM arbor_db.REPORT R
                    WHERE R.report_time >= time
                    GROUP BY R.sensor_id
                    ORDER BY COUNT(*) DESC
                    LIMIT k);

end;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION topSensors(months integer, k integer)
RETURNS TABLE (sensor_id integer, count bigint, last_charged timestamp,
                energy integer, last_read timestamp, x real, y real, maintainer_id char(9)
              )
AS
$$
DECLARE
BEGIN
    -- Creating a table with the full sensor schema and counts
    RETURN QUERY (SELECT * FROM retrieveTopSensorIds($1, $2) T
    NATURAL JOIN arbor_db.SENSOR S);
end;
$$ LANGUAGE plpgsql;
-- Testing topSensors(3 months, 5 sensors)
SELECT * FROM topSensors(3, 5);

--------------------------
-- 4. threeDegrees
-- input parameters
-- f1: forest_no of forest 1
-- f2: forest_no of forest 2

-- use the FOUND_IN table to find the species (genus, epithet) found in a given forest_no

-- possible approach:
-- First, check if there is a single hop between f1 and f2.
-- If not, then find a forest that has a species from f1 and count it as a hop (assign to f3)
-- check if f3 has a hop between itself and f2
-- if there is no hop between f3 and f2, find a forest with a shared species with f3, and a shared species with f2
-- if f4 does not exist, find a different f3 and try again
---------------------------
DROP FUNCTION IF EXISTS threeDegrees(f1 integer, f2 integer);
CREATE OR REPLACE FUNCTION threeDegrees(f1 integer, f2 integer)
-- if it needs to return a string use this
RETURNS VARCHAR
AS
$$
DECLARE
    -- Variables in here
    hop1 RECORD;
    hop2 RECORD;
    hop3 RECORD;

    temp1 arbor_db.FOUND_IN%ROWTYPE;
    temp2 arbor_db.FOUND_IN%ROWTYPE;
    temp3 arbor_db.FOUND_IN%ROWTYPE;
BEGIN

    -- find initial hop if it exists
    SELECT
        forest_1.forest_no as f1,
        f2 as f2,
        forest_1.genus as shared_genus,
        forest_1.epithet as shared_epithet
    FROM arbor_db.FOUND_IN forest_1
    WHERE forest_1.forest_no = f1
    AND EXISTS (
        SELECT *
        FROM arbor_db.FOUND_IN forest_2
        WHERE forest_2.forest_no = f2
        AND forest_1.genus = forest_2.genus
        AND forest_1.epithet = forest_2.epithet

    )
    INTO hop1;

    IF hop1 IS NOT NULL THEN -- return the one hop solution
        RETURN hop1.f1 || ' -> ' || hop1.f2 || ' with ' || hop1.shared_genus || ' and ' || hop1.shared_epithet;
    END IF;

    -- if initial hop does not exist, find a hop with a shared genus/epithet between f1 and f3, and f3 and f2
    SELECT *
    FROM arbor_db.found_in forest_1
    WHERE forest_1.forest_no = f1
    AND EXISTS(
        SELECT *
        FROM arbor_db.found_in forest_3
        WHERE forest_3.genus = forest_1.genus
        AND forest_3.epithet = forest_1.epithet
        -- verify a hop exists between 3 and 2
        AND EXISTS(
            SELECT *
            FROM arbor_db.found_in forest_2
            WHERE forest_2.forest_no = f2
            AND EXISTS(
                SELECT 1
                FROM arbor_db.found_in hop
                WHERE hop.forest_no = forest_3.forest_no
                AND hop.genus = forest_2.genus
                AND hop.epithet = forest_2.epithet
            )
        )
    )
    INTO temp1;

    -- RAISE NOTICE 'temp1 is % with % and %', temp1.forest_no, temp1.genus, temp1.epithet;

    IF temp1 IS NOT NULL THEN -- get the forestID of forest3 and print it
        SELECT *
        FROM arbor_db.found_in forest_3
        WHERE forest_3.genus = temp1.genus
        AND forest_3.epithet = temp1.epithet
        AND EXISTS(
            SELECT 1
            FROM arbor_db.found_in forest_2
            WHERE forest_2.forest_no = f2
            AND EXISTS(
                SELECT 1
                FROM arbor_db.found_in hop
                WHERE hop.forest_no = forest_3.forest_no
                AND hop.genus = forest_2.genus
                AND hop.epithet = forest_2.epithet
            )
        )
        INTO temp2;

        -- Return the two hop solution
        RETURN f1 || ' -> ' || temp2.forest_no || ' -> ' || f2;
    end if;

    -- if second hop doesn't exist, find a hop from f3 to f4, and f4 to f2
    -- in this case, temp1 is null so we need to find a new temp 1 that satisfies all jumps
    SELECT *
    FROM arbor_db.found_in forest_1
    WHERE forest_1.forest_no = f1
    AND EXISTS(
        SELECT 1
        FROM arbor_db.found_in forest_3
        WHERE forest_3.genus = forest_1.genus
        AND forest_3.epithet = forest_1.epithet
        -- given forest 3, find a valid forest 4 that hops to forest 2
        AND EXISTS(
            SELECT 1
            FROM arbor_db.found_in forest_4
            WHERE EXISTS(
                SELECT 1
                FROM arbor_db.found_in hop_one
                WHERE hop_one.forest_no = forest_3.forest_no
                AND hop_one.genus = forest_4.genus
                AND hop_one.epithet = forest_4.epithet
                -- find the hop to forest 2
                AND EXISTS(
                    SELECT 1
                    FROM arbor_db.found_in forest_2
                    WHERE forest_2.forest_no = f2
                    AND EXISTS(
                        SELECT 1
                        FROM arbor_db.found_in hop_two
                        WHERE hop_two.forest_no = forest_4.forest_no
                        AND hop_two.genus = forest_2.genus
                        AND hop_two.epithet = forest_2.epithet
                    )
                )
            )
        )
    )
    INTO temp1;

    RAISE NOTICE '3 hop found starting with % % %', temp1.forest_no, temp1.genus, temp1.epithet;

    IF temp1 IS NOT NULL THEN -- find the hops and print their numbers

        -- find the id of forest3 into hop 1
        SELECT
            temp1.forest_no as f1,
            forest_3.forest_no as f2,
            temp1.genus as shared_genus,
            temp1.epithet as shared_epithet
        FROM arbor_db.found_in forest_3
        WHERE forest_3.genus = temp1.genus
        AND forest_3.epithet = temp1.epithet
        -- given forest 3, find a valid forest 4 that hops to forest 2
        AND EXISTS(
            SELECT 1
            FROM arbor_db.found_in forest_4
            WHERE EXISTS(
                SELECT 1
                FROM arbor_db.found_in hop_one
                WHERE hop_one.forest_no = forest_3.forest_no
                AND hop_one.genus = forest_4.genus
                AND hop_one.epithet = forest_4.epithet
                -- find the hop to forest 2
                AND EXISTS(
                    SELECT 1
                    FROM arbor_db.found_in forest_2
                    WHERE forest_2.forest_no = f2
                    AND EXISTS(
                        SELECT 1
                        FROM arbor_db.found_in hop_two
                        WHERE hop_two.forest_no = forest_4.forest_no
                        AND hop_two.genus = forest_2.genus
                        AND hop_two.epithet = forest_2.epithet
                    )
                )
            )
        )
        INTO hop1;

        RAISE NOTICE 'Hop from % to % with % %', hop1.f1, hop1.f2, hop1.shared_genus, hop1.shared_epithet;

        -- find hop 2 from f3 to f4
        SELECT
            hop1.f2 as f1,
            forest_4.forest_no as f2,
            forest_4.genus as shared_genus,
            forest_4.epithet as shared_epithet
        FROM arbor_db.found_in forest_4
        WHERE EXISTS(
            SELECT 1
            FROM arbor_db.found_in hop_one
            WHERE hop_one.forest_no = hop1.f2
            AND hop_one.genus = forest_4.genus
            AND hop_one.epithet = forest_4.epithet
            AND EXISTS(
                SELECT 1
                FROM arbor_db.found_in forest_2
                WHERE forest_2.forest_no = f2
                AND EXISTS(
                    SELECT 1
                    FROM arbor_db.found_in hop_two
                    WHERE hop_two.forest_no = forest_4.forest_no
                    AND hop_two.genus = forest_2.genus
                    AND hop_two.epithet = forest_2.epithet
                )

            )
        )
        INTO hop2;

        RAISE NOTICE 'Hop from % to % with % %', hop2.f1, hop2.f2, hop2.shared_genus, hop2.shared_epithet;

        RETURN f1 || ' -> ' || hop2.f1 || ' -> ' || hop2.f2 || ' -> ' || f2;
    end if;

    -- Else, there are more than 3 hops, therefore we return null
    RETURN NULL;

end;
$$ LANGUAGE plpgsql;

SELECT * FROM threeDegrees(1, 2);