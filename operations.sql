-- Data Manipulation Operations --
----------------------------------

-- 1. addForest
CREATE OR REPLACE PROCEDURE addForest(name varchar(30), area integer, acid_level real, MBR_XMin real, MBR_XMax real, MBR_YMin real, MBR_YMax real)
AS $$
DECLARE
  f_no INTEGER;
BEGIN
  SELECT MAX(forest_no) + 1 INTO f_no FROM arbor_db.FOREST F;
  INSERT INTO arbor_db.FOREST VALUES (f_no, name, area, acid_level, MBR_XMin, MBR_XMax, MBR_YMin, MBR_YMax);
end;
$$ LANGUAGE plpgsql;


-- 2. addTreeSpecies
CREATE OR REPLACE PROCEDURE addTreeSpecies(genus varchar(30), epithet varchar(30), ideal_temperature real, largest_height real, raunkiaer_life_form varchar(16))
AS $$
BEGIN
  INSERT INTO arbor_db.TREE_SPECIES VALUES (genus, epithet, ideal_temperature, largest_height, raunkiaer_life_form);
end;
$$ LANGUAGE plpgsql;

-- 3. addSpeciesToForest
CREATE OR REPLACE PROCEDURE addSpeciesToForest(forest_no integer, genus varchar(30), epithet varchar(30))
AS $$
BEGIN
  INSERT INTO arbor_db.FOUND_IN VALUES (forest_no, genus, epithet);
end;
$$ LANGUAGE plpgsql;

-- 4. newWorker
CREATE OR REPLACE PROCEDURE newWorker(ssn char(9), first_name varchar(30), last_name varchar(30), mi char(1), rank varchar(10), state_abbr char(2))
AS $$
BEGIN
  INSERT INTO arbor_db.WORKER VALUES (ssn, first_name, last_name, mi, rank);
  INSERT INTO arbor_db.EMPLOYED VALUES (state_abbr, ssn);
end;
$$ LANGUAGE plpgsql;

-- 5. employWorkerToState
CREATE OR REPLACE PROCEDURE employWorkerToState(state_abbr char(2), ssn char(9))
AS $$
BEGIN
  INSERT INTO arbor_db.EMPLOYED VALUES (state_abbr, ssn);
end;
$$ LANGUAGE plpgsql;

-- 6. placeSensor
CREATE OR REPLACE PROCEDURE placeSensor(energy integer, X real, Y real, maintainer_id char(9))
AS $$
DECLARE
  new_sensor_id INTEGER;
  last_charged TIMESTAMP;
  last_read TIMESTAMP;
  time TIMESTAMP;
BEGIN
  SELECT MAX(sensor_id) + 1 INTO new_sensor_id FROM arbor_db.SENSOR;

  SELECT synthetic_time INTO time FROM arbor_db.CLOCK;
  last_charged := time;
  last_read := time;
  INSERT INTO arbor_db.SENSOR VALUES (new_sensor_id, last_charged, energy, last_read, X, Y, maintainer_id);
end;
$$ LANGUAGE plpgsql;

-- 7. generateReport
CREATE OR REPLACE PROCEDURE generateReport(sensor_id integer, report_time timestamp, temperature real)
AS $$
BEGIN
  INSERT INTO arbor_db.REPORT VALUES(sensor_id, report_time, temperature);
end;
$$ LANGUAGE plpgsql;

-- 8. removeSpeciesFromForest
CREATE OR REPLACE PROCEDURE removeSpeciesFromForest(d_forest_no integer, d_genus varchar(30), d_epithet varchar(30))
AS $$
BEGIN
  DELETE FROM arbor_db.FOUND_IN
  WHERE forest_no = d_forest_no
  AND genus = d_genus
  AND epithet = d_epithet;
end;
$$ LANGUAGE plpgsql;

-- 9. deleteWorker
CREATE OR REPLACE PROCEDURE deleteWorker(d_ssn char(9))
AS $$
BEGIN
  -- delete all sensors that were maintained by this worker
  DELETE FROM arbor_db.SENSOR
  WHERE maintainer_id = d_ssn;
  -- delete the worker from EMPLOYED
  DELETE FROM arbor_db.EMPLOYED
  WHERE worker = d_ssn;
  -- delete the worker from WORKER
  DELETE FROM arbor_db.WORKER
  WHERE SSN = d_ssn;
end;
$$ LANGUAGE plpgsql;

-- 10. moveSensor
CREATE OR REPLACE PROCEDURE moveSensor(target_sensor_id integer, n_X real, n_Y real)
AS $$
BEGIN
  UPDATE arbor_db.SENSOR
  SET X = n_X, Y = n_Y
  WHERE sensor_id = target_sensor_id;
end;
$$ LANGUAGE plpgsql;

-- 11. removeWorkerFromState
CREATE OR REPLACE PROCEDURE removeWorkerFromState(ssn char(9), abbrv char(2))
AS $$
DECLARE
    maintained_sensor arbor_db.SENSOR%ROWTYPE;
    min_worker char(9);
BEGIN
    -- 1. remove worker from EMPLOYED for given state
    DELETE FROM arbor_db.EMPLOYED
    WHERE worker = ssn
    AND state = abbrv;

    -- 2. get the replacement worker within the given state, if not found, delete the sensor(s)
    SELECT getMinWorkerInState(abbrv) INTO min_worker;
    RAISE NOTICE 'Min Worker: %', min_worker;
    IF (min_worker IS NOT NULL) THEN
        -- get list of sensors, replace their maintainer with the new worker SSN
        RAISE NOTICE 'Beginning maintainer replacement of sensors (this could be 0 or more sensors)...';
        FOR maintained_sensor IN SELECT * FROM getWorkerSensorsWithinState(ssn, abbrv) LOOP
            RAISE NOTICE 'Sensor to be replaced: %', maintained_sensor.sensor_id;
            UPDATE arbor_db.SENSOR
            SET maintainer_id = min_worker
            WHERE sensor_id = maintained_sensor.sensor_id;
        END LOOP;
    ELSE
        -- get list of sensors, delete them
        RAISE NOTICE 'Beginning delete of sensors (this could be 0 or more sensors)...';
        FOR maintained_sensor IN SELECT * FROM getWorkerSensorsWithinState(ssn, abbrv) LOOP
            RAISE NOTICE 'Sensor to be deleted: %', maintained_sensor.sensor_id;
            CALL removeSensor(maintained_sensor.sensor_id);
        END LOOP;
    END IF;

END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION getWorkerSensorsWithinState(ssn char(9), abbrv char(2))
RETURNS TABLE (
    sensor_id integer,
    last_charged timestamp,
    energy integer,
    last_read timestamp,
    X real,
    Y real,
    maintainer_id char(9)
              ) AS
$$
DECLARE
    MBR RECORD;
BEGIN
    SELECT mbr_xmin AS xmin, mbr_xmax AS xmax, mbr_ymin AS ymin, mbr_ymax AS ymax FROM arbor_db.STATE
    WHERE abbreviation = abbrv
    INTO MBR;

    RAISE NOTICE 'MBR %', MBR.xmin;
    -- get list of sensors within state and maintainer
    RETURN QUERY (SELECT * FROM arbor_db.SENSOR S
                    WHERE S.maintainer_id = ssn
                      AND (S.x >= MBR.xmin AND S.x <= MBR.xmax)
                      AND (S.y >= MBR.ymin AND S.y <= MBR.ymax));
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION getMinWorkerInState(abbrv char(2))
RETURNS char(9) AS
$$
DECLARE
    min_worker char(9);
BEGIN

    SELECT MIN(worker) FROM arbor_db.EMPLOYED
    WHERE state = abbrv
    INTO min_worker;

    RETURN min_worker;
END;
$$ LANGUAGE plpgsql;

-- 12. removeSensor
CREATE OR REPLACE PROCEDURE removeSensor(d_sensor_id integer)
AS $$
BEGIN
  DELETE FROM arbor_db.SENSOR
  WHERE sensor_id = d_sensor_id;
  DELETE FROM arbor_db.REPORT
  WHERE sensor_id = d_sensor_id;
end;
$$ LANGUAGE plpgsql;

-- 13. listSensors (function)
CREATE OR REPLACE FUNCTION listSensors(target_forest_id integer)
RETURNS TABLE (
  sensor_id integer,
  last_charged timestamp,
  energy integer,
  last_read timestamp,
  X real,
  Y real,
  maintainer_id char(9)
)
AS $$
DECLARE
  MBR RECORD;
  sensor RECORD;
BEGIN
  -- create temp table with all collums as SENSOR has
  CREATE TEMPORARY TABLE SENSORS_IN_FOREST AS
  SELECT * FROM arbor_db.SENSOR
  WHERE false; -- creates empty table, but with all collumns
  -- get the forest's MBR info
  SELECT MBR_XMin, MBR_XMax, MBR_YMin, MBR_YMax INTO MBR
  FROM arbor_db.FOREST
  WHERE forest_no = target_forest_id;
  -- list all sensors that are part of the forest with target_forest_id
  FOR sensor IN SELECT * FROM arbor_db.SENSOR LOOP
    IF (sensor.X >= MBR.MBR_XMin AND sensor.X <= MBR.MBR_XMax)
      AND
       (sensor.Y >= MBR.MBR_YMin AND sensor.Y <= MBR.MBR_YMax) THEN
        -- if in range, add to return table
        INSERT INTO SENSORS_IN_FOREST VALUES (sensor.sensor_id, sensor.last_charged, sensor.energy, sensor.last_read, sensor.X, sensor.Y, sensor.maintainer_id);
    end if;
  end loop;
  -- return the table
  RETURN QUERY SELECT * FROM SENSORS_IN_FOREST;
  DROP TABLE IF EXISTS SENSORS_IN_FOREST;
end;
$$ LANGUAGE plpgsql;

-- 14. listMaintainedSensors (function)
CREATE OR REPLACE FUNCTION listMaintainedSensors(w_ssn char(9))
RETURNS TABLE (
  sensor_id integer,
  last_charged timestamp,
  energy integer,
  last_read timestamp,
  X real,
  Y real,
  maintainer_id char(9)
)
AS $$
-- display all sensors that the worker is currently maintaining
DECLARE
    sensor arbor_db.SENSOR%ROWTYPE;
BEGIN
  -- create temp table with all collumns as SENSOR has
  CREATE TEMPORARY TABLE SENSORS_FOR_WORKER AS
  SELECT * FROM arbor_db.SENSOR
  WHERE false; -- creates empty table, but with all collumns
  -- loop through all sensors and compare worker ssn
  FOR sensor IN SELECT * FROM arbor_db.SENSOR LOOP
    -- if ssn match
    IF (sensor.maintainer_id = w_ssn) THEN
      -- add to our temporary table because we want to return it
      INSERT INTO SENSORS_FOR_WORKER VALUES (sensor.sensor_id, sensor.last_charged, sensor.energy, sensor.last_read, sensor.X, sensor.Y, sensor.maintainer_id);
    end if;
  end loop;
  -- return the table
  RETURN QUERY SELECT * FROM SENSORS_FOR_WORKER;
  DROP TABLE IF EXISTS SENSORS_FOR_WORKER;
end;
$$ LANGUAGE plpgsql;

-- 15. locateTreeSpecies (function)
CREATE OR REPLACE FUNCTION locateTreeSpecies(alpha varchar(30), beta varchar(30))
RETURNS TABLE (
  forest_no integer,
  name varchar(30),
  area integer,
  acid_level real,
  MBR_XMin real,
  MBR_XMax real,
  MBR_YMin real,
  MBR_YMax real
)
AS $$
DECLARE
  rec_found_in RECORD;
  rec_forest arbor_db.FOREST%ROWTYPE;
BEGIN
  -- create temp table with same schema as FOREST
  CREATE TEMPORARY TABLE FOREST_RESULTS AS
  SELECT * FROM arbor_db.FOREST
  WHERE false;

  -- loop through all the species in FOUND_IN table
  FOR rec_found_in IN SELECT * FROM arbor_db.FOUND_IN LOOP
    -- compare genus and epithet
    IF (rec_found_in.genus LIKE alpha OR rec_found_in.epithet LIKE beta) THEN
        -- Get full row from FOREST table now that we've found a match
        SELECT * FROM arbor_db.FOREST F
        WHERE F.forest_no = rec_found_in.forest_no
        INTO rec_forest;

        -- add values into results
        INSERT INTO FOREST_RESULTS VALUES (rec_forest.forest_no, rec_forest.name, rec_forest.area, rec_forest.acid_level, rec_forest.MBR_xmin, rec_forest.mbr_xmax, rec_forest.mbr_ymin, rec_forest.mbr_ymax);
    END IF;
  END LOOP;
  -- return the resulting table
  RETURN QUERY SELECT * FROM FOREST_RESULTS;
  DROP TABLE IF EXISTS FOREST_RESULTS;
end;
$$ LANGUAGE plpgsql;
