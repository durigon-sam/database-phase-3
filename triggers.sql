-- TRIGGERS
CREATE OR REPLACE FUNCTION createForestCoverage()
RETURNS TRIGGER AS
$$
DECLARE
    overlapping_area real;
    x_dist real;
    y_dist real;
    overlapping_percentage real;
    abbrv char(2);
    state arbor_db.STATE%ROWTYPE;
BEGIN
    RAISE NOTICE '%', NEW;
    RAISE NOTICE '% % % %', NEW.mbr_xmin, NEW.mbr_xmax, NEW.mbr_ymin, NEW.mbr_ymax;
    FOR state IN SELECT * FROM arbor_db.STATE LOOP
--         RAISE NOTICE 'Inside loop for state %', state;
        IF (NEW.mbr_xmin <> NEW.mbr_xmax AND NEW.mbr_ymin <> NEW.mbr_ymax) THEN
            IF (state.mbr_xmin <> state.mbr_xmax AND state.mbr_ymin <> state.mbr_ymax) THEN
                IF (((NEW.mbr_xmin >= state.mbr_xmin AND NEW.mbr_xmin <= state.mbr_xmax) OR
                    (state.mbr_xmin >= NEW.mbr_xmin AND state.mbr_xmin <= NEW.mbr_xmax))
                    AND
                    ((NEW.mbr_ymax >= state.mbr_ymin AND NEW.mbr_ymax <= state.mbr_ymax) OR
                    (state.mbr_ymax >= NEW.mbr_ymin AND state.mbr_ymax <= NEW.mbr_ymax))) THEN

                    -- THEY OVERLAP
                    x_dist := LEAST(NEW.mbr_xmax, state.mbr_xmax) - GREATEST(NEW.mbr_xmin, state.mbr_xmin);
                    y_dist := LEAST(NEW.mbr_ymax, state.mbr_ymax) - GREATEST(NEW.mbr_ymin, state.mbr_ymin);
                    overlapping_area := x_dist * y_dist;

                    -- TODO: Something about this percentage calculation is wrong...
                    RAISE NOTICE 'Overlapping area %', overlapping_area;
                    overlapping_percentage := (overlapping_area / NEW.area) * 100;
                    abbrv := state.abbreviation;

                    INSERT INTO arbor_db.COVERAGE VALUES (NEW.forest_no, abbrv, overlapping_percentage, overlapping_area);
                END IF;
            END IF;
        END IF;
    END LOOP;
    RETURN NEW;
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER addForestCoverage
    AFTER INSERT
    ON arbor_db.FOREST
    FOR EACH ROW
    EXECUTE FUNCTION createForestCoverage();

ALTER TABLE arbor_db.FOREST ENABLE TRIGGER ALL;


-- Second Trigger

-- Upon adding a new sensor to the SENSOR relation or updating the location (X, Y) of an existing sensor, this trigger
--  will prevent the insertion or update of the sensor if the maintainer is not employed by a state that covers the
--  sensor's new location (X, Y)

CREATE OR REPLACE FUNCTION checkMaintainerEmployment()
RETURNS TRIGGER AS $$
DECLARE
    state arbor_db.STATE%ROWTYPE;
    abbrv char(2);
    employ arbor_db.EMPLOYED%ROWTYPE;
    found bool;
BEGIN

    -- find the state that covers the SENSOR's x,y position
    FOR state IN SELECT * FROM arbor_db.STATE LOOP
        -- check that the current x position is between the state's position
        IF (NEW.X <= state.MBR_XMax AND NEW.X >= state.MBR_XMin)
            AND
           (NEW.Y <= state.MBR_YMax AND NEW.Y >= state.MBR_YMin)
           THEN
            abbrv := state.abbreviation;
        end IF;
    end loop;
    RAISE NOTICE 'abbrv %', abbrv;

    -- Get a list of all workers employed by this state
    found := false;
    FOR employ IN SELECT * FROM arbor_db.EMPLOYED E WHERE E.state = abbrv LOOP
        -- check that the SENSOR's maintainer works for this state (included in the created list)
        RAISE NOTICE 'employ.state %', employ.state;
        IF (NEW.maintainer_id = employ.worker) THEN
            found := true;
            EXIT;
        end IF;
    end loop;
    -- raise exception to prevent insert or update
    IF (found IS false) THEN
        RAISE EXCEPTION 'The new maintainer of this sensor is not employed by a state which covers the sensor. This operation has been reverted.';
    end IF;

    RETURN NEW;
end;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER addMaintainerEmployment
    BEFORE INSERT OR
        UPDATE OF X, Y
    ON arbor_db.SENSOR
    FOR EACH ROW
    -- WHEN (OLD.X IS DISTINCT FROM NEW.X OR OLD.Y IS DISTINCT FROM NEW.Y)
    -- ^ use this if the "update of x, y" doesn't work
    EXECUTE FUNCTION checkMaintainerEmployment();

ALTER TABLE arbor_db.SENSOR ENABLE TRIGGER ALL;

