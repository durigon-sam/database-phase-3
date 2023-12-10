DROP SCHEMA IF EXISTS arbor_db CASCADE;
CREATE SCHEMA arbor_db;

DROP TABLE IF EXISTS arbor_db.FOREST CASCADE;
DROP TABLE IF EXISTS arbor_db.EMPLOYED CASCADE;
DROP TABLE IF EXISTS arbor_db.PHONE CASCADE;
DROP TABLE IF EXISTS arbor_db.REPORT CASCADE;
DROP TABLE IF EXISTS arbor_db.SENSOR CASCADE;
DROP TABLE IF EXISTS arbor_db.STATE CASCADE;
DROP TABLE IF EXISTS arbor_db.TREE_COMMON_NAME CASCADE;
DROP TABLE IF EXISTS arbor_db.TREE_SPECIES CASCADE;
DROP TABLE IF EXISTS arbor_db.WORKER CASCADE;
DROP TABLE IF EXISTS arbor_db.COVERAGE CASCADE;
DROP TABLE IF EXISTS arbor_db.FOUND_IN CASCADE;
DROP TABLE IF EXISTS arbor_db.CLOCK CASCADE;

DROP DOMAIN IF EXISTS Raunkiaer_life_form;
DROP DOMAIN IF EXISTS Rank;

CREATE TABLE arbor_db.FOREST (
    forest_no integer PRIMARY KEY,
    name varchar(30),
    area integer,
    acid_level real,
    MBR_XMin real,
    MBR_XMax real,
    MBR_YMin real,
    MBR_YMax real
);

-- STATE (name, abbreviation, area, population, MBR XMin, MBR XMax, MBR YMin, MBR YMax)
CREATE TABLE arbor_db.STATE (
    name varchar(30) UNIQUE,
    abbreviation char(2) PRIMARY KEY,
    area integer,
    population integer,
    MBR_XMin real,
    MBR_XMax real,
    MBR_YMin real,
    MBR_YMax real
);

-- TREE SPECIES (genus, epithet, ideal temperature, largest height, raunkiaer life form)
CREATE DOMAIN Raunkiaer_life_form varchar(16)
    CHECK (VALUE IN ('Phanerophytes', 'Epiphytes', 'Chamaephytes', 'Hemicryptophytes',
'Cryptophytes', 'Therophytes', 'Aerophytes'));

CREATE TABLE arbor_db.TREE_SPECIES (
    genus varchar(30),
    epithet varchar(30),
    ideal_temperature real,
    largest_height real,
    raunkiaer_life_form Raunkiaer_life_form,
    CONSTRAINT TREE_SPECIES_PK PRIMARY KEY (genus, epithet)
);

-- TREE COMMON NAME (genus, epithet, common name)
CREATE TABLE arbor_db.TREE_COMMON_NAME (
    genus varchar(30),
    epithet varchar(30),
    common_name varchar(30),
    CONSTRAINT TREE_COMMON_NAME_PK PRIMARY KEY (genus, epithet, common_name),
    FOREIGN KEY (genus, epithet) REFERENCES arbor_db.TREE_SPECIES(genus, epithet)
);

-- WORKER (SSN, first, last, middle, rank)
CREATE DOMAIN Rank varchar(10)
    CHECK (VALUE IN ('Lead', 'Senior', 'Associate'));
CREATE TABLE arbor_db.WORKER (
    SSN char(9) PRIMARY KEY CHECK (length(SSN) = 9 AND SSN ~ '^[0-9]+$'),
    first varchar(30),
    last varchar(30),
    middle char(1),
    rank Rank
);
-- SENSOR (sensor id, last charged, energy, last read, X, Y, maintainer id)
CREATE TABLE arbor_db.SENSOR (
    sensor_id integer PRIMARY KEY,
    last_charged timestamp,
    energy integer,
    last_read timestamp,
    X real,
    Y real,
    maintainer_id char(9) REFERENCES arbor_db.WORKER(SSN)
);

CREATE TABLE arbor_db.REPORT (
    sensor_id integer REFERENCES arbor_db.SENSOR(sensor_id),
    report_time timestamp,
    temperature real,
    CONSTRAINT REPORT_PK PRIMARY KEY (sensor_id, report_time)
);

CREATE TABLE arbor_db.PHONE (
    worker char(9) REFERENCES arbor_db.WORKER(SSN),
    type varchar(30),
    number real,
    CONSTRAINT PHONE_PK PRIMARY KEY (worker, type, number)
);

-- COVERAGE (forest no, state, percentage, area)
CREATE TABLE arbor_db.COVERAGE (
    forest_no integer REFERENCES arbor_db.FOREST(forest_no),
    state char(2) REFERENCES arbor_db.STATE(abbreviation),
    percentage real,
    area integer,
    CONSTRAINT COVERAGE_PK PRIMARY KEY (forest_no, state)
);

-- FOUND IN (forest no, genus, epithet)
CREATE TABLE arbor_db.FOUND_IN (
    forest_no integer REFERENCES arbor_db.FOREST(forest_no),
    genus varchar(30),
    epithet varchar(30),
    CONSTRAINT FOUND_IN_PK PRIMARY KEY (forest_no, genus, epithet),
    FOREIGN KEY (genus, epithet) REFERENCES arbor_db.TREE_SPECIES(genus, epithet)
);

CREATE TABLE arbor_db.EMPLOYED (
    state char(2) REFERENCES arbor_db.STATE(abbreviation),
    worker char(9) REFERENCES arbor_db.WORKER(SSN),
    CONSTRAINT EMPLOYED_PK PRIMARY KEY (state, worker)
);
CREATE TABLE arbor_db.CLOCK (
    synthetic_time timestamp PRIMARY KEY
);


