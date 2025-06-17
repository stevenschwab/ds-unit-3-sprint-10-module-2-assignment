'''Queries for csv to Postgres pipeline'''

DROP_TITANIC_TABLE = """
    DROP TABLE IF EXISTS titanic;
"""

CREATE_TITANIC_TABLE = """
    DROP TYPE IF EXISTS sex;
    CREATE TYPE sex AS ENUM ('male', 'female');
    CREATE TABLE IF NOT EXISTS titanic (
        Passenger_Id SERIAL PRIMARY KEY,
        Survived INT,
        Pclass INT,
        Name VARCHAR(255),
        Sex sex,
        Age NUMERIC(3, 1),
        Siblings_Spouses_Aboard INT,
        Parents_Children_Aboard INT,
        Fare NUMERIC(8, 5)
    );
"""

# Postgres explorer queries
GET_PASSENGERS = """
    SELECT * FROM titanic;
"""

GET_PASSENGER_COUNT = """
    SELECT COUNT(*)
    FROM titanic;
"""

GET_PASSENGER_TABLE_INFO = """
    PRAGMA table.info(titanic);
"""