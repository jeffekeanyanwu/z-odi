# app/db_utils.py
import duckdb
import argparse
import logging
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("cricket_db.log")
    ]
)

def initialize_db(db_file):
    """Initialize the database schema and return connection."""
    try:
        # Connect to database (will create if doesn't exist)
        conn = duckdb.connect(db_file)

        # Create matches table with UUID primary key
        conn.execute("""
        CREATE TABLE IF NOT EXISTS matches (
            match_id UUID PRIMARY KEY DEFAULT uuid(),
            balls_per_over INTEGER DEFAULT 6,
            city TEXT,
            dates TEXT[], -- Array of dates
            event_name TEXT,
            event_match_number INTEGER,
            event_group TEXT,
            event_stage TEXT,
            gender TEXT NOT NULL,
            match_type TEXT NOT NULL, -- Test, ODI, T20, etc.
            match_type_number INTEGER,
            outcome_winner TEXT,
            outcome_by TEXT, -- JSON string for by object
            outcome_method TEXT, -- D/L, VJD, etc.
            outcome_result TEXT, -- draw, tie, no result
            outcome_eliminator TEXT, -- super over winner
            overs INTEGER,
            player_of_match TEXT[], -- Array of players
            season TEXT NOT NULL,
            team_type TEXT NOT NULL, -- international or club
            team_1 TEXT NOT NULL,
            team_2 TEXT NOT NULL,
            toss_winner TEXT NOT NULL,
            toss_decision TEXT NOT NULL,
            toss_uncontested BOOLEAN DEFAULT FALSE,
            venue TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        # Create innings table with auto-incrementing ID and UUID foreign key
        conn.execute("""
        CREATE SEQUENCE IF NOT EXISTS innings_id_seq;
        """)

        conn.execute("""
        CREATE TABLE IF NOT EXISTS innings (
            innings_id INTEGER PRIMARY KEY DEFAULT nextval('innings_id_seq'),
            match_id UUID,
            innings_number INTEGER,
            team TEXT NOT NULL,
            over INTEGER,
            ball INTEGER,
            batter TEXT NOT NULL,
            bowler TEXT NOT NULL,
            non_striker TEXT NOT NULL,
            runs_batter INTEGER DEFAULT 0,
            runs_extras INTEGER DEFAULT 0,
            runs_total INTEGER DEFAULT 0,
            runs_non_boundary BOOLEAN DEFAULT FALSE,
            extras_byes INTEGER,
            extras_legbyes INTEGER,
            extras_noballs INTEGER,
            extras_penalty INTEGER,
            extras_wides INTEGER,
            wicket_type TEXT,
            player_out TEXT,
            fielders TEXT[], -- Array of fielders involved
            declared BOOLEAN DEFAULT FALSE,
            forfeited BOOLEAN DEFAULT FALSE,
            super_over BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (match_id) REFERENCES matches(match_id)
        );
        """)

        # Create players table
        conn.execute("""
        CREATE TABLE IF NOT EXISTS players (
            player_id TEXT PRIMARY KEY,
            player_name TEXT NOT NULL,
            registry_id TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        # Create match_players junction table with UUID foreign key
        conn.execute("""
        CREATE TABLE IF NOT EXISTS match_players (
            match_id UUID,
            player_id TEXT,
            team TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (match_id, player_id),
            FOREIGN KEY (match_id) REFERENCES matches(match_id),
            FOREIGN KEY (player_id) REFERENCES players(player_id)
        );
        """)

        # Create officials table with UUID foreign key
        conn.execute("""
        CREATE TABLE IF NOT EXISTS officials (
            match_id UUID,
            official_name TEXT,
            role TEXT, -- match_referee, umpire, tv_umpire, reserve_umpire
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (match_id, official_name, role),
            FOREIGN KEY (match_id) REFERENCES matches(match_id)
        );
        """)

        logging.info(f"Successfully initialized database at {db_file}")
        return conn
    except Exception as e:
        logging.error(f"Error initializing database: {e}")
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Database utility script")
    parser.add_argument("--initialize", help="Initialize the database schema", type=str)
    args = parser.parse_args()

    if args.initialize:
        try:
            conn = initialize_db(args.initialize)
            print("Database initialized successfully.")
            conn.close()
        except Exception as e:
            print(f"Failed to initialize database: {e}")
            exit(1)
