# app/ingest.py
import json
import logging
import os
import zipfile
from pathlib import Path
from typing import Optional

import duckdb
import requests

from db_utils import initialize_db
from preprocessing import (
    validate_and_preprocess,
    InfoModel,
    InningsModel,
    EventModel,
    OutcomeModel,
    TossModel,
    DeliveryModel,
    OfficialsModel
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("cricket_ingest.log")
    ]
)


def download_and_extract_zip(url: str, extract_to: str = "data") -> None:
    """Download and extract cricket data ZIP file."""
    os.makedirs(extract_to, exist_ok=True)
    try:
        logging.info(f"Downloading file from {url}...")
        response = requests.get(url)
        response.raise_for_status()

        zip_path = os.path.join(extract_to, "temp.zip")
        with open(zip_path, "wb") as file:
            file.write(response.content)
        logging.info(f"Downloaded zip file to {zip_path}")

        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(extract_to)
        logging.info(f"Extracted JSON files to {extract_to}")

        os.remove(zip_path)
        logging.info("Temporary zip file removed.")
    except Exception as e:
        logging.error("Failed to download or extract the zip file.")
        logging.exception(e)
        raise


def process_event(event: Optional[EventModel]) -> tuple:
    """Process event model and return tuple of values for database."""
    if not event:
        return None, None, None, None
    return event.name, event.match_number, event.group, event.stage


def process_outcome(outcome: Optional[OutcomeModel]) -> tuple:
    """Process outcome model and return tuple of values for database."""
    if not outcome:
        return None, None, None, None, None
    outcome_by = json.dumps(outcome.by) if outcome.by else None
    return (
        outcome.winner,
        outcome_by,
        outcome.method,
        outcome.result,
        outcome.eliminator
    )


def process_toss(toss: Optional[TossModel]) -> tuple:
    """Process toss model and return tuple of values for database."""
    if not toss:
        return "unknown", "unknown", False
    return toss.winner, toss.decision, toss.uncontested or False


def process_match_info(conn: duckdb.DuckDBPyConnection, info: InfoModel) -> Optional[str]:
    """Insert match info and related data into the database using Pydantic models."""
    try:
        match_sql = """
        INSERT INTO matches (
            balls_per_over, city, dates, event_name, event_match_number,
            event_group, event_stage, gender, match_type, match_type_number,
            outcome_winner, outcome_by, outcome_method, outcome_result, outcome_eliminator,
            overs, player_of_match, season, team_type, team_1, team_2,
            toss_winner, toss_decision, toss_uncontested, venue
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        RETURNING match_id;
        """

        event_name, event_number, event_group, event_stage = process_event(info.event)
        outcome_winner, outcome_by, outcome_method, outcome_result, outcome_eliminator = process_outcome(info.outcome)
        toss_winner, toss_decision, toss_uncontested = process_toss(info.toss)

        result = conn.execute(match_sql, (
            info.balls_per_over,
            info.city,
            info.dates,
            event_name,
            event_number,
            event_group,
            event_stage,
            info.gender,
            info.match_type,
            None,  # match_type_number not in model
            outcome_winner,
            outcome_by,
            outcome_method,
            outcome_result,
            outcome_eliminator,
            info.overs,
            info.player_of_match,
            str(info.season),
            info.team_type,
            info.teams[0] if info.teams else "unknown",
            info.teams[1] if len(info.teams) > 1 else "unknown",
            toss_winner,
            toss_decision,
            toss_uncontested,
            info.venue
        )).fetchone()

        if not result:
            logging.error("Failed to insert match info - no ID returned")
            return None

        match_id = result[0]

        process_players(conn, match_id, info)
        if info.officials:
            process_officials(conn, match_id, info.officials)

        logging.info(f"Successfully inserted match info with ID: {match_id}")
        return match_id

    except Exception as e:
        logging.error(f"Error in process_match_info: {e}")
        raise


def process_players(conn: duckdb.DuckDBPyConnection, match_id: str, info: InfoModel) -> None:
    """Process players using Pydantic model data."""
    try:
        if not info.players:
            return

        registry_people = info.registry.get("people", {})

        for team, players in info.players.items():
            for player in players:
                registry_id = registry_people.get(player)

                player_sql = """
                INSERT INTO players (player_id, player_name, registry_id)
                VALUES (?, ?, ?)
                ON CONFLICT (player_id) DO NOTHING;
                """
                player_id = registry_id if registry_id else player
                conn.execute(player_sql, (player_id, player, registry_id))

                match_player_sql = """
                INSERT INTO match_players (match_id, player_id, team)
                VALUES (?, ?, ?);
                """
                conn.execute(match_player_sql, (match_id, player_id, team))
    except Exception as e:
        logging.error(f"Error processing players for match {match_id}: {e}")
        raise


def process_officials(conn: duckdb.DuckDBPyConnection, match_id: str, officials: OfficialsModel) -> None:
    """Process match officials using Pydantic model."""
    try:
        role_mapping = {
            'match_referees': 'match_referee',
            'reserve_umpires': 'reserve_umpire',
            'tv_umpires': 'tv_umpire',
            'umpires': 'umpire'
        }

        for role, officials_list in {
            'match_referees': officials.match_referees,
            'reserve_umpires': officials.reserve_umpires,
            'tv_umpires': officials.tv_umpires,
            'umpires': officials.umpires
        }.items():
            if not officials_list:
                continue

            role_mapped = role_mapping.get(role)
            if not role_mapped:
                continue

            for name in officials_list:
                sql = """
                INSERT INTO officials (match_id, official_name, role)
                VALUES (?, ?, ?);
                """
                conn.execute(sql, (match_id, name, role_mapped))
    except Exception as e:
        logging.error(f"Error processing officials for match {match_id}: {e}")
        raise


def process_delivery(delivery: DeliveryModel, match_id: str, innings_num: int,
                     over_num: int, ball_num: int, team: str) -> tuple:
    """Process delivery using Pydantic model and return tuple for database insertion."""
    wicket = delivery.wickets[0] if delivery.wickets else None
    fielders = [f.name for f in wicket.fielders] if wicket and wicket.fielders else []

    return (
        match_id,
        innings_num,
        team,
        over_num,
        ball_num,
        delivery.batter,
        delivery.bowler,
        delivery.non_striker,
        delivery.runs.batter,
        delivery.runs.extras,
        delivery.runs.total,
        delivery.runs.non_boundary,
        delivery.extras.byes if delivery.extras else None,
        delivery.extras.legbyes if delivery.extras else None,
        delivery.extras.noballs if delivery.extras else None,
        delivery.extras.penalty if delivery.extras else None,
        delivery.extras.wides if delivery.extras else None,
        wicket.kind if wicket else None,
        wicket.player_out if wicket else None,
        fielders
    )


def process_innings_data(conn: duckdb.DuckDBPyConnection, match_id: str,
                         innings_num: int, inning: InningsModel) -> None:
    """Process innings data using Pydantic models."""
    try:
        if not inning.overs:
            logging.warning(f"Skipping innings {innings_num} for match {match_id}: No overs data")
            return

        sql = """
        INSERT INTO innings (
            match_id, innings_number, team, over, ball,
            batter, bowler, non_striker,
            runs_batter, runs_extras, runs_total, runs_non_boundary,
            extras_byes, extras_legbyes, extras_noballs, extras_penalty, extras_wides,
            wicket_type, player_out, fielders,
            declared, forfeited, super_over
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """

        for over in inning.overs:
            for ball_num, delivery in enumerate(over.deliveries, 1):
                values = list(process_delivery(delivery, match_id, innings_num,
                                               over.over, ball_num, inning.team))
                values.extend([inning.declared, inning.forfeited, inning.super_over])
                conn.execute(sql, tuple(values))

        logging.info(f"Successfully inserted innings {innings_num} data for {match_id}")
    except Exception as e:
        logging.error(f"Error in process_innings_data for match_id {match_id}, innings {innings_num}: {e}")
        raise


def parse_and_load(file_path: str, db_path: str) -> None:
    """Parse and load validated JSON data into the database using Pydantic models."""
    conn = None
    try:
        print(f"Processing file: {file_path}")
        validated_data = validate_and_preprocess(file_path)

        if not validated_data:
            logging.warning(f"No valid data found in {file_path}. Skipping ingestion.")
            return

        info = InfoModel(**validated_data['info'])
        innings = [InningsModel(**inning) for inning in validated_data['innings']]

        try:
            conn = initialize_db(db_path)
            conn.execute("BEGIN")

            match_id = process_match_info(conn, info)
            if not match_id:
                raise ValueError("Failed to generate match_id")

            for innings_num, inning in enumerate(innings, 1):
                process_innings_data(conn, match_id, innings_num, inning)

            conn.execute("COMMIT")
            logging.info(f"Successfully processed file: {file_path}")
        except Exception as e:
            if conn:
                conn.execute("ROLLBACK")
            logging.error(f"Failed to process file {file_path}: {e}")
            raise
        finally:
            if conn:
                conn.close()
    except Exception as e:
        logging.error(f"Error processing file {file_path}: {e}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Cricket data ingestion script')
    parser.add_argument('-t', '--test', action='store_true',
                        help='Run in test mode (process only first 10 files)')
    parser.add_argument('-n', '--num-files', type=int, default=10,
                        help='Number of files to process in test mode (default: 10)')
    args = parser.parse_args()

    cricsheet_url = "https://cricsheet.org/downloads/odis_json.zip"
    root_dir = Path(__file__).parent.parent
    data_dir = root_dir / "data"
    db_path = root_dir / "odi_data.db"

    os.makedirs(data_dir, exist_ok=True)

    if db_path.exists():
        try:
            db_path.unlink()
            logging.info(f"Removed existing database at {db_path}")
        except Exception as e:
            logging.error(f"Error removing existing database: {e}")
            exit(1)

    conn = initialize_db(str(db_path))
    conn.close()

    download_and_extract_zip(cricsheet_url, str(data_dir))

    json_files = list(data_dir.glob("*.json"))

    if args.test:
        json_files = json_files[:args.num_files]
        logging.info(f"Running in test mode with first {len(json_files)} files")

    total_files = len(json_files)
    for i, file in enumerate(json_files, 1):
        try:
            logging.info(f"Processing file {i}/{total_files}: {file.name}")
            parse_and_load(str(file), str(db_path))
        except Exception as e:
            logging.error(f"Failed to process file {file}: {e}")
            continue
