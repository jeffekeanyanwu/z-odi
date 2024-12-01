# app/ingest.py
import requests
import zipfile
import os
import logging
import json
import duckdb
from pathlib import Path
from typing import Dict, List, Any, Optional
from preprocessing import (
    validate_and_preprocess,
    InfoModel,
    InningsModel
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


def process_chunk(files: List[Path], db_path: str, chunk_num: int, total_chunks: int) -> int:
    """Process a chunk of files in a single transaction."""
    logging.info(f"Processing chunk {chunk_num}/{total_chunks} with {len(files)} files")

    conn = duckdb.connect(db_path)
    successful = 0

    try:
        conn.execute("BEGIN")

        for file_path in files:
            try:
                # Validate and create models
                validated_data = validate_and_preprocess(str(file_path))
                if not validated_data:
                    continue

                info = InfoModel(**validated_data['info'])
                innings = [InningsModel(**inning) for inning in validated_data['innings']]

                # Process match info
                match_sql = """
                INSERT INTO matches (
                    balls_per_over, city, date, event_name, event_match_number,
                    event_group, event_stage, gender, match_type, match_type_number,
                    outcome_winner, outcome_by, outcome_method, outcome_result, outcome_eliminator,
                    overs, player_of_match, season, team_type, team_1, team_2,
                    toss_winner, toss_decision, toss_uncontested, venue
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                RETURNING match_id;
                """

                # Extract data from models
                event = info.event
                outcome = info.outcome
                toss = info.toss
                outcome_by = json.dumps(outcome.by) if outcome and outcome.by else None

                # Get first date from dates array
                match_date = info.dates[0] if info.dates else None

                result = conn.execute(match_sql, (
                    info.balls_per_over,
                    info.city,
                    match_date,
                    event.name if event else None,
                    event.match_number if event else None,
                    event.group if event else None,
                    event.stage if event else None,
                    info.gender,
                    info.match_type,
                    None,
                    outcome.winner if outcome else None,
                    outcome_by,
                    outcome.method if outcome else None,
                    outcome.result if outcome else None,
                    outcome.eliminator if outcome else None,
                    info.overs,
                    info.player_of_match,
                    str(info.season),
                    info.team_type,
                    info.teams[0] if info.teams else "unknown",
                    info.teams[1] if len(info.teams) > 1 else "unknown",
                    toss.winner if toss else "unknown",
                    toss.decision if toss else "unknown",
                    toss.uncontested if toss else False,
                    info.venue
                )).fetchone()

                match_id = result[0]

                # Process players in bulk
                if info.players:
                    player_data = []
                    match_player_data = []
                    for team, players in info.players.items():
                        for player in players:
                            registry_id = info.registry.get("people", {}).get(player)
                            player_data.append((registry_id or player, player, registry_id))
                            match_player_data.append((match_id, registry_id or player, team))

                    conn.executemany("""
                    INSERT INTO players (player_id, player_name, registry_id)
                    VALUES (?, ?, ?)
                    ON CONFLICT (player_id) DO NOTHING;
                    """, player_data)

                    conn.executemany("""
                    INSERT INTO match_players (match_id, player_id, team)
                    VALUES (?, ?, ?);
                    """, match_player_data)

                # Process innings in bulk
                innings_data = []
                for innings_num, inning in enumerate(innings, 1):
                    for over in inning.overs:
                        for ball_num, delivery in enumerate(over.deliveries, 1):
                            wicket = delivery.wickets[0] if delivery.wickets else None
                            fielders = [f.name for f in wicket.fielders] if wicket and wicket.fielders else []

                            innings_data.append((
                                match_id,
                                innings_num,
                                inning.team,
                                over.over,
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
                                fielders,
                                inning.declared,
                                inning.forfeited,
                                inning.super_over
                            ))

                if innings_data:
                    conn.executemany("""
                    INSERT INTO innings (
                        match_id, innings_number, team, over, ball,
                        batter, bowler, non_striker,
                        runs_batter, runs_extras, runs_total, runs_non_boundary,
                        extras_byes, extras_legbyes, extras_noballs, extras_penalty, extras_wides,
                        wicket_type, player_out, fielders,
                        declared, forfeited, super_over
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                    """, innings_data)

                successful += 1

            except Exception as e:
                logging.error(f"Error processing file {file_path}: {e}")
                continue

        conn.execute("COMMIT")
        logging.info(f"Successfully committed chunk {chunk_num}/{total_chunks}")
        return successful

    except Exception as e:
        conn.execute("ROLLBACK")
        logging.error(f"Error processing chunk {chunk_num}: {e}")
        return successful
    finally:
        conn.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Cricket data ingestion script')
    parser.add_argument('-t', '--test', action='store_true',
                        help='Run in test mode (process only first 10 files)')
    parser.add_argument('-n', '--num-files', type=int, default=10,
                        help='Number of files to process in test mode (default: 10)')
    parser.add_argument('-c', '--chunk-size', type=int, default=100,
                        help='Number of files to process in each chunk (default: 100)')
    args = parser.parse_args()

    cricsheet_url = "https://cricsheet.org/downloads/odis_json.zip"
    root_dir = Path(__file__).parent.parent
    data_dir = root_dir / "data"
    db_path = root_dir / "odi_data.db"

    # Initialize
    os.makedirs(data_dir, exist_ok=True)

    # Download data
    download_and_extract_zip(cricsheet_url, str(data_dir))

    # Get list of files
    json_files = list(data_dir.glob("*.json"))
    if args.test:
        json_files = json_files[:args.num_files]
        logging.info(f"Running in test mode with {len(json_files)} files")

    # Process in chunks
    total_files = len(json_files)
    chunk_size = min(args.chunk_size, total_files)
    chunks = [json_files[i:i + chunk_size] for i in range(0, total_files, chunk_size)]
    total_chunks = len(chunks)

    logging.info(f"Processing {total_files} files in {total_chunks} chunks of size {chunk_size}")

    total_successful = 0
    for i, chunk in enumerate(chunks, 1):
        successful = process_chunk(chunk, str(db_path), i, total_chunks)
        total_successful += successful

    logging.info(f"Processing complete. Successfully processed {total_successful}/{total_files} files")
