# app/preprocessing.py
import json
import logging
from typing import Any, Dict, List, Optional, Union
from pydantic import BaseModel, Field, ValidationError

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


class TossModel(BaseModel):
    decision: str  # 'bat' or 'field'
    winner: str
    uncontested: Optional[bool] = None


class OutcomeModel(BaseModel):
    winner: Optional[str] = None
    by: Optional[Dict[str, int]] = None
    method: Optional[str] = None  # D/L, VJD, etc.
    result: Optional[str] = None  # 'draw', 'tie', 'no result'
    eliminator: Optional[str] = None  # for super over winners


class EventModel(BaseModel):
    name: str
    match_number: Optional[int] = None
    group: Optional[str] = None
    stage: Optional[str] = None


class OfficialsModel(BaseModel):
    match_referees: Optional[List[str]] = None
    reserve_umpires: Optional[List[str]] = None
    tv_umpires: Optional[List[str]] = None
    umpires: Optional[List[str]] = None


class InfoModel(BaseModel):
    balls_per_over: int = 6
    city: Optional[str] = None
    dates: List[str]  # Required
    event: Optional[EventModel] = None
    gender: str
    match_type: str  # Required: Test, ODI, T20, etc.
    officials: Optional[OfficialsModel] = None
    outcome: OutcomeModel
    overs: Optional[int] = None
    player_of_match: Optional[List[str]] = None
    players: Dict[str, List[str]]  # Required: teams and their players
    registry: Dict[str, Dict[str, str]]
    season: Union[str, int]
    team_type: str  # 'international' or 'club'
    teams: List[str]  # Required
    toss: TossModel
    venue: Optional[str] = None


class RunsModel(BaseModel):
    batter: int = 0
    extras: int = 0
    total: int = 0
    non_boundary: Optional[bool] = None


class ExtrasModel(BaseModel):
    byes: Optional[int] = None
    legbyes: Optional[int] = None
    noballs: Optional[int] = None
    penalty: Optional[int] = None
    wides: Optional[int] = None


class WicketFielderModel(BaseModel):
    name: str
    substitute: Optional[bool] = None


class WicketModel(BaseModel):
    kind: str  # bowled, caught, etc.
    player_out: str
    fielders: Optional[List[WicketFielderModel]] = None


class DeliveryModel(BaseModel):
    batter: str
    bowler: str
    non_striker: str
    runs: RunsModel
    extras: Optional[ExtrasModel] = None
    wickets: Optional[List[WicketModel]] = None


class OverModel(BaseModel):
    over: int
    deliveries: List[DeliveryModel]


class InningsModel(BaseModel):
    team: str
    overs: List[OverModel]
    declared: Optional[bool] = None
    forfeited: Optional[bool] = None
    super_over: Optional[bool] = None


def validate_and_preprocess(file_path: str) -> Dict[str, Any]:
    """
    Validate and preprocess a JSON file.
    Games without required fields (teams, dates) are rejected.
    """
    try:
        with open(file_path, "r") as file:
            data = json.load(file)

        # Validate and preprocess `info`
        info = {}
        try:
            raw_info = data.get("info", {})

            # Check required fields
            if not raw_info.get("teams") or not raw_info.get("dates"):
                logging.warning(f"Skipping {file_path}: Missing required teams or dates")
                return {}

            # Additional validation for match type
            if not raw_info.get("match_type"):
                logging.warning(f"Skipping {file_path}: Missing match type")
                return {}

            info = InfoModel(**raw_info)
            info = info.model_dump()
        except ValidationError as e:
            logging.warning(f"Info validation failed for {file_path}. Errors: {e}")
            return {}

        # Validate and preprocess innings
        innings = []
        for i, inning_data in enumerate(data.get("innings", [])):
            try:
                inning = InningsModel(**inning_data)
                innings.append(inning.model_dump())
            except ValidationError as e:
                logging.warning(f"Innings {i + 1} validation failed in {file_path}. Errors: {e}")
                continue  # Skip invalid innings

        if not innings:
            logging.warning(f"No valid innings data in {file_path}")
            return {}

        validated_data = {
            "info": info,
            "innings": innings,
        }

        logging.info(f"Validated data for {file_path}")
        return validated_data

    except Exception as e:
        logging.error(f"Error processing file {file_path}")
        logging.exception(e)
        return {}


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        file_path = sys.argv[1]
        validated_data = validate_and_preprocess(file_path)
        if validated_data:
            print("Validation successful!")
            print(f"Teams: {validated_data['info']['teams']}")
            print(f"Date: {validated_data['info']['dates']}")
            print(f"Number of innings: {len(validated_data['innings'])}")
        else:
            print("Validation failed!")
