from pathlib import Path
import os
import json
from googleapiclient.discovery import build
import google.oauth2
from google.auth.credentials import Credentials
import pandas as pd
import numpy as np
from con_spreadsheet import ConSpreadsheet
from dotenv import load_dotenv
import fastapi
from fastapi import FastAPI
from loguru import logger
from guidebook import Guidebook


load_dotenv()
app = FastAPI(default_response_class=fastapi.responses.ORJSONResponse)

logger.add("./cache/log.txt", rotation="1 MB", serialize=True)


def update_guidebook_cache(
    guidebook: Guidebook, guide_id: int, cache_path=Path("./cache")
) -> dict:
    cache_path.mkdir(exist_ok=True)
    response_urls = []
    # Datetime columns intentionally left as string since that's how they
    # will be served.
    schedule_tracks, urls = guidebook.get_schedule_tracks(guide_id)
    df = pd.DataFrame(schedule_tracks)
    df.to_feather(cache_path / "schedule_tracks.feather")
    response_urls += urls

    sessions, urls = guidebook.get_sessions(guide_id)
    df = pd.DataFrame(sessions)
    df.to_feather(cache_path / "sessions.feather")
    response_urls += urls

    locations, urls = guidebook.get_locations(guide_id)
    df = pd.DataFrame(locations)
    df.to_feather(cache_path / "locations.feather")
    response_urls += urls

    return {"status": "success", "urls": response_urls}


def get_credentials() -> Credentials:
    SCOPES = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.file",
        "https://www.googleapis.com/auth/drive",
    ]

    return google.oauth2.service_account.Credentials.from_service_account_file(
        "./consign-464501-ef12de24c5f0.json", scopes=SCOPES
    )


@app.get("/")
async def root():
    return {"message": "Digital signage caching server"}


def convert_for_response(value):
    if isinstance(value, np.ndarray):
        return value.tolist()

    return value


def build_df_response(df: pd.DataFrame) -> dict:
    return {
        "count": len(df),
        "next": None,
        "previous": None,
        "results": [
            {key: convert_for_response(val) for key, val in row.items()}
            for i, row in df.iterrows()
        ],
    }


@app.get("/api/v1.1/schedule-tracks")
@logger.catch
async def get_schedule_tracks(guide: int | None = None):
    df = pd.read_feather("./cache/schedule_tracks.feather")
    if guide is not None:
        df = df[df.guide == guide]

    return build_df_response(df)


@app.get("/api/v1.1/locations")
@logger.catch
async def get_locations(guide: int | None = None):
    df = pd.read_feather("./cache/locations.feather")
    if guide is not None:
        df = df[df.guide == guide]

    return build_df_response(df)


@app.get("/api/v1.1/sessions")
@logger.catch
async def get_sessions(guide: int | None = None):
    df = pd.read_feather("./cache/sessions.feather")
    if guide is not None:
        df = df[df.guide == guide]

    return build_df_response(df)


@app.post("/svc/pull/{con}")
@logger.catch
async def pull_guidebook_data(con: str):
    guidebook = Guidebook(os.environ["GUIDEBOOK_API_KEY"])

    try:
        guide_id = int(con)
    except Exception:
        guide_id = {"zenkaikon25": 208676, "otakon24": 191558}[con]

    result = update_guidebook_cache(guidebook, guide_id)
    return result


@app.get("/svc/log")
@logger.catch
async def get_log():
    log_path = Path("./cache/log.txt")
    if log_path.exists():
        return {
            "log": [json.loads(line) for line in open(log_path).readlines()]
        }
    else:
        return {"log": []}


def main():
    credentials = get_credentials()
    drive_svc = build("drive", "v3", credentials=credentials)
    sheets_svc = build("sheets", "v4", credentials=credentials)

    result = (
        drive_svc.files()
        .list(pageSize=10, fields="nextPageToken, files(id, name)")
        .execute()
    )
    file_id = result["files"][0]["id"]

    con_spreadsheet = ConSpreadsheet(sheets_svc, file_id)
    # guidebook = Guidebook(con_spreadsheet.get_config("guidebook-api-key"))
    # guide_id = con_spreadsheet.get_config("guidebook-guide-id")
    # update_guidebook_cache(guidebook, guide_id)


if __name__ == "__main__":
    main()
