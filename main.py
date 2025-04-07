import logging
import logging.config
import multiprocessing
import pathlib
import re

import requests
import pandas as pd
import sqlite3


METADATA_URL = (
    "https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items"
)

DIR = str(pathlib.Path(__file__).parent)
CONTROL_DATABASE_PATH = f"{DIR}/control.db"

logging.config.dictConfig(
    {
        "version": 1,
        "disable_existing_loggers": False,
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": "INFO",
                "formatter": "simple",
                "stream": "ext://sys.stdout",
            },
        },
        "formatters": {
            "simple": {
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            },
        },
        "loggers": {
            "": {
                "handlers": ["console"],
                "level": "INFO",
                "propagate": True,
            },
        },
    }
)
logger = logging.getLogger(__name__)


def download_metadata(url):
    logger.info("Downloading metadata")
    req = requests.get(url)
    req.raise_for_status()
    data = req.json()
    logger.info("Finished downloading metadata")
    return data


def filter_metadata_by_keyword(pattern, data):
    for row in data:
        keywords = row["keyword"]
        for keyword in keywords:
            if re.search(pattern, keyword, flags=re.IGNORECASE):
                yield row
                break


def convert_to_snake_case(val: str) -> str:
    # remove apostrophes
    val = re.sub("'", "", val)
    # set multiple spaces to one
    val = re.sub(" +", " ", val)
    # strip leading and trailing whitespace
    val = val.strip()
    # convert space to underscore
    val = re.sub(" ", "_", val)
    val = val.lower()
    return val


def get_latest_completed_download(id: str):
    logger.info(f"Finding latest metadata for %s", id)
    with sqlite3.connect(CONTROL_DATABASE_PATH) as con:
        cur = con.cursor()
        sql = """
        select last_modified
        from control
        where 
            id = ?
            and status = 'completed'
        """
        vals = (id,)
        cur.execute(sql, vals)
        return cur.fetchone()


def get_download_status(id: str, last_modified: str):
    logger.info("Finding download status for %s at %s", id, last_modified)
    with sqlite3.connect(CONTROL_DATABASE_PATH) as con:
        cur = con.cursor()
        sql = """
        select status
        from control
        where
            id = ?
            and last_modified = ?
        """
        vals = (id, last_modified)
        cur.execute(sql, vals)
        return cur.fetchone()


def update_download_status(id: str, status: str, last_modified: str) -> None:
    logger.info(
        "Updating download status for %s at %s with %s", id, last_modified, status
    )
    with sqlite3.connect(CONTROL_DATABASE_PATH) as con:
        cur = con.cursor()
        sql = f"""
        update control
        set status = ?
        where
            id = ?
            and last_modified = ?
        """
        vals = (status, id, last_modified)
        cur.execute(sql, vals)
        con.commit()


def insert_new_download_status(id: str, status: str, last_modified: str) -> None:
    logger.info(
        "Inserting download status for %s at %s with %s", id, last_modified, status
    )
    with sqlite3.connect(CONTROL_DATABASE_PATH) as con:
        cur = con.cursor()
        sql = """
        insert into control (id, status, last_modified)
        values (?, ?, ?)
        """
        vals = (id, status, last_modified)
        cur.execute(sql, vals)
        con.commit()


def download_csv(id, url, last_modified):
    logger.info("Starting download for %s at %s", id, last_modified)
    status = get_download_status(id, last_modified)
    if not status:
        insert_new_download_status(id, "processing", last_modified)
    elif status in ("processing", "completed"):
        logger.info("Skipping because status for %s is %s", id, status)
        pass
    else:
        update_download_status(id, "processing", last_modified)
    try:
        logger.info("Downloading %s at %s", id, last_modified)
        df = pd.read_csv(url)
        df = df.rename(columns=convert_to_snake_case)
        filename = url.split("/")[-1]
        path = f"{DIR}/data/{filename}"
        df.to_csv(path, index=False)
        update_download_status(id, "completed", last_modified)
        logger.info("Done downloading %s at %s", id, last_modified)
    except Exception as e:
        logger.error("Failed downloading %s at %s", id, last_modified, exc_info=True)
        update_download_status(id, "failed", last_modified)


def get_download_list(pattern):
    result = []
    metadata = download_metadata(METADATA_URL)
    for row in filter_metadata_by_keyword(pattern, metadata):
        id = row["identifier"]
        url = row["distribution"][0]["downloadURL"]
        last_modified = row["modified"]
        latest = get_latest_completed_download(id)
        if not latest or latest[0] < last_modified:
            logger.info("Adding %s at %s to download list", id, last_modified)
            result.append((id, url, last_modified))
        else:
            logger.info("Skipping %s at %s", id, last_modified)
    return result


def download_all(pattern):
    links = get_download_list(pattern)
    with multiprocessing.Pool() as pool:
        pool.starmap(download_csv, links)


if __name__ == "__main__":
    pattern = r"hospital"
    download_all(pattern)
