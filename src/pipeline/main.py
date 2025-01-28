#!/usr/bin/env python3

import httpx
import psycopg2
from prefect import flow, get_run_logger, task

@task
def retrieve_inventory(
    base_url: str,
    path: str,
    secure: bool,
):
    logger = get_run_logger()
    if secure:
        url = f"https://{base_url}{path}"
    else:
        url = f"http://{base_url}{path}"
    response = httpx.get(url)
    response.raise_for_status()
    inventory_stats = response.json()
    logger.info(inventory_stats)
    return inventory_stats

@flow
def collect_petstore_inventory(
    base_url: str = "petstore.swagger.io",
    path: str = "/v2/store/inventory",
    secure: bool = True,
    db_host: str="localhost",
    db_user: str="root",
    db_pass: str="root",
    db_name: str="petstore",
):
    inventory_stats = retrieve_inventory(
        base_url=base_url,
        path=path,
        secure=secure
    )

    inventory_stats = clean_stats_data.fn(
        inventory_stats=inventory_stats
    )

    insert_to_db(
        inventory_stats,
        db_host,
        db_user,
        db_pass,
        db_name,
    )

@task
def clean_stats_data(inventory_stats: dict) -> dict:
    return {
        "sold": inventory_stats.get("sold", 0) + inventory_stats.get("Sold", 0),
        "available": inventory_stats.get("available", 0) + inventory_stats.get("Available", 0),
        "unavailable": inventory_stats.get("unavailable", 0) + inventory_stats.get("Unavailable", 0),
        "pending": inventory_stats.get("pending", 0) + inventory_stats.get("Pending", 0),
    }

@task
def insert_to_db(
    inventory_stats: dict,
    db_host: str,
    db_user: str,
    db_pass: str,
    db_name: str,
):
    with psycopg2.connect(
        host=db_host,
        user=db_user,
        password=db_pass,
        dbname=db_name
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO inventory_history (fetch_timestamp, sold, available, unavailable, pending)
                VALUES (now(), %(sold)s, %(available)s, %(unavailable)s, %(pending)s)
                """,
                inventory_stats
            )
    logger = get_run_logger()
    logger.info("Data has been inserted to the database.")

def main():
    collect_petstore_inventory.serve("petstore-collection-deployment")
    
if __name__ == "__main__":
    main()
