import requests
from datetime import timedelta

from config.settings import IPOS_TOKEN
from utils.logger import setup_logger
from utils.time_utils import to_timestamp
from database.db_connection import safe_execute

from database.insert_operations import (
    insert_invoice_record,
    insert_store_address_if_any,
    insert_invoice_payment,
    insert_sale_detail,
    insert_payment_methods
)

logger = setup_logger("ipos_service")

def fetch_ipos_data_range(brand_id, start_date, end_date, cursor_holder):
    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "access_token": IPOS_TOKEN
    }

    for day in range((end_date - start_date).days + 1):
        day_start = start_date + timedelta(days=day)
        logger.info(f"Fetching iPOS data for brand {brand_id} on {day_start.date()}")

        params = {
            "brand_id": brand_id,
            "tran_date": to_timestamp(day_start),
            "page": 1,
            "page_size": 1000
        }

        response = requests.get(url="https://dwapis.ipos.vn/api/v1/partners/get-sales?", params=params, headers=headers)
        data = response.json()

        total_pages = data.get("total_pages", 1)

        for page in range(1, total_pages + 1):
            if page > 1:
                params["page"] = page
                response = requests.get(url="https://dwapis.ipos.vn/api/v1/partners/get-sales?", params=params, headers=headers)
                data = response.json()

            for record in data["data"]:
                try:
                    safe_execute(
                        f"SELECT * FROM [Menas_DB].[fabi].[sales_invoices] WHERE sale_id = '{record['sale_id']}'"
                    , cursor_holder)
                    existing = cursor_holder['cursor'].fetchall()

                    if len(existing) < 1:
                        insert_invoice_record(cursor_holder, record)
                        insert_store_address_if_any(cursor_holder, record)
                        insert_invoice_payment(cursor_holder, record)
                        insert_sale_detail(cursor_holder, record)
                        insert_payment_methods(cursor_holder, record)

                    cursor_holder['cursor'].commit()
                except Exception as e:
                    logger.error(f"Error inserting sale {record['sale_id']}: {e}")
