from database.db_connection import get_db_connection
from services.ipos_service import fetch_ipos_data_range
from utils.logger import setup_logger
from utils.time_utils import get_yesterday_start
from database.db_connection import safe_execute

logger = setup_logger("main")

def run_sync():
    logger.info("Starting sync process...")
    conn = get_db_connection()
    cursor_holder = {"conn": conn, "cursor": conn.cursor()}
    safe_execute("SELECT MAX(sale_date) FROM fabi.sales_invoice_items",  cursor_holder)
    start_date = cursor_holder['cursor'].fetchone()[0]
    end_date = get_yesterday_start()
    for brand_id in ['BRAND-ISIV', 'BRAND-2XCL', 'BRAND-GH3C']:
        fetch_ipos_data_range(brand_id, start_date, end_date, cursor_holder)
    logger.info("Sync process completed.")

if __name__ == "__main__":
    run_sync()