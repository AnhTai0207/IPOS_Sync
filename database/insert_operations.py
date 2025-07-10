from datetime import datetime
from utils.logger import setup_logger
from utils.time_utils import from_timestamp
from database.db_connection import safe_execute

logger = setup_logger("insert_operations")

def insert_invoice_record(cursor_holder, record):
    logger.debug(f"Inserting invoice record: {record['sale_id']}")
    q = f"""
    INSERT INTO [Menas_DB].[fabi].[sales_invoices] (
        datastate, sale_id, origin_sale_id, tran_date, source_fb_id, amount_org,
        amount, amount_discount_price, amount_discount_detail, amount_discount_extra,
        amount_service_charge, amount_vat, coupon_amount, coupon_amount_paid,
        ship_charge, commission_amount, partner_marketing_amount, fb_store_id,
        pos_type, store_id, brand_id, company_id, created_at, updated_at, is_web,
        is_group_bill, final_amount, id, created_by, updated_by, deleted
    ) VALUES (
        {record['datastate']},
        '{record['sale_id']}',
        '{record['origin_sale_id']}',
        '{datetime.fromtimestamp(record['tran_date'])}',
        '{record['source_fb_id']}',
        {record['amount_org']},
        {record['amount']},
        {record['amount_discount_price']},
        {record['amount_discount_detail']},
        {record['amount_discount_extra']},
        {record['amount_service_charge']},
        {record['amount_vat']},
        {record['coupon_amount']},
        {record['coupon_amount_paid']},
        {record['ship_charge']},
        {record['commission_amount']},
        {record['partner_marketing_amount']},
        '{record['fb_store_id']}',
        '{record['pos_type']}',
        '{record['store_id']}',
        '{record['brand_id']}',
        '{record['company_id']}',
        '{datetime.fromtimestamp(record['created_at'])}',
        '{datetime.fromtimestamp(record['updated_at'])}',
        {int(record['is_web'] == 'True')},
        {int(record['is_group_bill'] == 'True')},
        {record['final_amount']},
        '{record['id']}',
        '{record['created_by']}',
        '{record['updated_by']}',
        {int(record['deleted'] == 'True')}
    )
    """
    safe_execute(q, cursor_holder)


def insert_store_address_if_any(cursor_holder, record):
    logger.debug(f"Inserting store address for: {record['sale_id']}")

    store_address = record.get("sale", {}).get("store_address")
    if not store_address or store_address == {}:
        return

    try:
        q = f"""
        INSERT INTO [Menas_DB].[fabi].[StoreAddress] (
            Latitude, Longitude, LocationType,
            Viewport_Northeast_Lat, Viewport_Northeast_Lng,
            Viewport_Southwest_Lat, Viewport_Southwest_Lng,
            PlaceID, GlobalPlusCode, CompoundPlusCode,
            FormattedAddress, StreetNumber, RouteLongName, RouteShortName,
            DistrictLongName, DistrictShortName, CityLongName, CityShortName,
            CountryLongName, CountryShortName
        ) VALUES (
            {float(store_address["geometry"]["location"]["lat"])},
            {float(store_address["geometry"]["location"]["lng"])},
            N'{store_address["geometry"]["location_type"]}',
            {float(store_address["geometry"]["viewport"]["northeast"]["lat"])},
            {float(store_address["geometry"]["viewport"]["northeast"]["lng"])},
            {float(store_address["geometry"]["viewport"]["southwest"]["lat"])},
            {float(store_address["geometry"]["viewport"]["southwest"]["lng"])},
            N'{store_address["place_id"]}',
            N'{store_address["plus_code"]["global_code"]}',
            N'{store_address["plus_code"]["compound_code"]}',
            N'{store_address["formatted_address"]}',
            N'{store_address["address_details"]["street_number"]["long_name"]}',
            N'{store_address["address_details"]["route_info"]["long_name"]}',
            N'{store_address["address_details"]["route_info"]["short_name"]}',
            N'{store_address["address_details"]["district_info"]["long_name"]}',
            N'{store_address["address_details"]["district_info"]["short_name"]}',
            N'{store_address["address_details"]["city_info"]["long_name"]}',
            N'{store_address["address_details"]["city_info"]["short_name"]}',
            N'{store_address["address_details"]["country_info"]["long_name"]}',
            N'{store_address["address_details"]["country_info"]["short_name"]}'
        )
        """
        cursor_holder['cursor'].execute(q)
    except Exception as e:
        logger.warning(f"Store address insert skipped (may already exist): {e}")

def insert_invoice_payment(cursor_holder, record):
    logger.debug(f"Inserting invoice payment: {record['sale_id']}")
    sale = record['sale']
    if sale.get("note") and "'" in sale["note"]:
        sale["note"] = sale["note"].replace("'", "''")
    if "'" in sale["store_name"]:
        sale["store_name"] = sale["store_name"].replace("'", "''")
    if "'" in sale["user_name"]:
        sale["user_name"] = sale["user_name"].replace("'", "''")

    vat_tran_date = str(sale.get("vat_tran_date"))

    if len(vat_tran_date) > 10:
        vat_tran_date = int(vat_tran_date[:10])

        vat_tran_date = from_timestamp(float(vat_tran_date))
    elif vat_tran_date == '0':
        vat_tran_date = ""



    if sale.get("membership_birthday"):
        sale["membership_birthday"] = from_timestamp(sale["membership_birthday"])
    else:
        sale["membership_birthday"] = ""

    q = f"""
    INSERT INTO [Menas_DB].[fabi].[invoice_payments] (
        vat, note, pos_id, status, area_id, is_temp, room_id,
        sale_id, tran_id, tran_no, user_id, shift_id, store_id,
        date_last, hour_last, source_id, tran_date, user_name, get_amount,
        store_name, customer_id, device_code, minute_last, number_male, print_count,
        vat_content, vat_tran_no, amount_point, coupon_count, shift_charge, source_fb_id,
        vat_tax_code, coupon_amount, exchange_rate, membership_id, number_female,
        number_people, return_amount, StoreAddressID, vat_bank_name, vat_tran_date,
        card_info_code, discount_extra, origin_sale_id, payment_status, service_charge,
        store_latitude, tran_date_orig, dinner_table_id, store_longitude, address_delivery,
        currency_type_id, vat_bank_account, vat_company_name, commission_amount,
        foodbook_order_id, membership_id_new, source_voucher_id, vat_customer_name,
        membership_type_id, membership_voucher, vat_customer_email, vat_customer_phone,
        vat_payment_method, membership_birthday, vat_customer_address,
        partner_marketing_amount, vat_sign, session_id, vat_amount, last_version,
        promotion_id, station_code, tran_no_temp, deposit_amount, promotion_name,
        pr_key_bookings, vat_identity_card, vat_amount_reverse, amount_discount_extra2,
        sale_sign
    ) VALUES (
        {sale['vat']}, N'{sale['note']}', {sale['pos_id']}, {sale['status']},
        '{sale['area_id']}', {int(sale['is_temp'] is not None)}, '{sale['room_id']}',
        '{sale['sale_id']}', '{sale['tran_id']}', '{sale['tran_no']}',
        '{sale['user_id']}', '{sale['shift_id']}', '{sale['store_id']}',
        '{from_timestamp(sale['date_last'])}', {sale['hour_last']},
        '{sale['source_id']}', '{from_timestamp(sale['tran_date'])}',
        N'{sale['user_name']}', {sale['get_amount']}, N'{sale['store_name']}',
        '{sale['customer_id']}', '{sale['device_code']}', {sale['minute_last']},
        {0 if sale['number_male'] is None else sale['number_male']},
        {sale['print_count']}, '{sale['vat_content']}', '{sale['vat_tran_no']}',
        {sale['amount_point']}, {sale['coupon_count']}, {sale['shift_charge']},
        '{sale['source_fb_id']}', '{sale['vat_tax_code']}', {sale['coupon_amount']},
        {sale['exchange_rate']}, '{sale['membership_id']}',
        {0 if sale['number_female'] is None else sale['number_female']},
        {sale['number_people']}, {sale['return_amount']}, 1,
        '{sale['vat_bank_name']}', '{vat_tran_date}', '{sale['card_info_code']}',
        {sale['discount_extra']}, '{sale['origin_sale_id']}',
        '{sale['payment_status']}', {sale['service_charge']},
        {0 if sale['store_latitude'] is None else sale['store_latitude']},
        '{from_timestamp(sale['tran_date_orig'])}', '{sale['dinner_table_id']}',
        {0 if sale['store_longitude'] is None else sale['store_longitude']},
        '{sale['address_delivery']}', '{sale['currency_type_id']}',
        '{sale['vat_bank_account']}', N'{sale['vat_company_name']}',
        {sale['commission_amount']}, '{sale['foodbook_order_id']}',
        '{sale['membership_id_new']}', '{sale['source_voucher_id']}',
        N'{sale['vat_customer_name']}', '{sale['membership_type_id']}',
        {sale['membership_voucher']}, '{sale['vat_customer_email']}',
        '{sale['vat_customer_phone']}', '{sale['vat_payment_method']}',
        '{sale['membership_birthday']}', N'{sale['vat_customer_address']}',
        {sale['partner_marketing_amount']}, '{sale['vat_sign']}',
        '{sale['session_id']}', {sale['vat_amount']}, '{sale['last_version']}',
        '{sale['promotion_id']}', '{sale['station_code']}', '{sale['tran_no_temp']}',
        {sale['deposit_amount']}, N'{sale['promotion_name']}',
        '{sale['pr_key_bookings']}', '{sale['vat_identity_card']}',
        {sale['vat_amount_reverse']}, {sale['amount_discount_extra2']},
        '{sale['sale_sign']}'
    )
    """

    safe_execute(q, cursor_holder)


def insert_sale_detail(cursor_holder, record):
    logger.debug(f"Inserting sale details: {record['sale_id']}")
    for item in record["sale_detail"]:
        if "'" in item["description"]:
            item["description"] = item["description"].replace("'", "''")
        if item.get("promotion_name") and "'" in item["promotion_name"]:
            item["promotion_name"] = item["promotion_name"].replace("'", "''")

        q = f"""
        INSERT INTO [Menas_DB].[fabi].[sales_invoice_items] (
            sale_detail_id, sale_id, item_id, description, unit_id, quantity,
            price_org, price_sale, cost_price, discount, discount_amount, amount,
            vat, tax_amount, amount_service_charge, amount_discount_extra,
            amount_discount_on_price, commission_amount, coupon_amount,
            partner_marketing_amount, distribute_discount_extra2, amount_point,
            payment_type, payment, tran_id, ship_charge, promotion_id, promotion_name,
            user_id, shift_id, sale_date, end_date, hour_start, minute_start,
            hour_end, minute_end, is_kit, is_gift, is_eat_with, package_id,
            parent_item_id, item_id_mapping, source_fb_id, fix, is_fc, is_set,
            number, ots_ta, discount_vat, pr_key_order, stop_service, printed_label,
            is_print_label, temp_calculate, topping_item_id, quantity_at_temp,
            parent_item_index, parent_item_price, unit_id_secondary, quantity_secondary,
            tax_amount_reverse, topping_item_price, discount_vat_amount, parent_item_quantity,
            topping_item_quantity, amount_discount_values, parent_item_quantity_secondary,
            topping_item_quantity_secondary, amount_vat, unit_name, is_invoice,
            is_service, list_order
        ) VALUES (
            '{item['sale_detail_id']}', '{item['sale_id']}', '{item['item_id']}',
            N'{item['description']}', '{item['unit_id']}', {item['quantity']},
            {item['price_org']}, {item['price_sale']}, {item['cost_price']},
            {item['discount']}, {item['discount_amount']}, {item['amount']},
            {item['tax']}, {item['tax_amount']}, {item['amount_service_charge']},
            {item['amount_discount_extra']}, {item['amount_discount_on_price']},
            {item['commission_amount']}, {item['coupon_amount']},
            {item['partner_marketing_amount']}, {item['distribute_discount_extra2']},
            {item['amount_point']}, '{item['payment_type']}', '{item['payment']}',
            '{item['tran_id']}', {item['ship_charge']}, '{item['promotion_id']}',
            '{item['promotion_name']}', '{item['user_id']}', '{item['shift_id']}',
            '{from_timestamp(item['sale_date'])}', '{from_timestamp(item['end_date'])}',
            {item['hour_start']}, {item['minute_start']}, {item['hour_end']},
            {item['minute_end']}, {item['is_kit']}, {item['is_gift']}, {item['is_eat_with']},
            '{item['package_id']}', '{item['parent_item_id']}', '{item['item_id_mapping']}',
            '{item['source_fb_id']}', {item['fix']}, {item['is_fc']}, {item['is_set']},
            {item['number']}, {item['ots_ta']}, {item['discount_vat']}, {item['pr_key_order']},
            {item['stop_service']}, {item['printed_label']}, {item['is_print_label']},
            {item['temp_calculate']}, '{item.get('topping_item_id')}', {item['quantity_at_temp']},
            '{item['parent_item_index']}', {0 if item['parent_item_price'] is None else item['parent_item_price']},
            '{item['unit_id_secondary']}', {item['quantity_secondary']},
            {item['tax_amount_reverse']},
            {0 if item['topping_item_price'] is None else item['topping_item_price']},
            {item['discount_vat_amount']},
            {0 if item['parent_item_quantity'] is None else item['parent_item_quantity']},
            {0 if item['topping_item_quantity'] is None else item['topping_item_quantity']},
            {item['amount_discount_values']},
            {0 if item['parent_item_quantity_secondary'] is None else item['parent_item_quantity_secondary']},
            {0 if item['topping_item_quantity_secondary'] is None else item['topping_item_quantity_secondary']},
            {item['amount_vat']}, '{item['unit_name']}', {item['is_invoice']},
            {item['is_service']}, {item['list_order']}
        )
        """
        safe_execute(q, cursor_holder)


def insert_payment_methods(cursor_holder, record):
    logger.debug(f"Inserting payment methods: {record['sale_id']}")
    for p in record["sale_payment_method"]:
        q = f"""
        INSERT INTO [Menas_DB].[fabi].[sales_invoice_payments] (
            sale_payment_method_id, sale_id, user_id, shift_id, trace_no,
            tran_date, tran_hour, tran_minute, amount, amount_orig,
            amount_orig_get, amount_orig_return, exchange_rate,
            exchange_rate_return, currency_type_id, currency_type_id_return,
            payment_method_id, source_fb_id, list_order, payment_type
        ) VALUES (
            '{p['sale_payment_method_id']}', '{p['sale_id']}', '{p['user_id']}',
            '{p['shift_id']}', '{p['trace_no']}', '{from_timestamp(p['tran_date'])}',
            {p['tran_hour']}, {p['tran_minute']}, {p['amount']}, {p['amount_orig']},
            {p['amount_orig_get']}, {p['amount_orig_return']}, {p['exchange_rate']},
            {p['exchange_rate_return']}, '{p['currency_type_id']}',
            '{p['currency_type_id_return']}', '{p['payment_method_id']}',
            '{p['source_fb_id']}', {p['list_order']}, '{p['payment_type']}'
        )
        """
        safe_execute(q, cursor_holder)
