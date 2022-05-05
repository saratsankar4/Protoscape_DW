from config import config as cfg
from db_connection import read_from_db, save_to_db
import os, os.path
import numpy as np
import pandas as pd
from datetime import datetime, time
import schedule
import time

table_details = cfg('table_details')

db_details = cfg('db')

f_path = cfg('folder_path')

files_name = cfg('files_name')

folder_path = f_path['folder_path']

sheet_names = cfg('sheet_names')


# to get modified date of file
def get_mod_time(a):
    modificationtime = time.strftime('%Y-%m-%d %H:%M:%S',
                                     time.localtime(os.path.getmtime(os.path.join(folder_path, a))))
    return modificationtime


# To Insert  inventory_data
def insert_blanket_sales(files):
    # files = os.path.join(folder_path, files)
    df = pd.read_excel(files, sheet_name=sheet_names['sheet_1'])

    cur_timestamp = get_mod_time(files)
    df['modified_time'] = cur_timestamp

    df.rename(columns={"Customer": "customer",
                       "Blanket": "blanket",
                       "PO": "po",
                       "Internal Item": "internal_item",
                       "Internal Item Description": "internal_item_desc",
                       "Activation Date": "activation_date",
                       "Expiration Date.": "expiration_date",
                       "Max Qty Agreed": "max_qty_agreed",
                       "Fulfilled Qty": "fulfilled_qty",
                       "Released Qty": "released_qty",
                       "Unreleased Quantity": "unreleased_qty",
                       "Last Updated": "last_updated",
                       "Last Updated By": "last_updated_by",
                       "Status": "status",
                       "Header Activation Date": "header_act_date",
                       "Header Expiration Date": "header_exp_date",
                       "UNIT_PRICE_IN": "unit_price_in",
                       "TRANSACTIONAL_CURR_CODE": "trnsn_curr_code",
                       "Line": "line",
                       "modified_time": "file_modified_time"
                       },
              inplace=True)

    df['file_row_no'] = np.arange(df.shape[0])

    file_name = table_details['blanket_data'] + ".csv"
    # df.to_csv(file_name, index=False)
    save_to_db(table_details['blanket_data'], "append", cfg('db'), df)


def insert_item_master(files):
    # files = os.path.join(folder_path, files)
    df = pd.read_excel(files, sheet_name=sheet_names['sheet_2'])
    cur_timestamp = get_mod_time(files)
    df['modified_time'] = cur_timestamp

    df.rename(columns={
        "Organization": "organization",
        "Item": "item",
        "PL": "pl",
        "Description": "description",
        "Primary UOM": "primary_uom",
        "Supplier": "supplier",
        "Buyer": "buyer",
        "WIP Supply Type": "wip_supply_type",
        "Item Status": "item_status",
        "LT": "lt",
        "Frozen Cost": "frozen_cost",
        "Item_Type": "item_type",
        "modified_time": "file_modified_time"
    }, inplace=True)

    df['file_row_no'] = np.arange(df.shape[0])

    # file_name = table_details['item_master_data'] + ".csv"
    # df.to_csv(file_name, index=False)
    save_to_db(table_details['item_master_data'], "append", cfg('db'), df)


def insert_inventory_onhand(files):
    # files = os.path.join(folder_path, files)
    df = pd.read_excel(files, sheet_name=sheet_names['sheet_3'])
    cur_timestamp = get_mod_time(files)
    df['modified_time'] = cur_timestamp

    df.rename(columns={
        "Org": "org",
        "Subinventory": "subinventory",
        "Locator": "locator",
        "Item": "item",
        "Item Revision": "item_revision",
        "Item Description": "item_desc",
        "Location Reference": "location_reference",
        "UOM": "uom",
        "Quantity": "quantity",
        "Supplier Category": "supplier_category",
        "Final Assembly Cell": "final_assembly_cell",
        "Item Cost": "item_cost",
        "Extended Cost": "extended_cost",
        "Org Item Class": "org_item_class",
        "planning_make_buy_code": "planning_make_buy_code",
        "modified_time": "file_modified_time"
    },
        inplace=True)

    df['file_row_no'] = np.arange(df.shape[0])

    file_name = table_details['inventory_onhand_data'] + ".csv"
    # df.to_csv(file_name, index=False)
    save_to_db(table_details['inventory_onhand_data'], "append", cfg('db'), df)


def insert_past_12_shipment(files):
    # files = os.path.join(folder_path, files)
    df_1 = pd.read_excel(files, sheet_name=sheet_names['sheet_4'])
    df_1 = df_1[df_1['Customer'].notna()]
    df_1['file_row_no'] = np.arange(df_1.shape[0])
    df_1['bus_unit'] = 'DC & BLDC'
    df_2 = pd.read_excel(files, sheet_name=sheet_names['sheet_5'])
    df_2 = df_2[df_2['Customer'].notna()]
    df_2['file_row_no'] = np.arange(df_2.shape[0])
    df_2['bus_unit'] = 'Stepper'
    df_3 = pd.concat([df_1, df_2], ignore_index=True)
    cur_timestamp = get_mod_time(files)
    df_3['modified_time'] = cur_timestamp

    df_3.rename(columns={
        "Customer": "customer",
        "PO": "po",
        "Order Type": "order_type",
        "Order #": "order_no",
        "Line #": "line_no",
        "Scheduled Date": "scheduled_date",
        "Promise Date": "promise_date",
        "Ordered Date": "ordered_date",
        "Request Date": "request_date",
        "Shipped Date.": "shipped_date",
        "Assy. Cell": "cell",
        "Item": "item",
        "Item Description": "item_descr",
        "Ordered Item": "ordered_item",
        "Open Quantity": "open_qty",
        "Shipped Qty": "shipped_qty",
        "Selling Price (Entered)": "selling_price_entered",
        "Extended Ship Value (Entered)": "extd_ship_val_entered",
        "Extended Cost": "extended_cost",
        "Invoice Number": "invoice_no",
        "Invoice Date": "invoice_date",
        "Item Category": "item_category",
        "Item Class": "item_class",
        "Customer Class": "customer_class",
        "Salesperson": "salesperson",
        "Requested Days": "requested_days",
        "Currency (Entered)": "currency_entered",
        "Functional Currency": "functional_currency",
        "Exchange  Date": "exchange_date",
        "Exchange Rate": "exchange_rate",
        "Functional Selling Price": "fnctl_selling_price",
        "Extended Functional Ship Value": "extd_fnctl_ship_val",
        "modified_time": "file_modified_time"
    },
        inplace=True)

    # file_name = table_details['past_12_months_shipment_data'] + ".csv"
    # df_3.to_csv(file_name, index=False)
    save_to_db(table_details['past_12_months_shipment_data'], "append", cfg('db'), df_3)


def insert_blanket_po(files):
    # files = os.path.join(folder_path, files)
    df = pd.read_excel(files, sheet_name=sheet_names['sheet_6'])
    cur_timestamp = get_mod_time(files)
    df['modified_time'] = cur_timestamp

    df.rename(columns={
        "Inventory_Organization": "inventory_org",
        "Authorization Status": "auth_status",
        "PO": "po",
        "PO Line": "po_line",
        "Supplier": "supplier",
        "Supplier Site Code": "supplier_site_code",
        "Buyer": "buyer",
        "PO Line Creation Date": "po_line_creation_date",
        "Amount Agreed Base": "amt_agreed_base",
        "Expiration Date": "exp_date",
        "Item": "item",
        "Item Description": "item_desc",
        "Agreed Unit Price": "agreed_unit_price",
        "Quantity Agreed": "quantity_agreed",
        "Quantity Released": "quantity_released",
        "Kanban": "kanban",
        "Kanban Size": "kanban_size",
        "Percent Released": "percent_released",
        "Number of Kanban Remaining": "np_of_kb_remaining",
        "Org Item Class": "org_item_class",
        "PO Header Description": "po_header_desc",
        "modified_time": "file_modified_time"
    }, inplace=True)

    df['file_row_no'] = np.arange(df.shape[0])

    file_name = table_details['blanket_po_data'] + ".csv"
    # df.to_csv(file_name, index=False)
    save_to_db(table_details['blanket_po_data'], "append", cfg('db'), df)


def insert_standard_release_po(files):
    # files = os.path.join(folder_path, files)
    df = pd.read_excel(files, sheet_name=sheet_names['sheet_7'])
    cur_timestamp = get_mod_time(files)
    df['modified_time'] = cur_timestamp

    df.rename(columns={
        "Shipment Creation Date": "shipment_creation_date",
        "PO": "po",
        "Release #": "release_number",
        "Supplier": "supplier",
        "Item$ITEM": "item",
        "Description": "description",
        "Unit Price": "unit_price",
        "Base_Currency_Code": "base_currency_code",
        "Ordered Quantity": "ordered_quantity",
        "Received Quantity": "received_quantity",
        "Outstanding Qty": "outstanding_qty",
        "UOM": "uom",
        "PO_Type": "po_type",
        "Outstanding Amount": "outstanding_amount",
        "Frozen Cost": "frozen_cost",
        "Requested Date": "requested_date",
        "Promised Date": "promised_date",
        "Approved_Code": "approved_code",
        "Buyer": "buyer",
        "A$Ship_To_Organization": "ship_to_organization",
        "PL": "pl",
        "Shipment Status": "shipment_status",
        "modified_time": "file_modified_time"
    }, inplace=True)

    df['file_row_no'] = np.arange(df.shape[0])

    # file_name = table_details['standard_release_po_data'] + ".csv"
    # df.to_csv(file_name, index=False)
    save_to_db(table_details['standard_release_po_data'], "append", cfg('db'), df)


def insert_pending_iqa(files):
    # files = os.path.join(folder_path, files)
    df = pd.read_excel(files, sheet_name=sheet_names['sheet_8'])
    cur_timestamp = get_mod_time(files)
    df['modified_time'] = cur_timestamp

    df.rename(columns={
        "Item": "item",
        "Item Description": "item_desc",
        "Quantity": "quantity",
        "modified_time": "file_modified_time",

    }, inplace=True)

    df['file_row_no'] = np.arange(df.shape[0])

    # file_name = table_details['pending_iqa_data'] + ".csv"
    # df.to_csv(file_name, index=False)
    save_to_db(table_details['pending_iqa_data'], "append", cfg('db'), df)


def insert_pending_sc(files):
    # files = os.path.join(folder_path, files)
    df = pd.read_excel(files, sheet_name=sheet_names['sheet_9'])
    cur_timestamp = get_mod_time(files)
    df['modified_time'] = cur_timestamp

    df.rename(columns={
        "Item": "item",
        "Item Description": "item_desc",
        "Quantity": "quantity",
        "modified_time": "file_modified_time",

    }, inplace=True)

    df['file_row_no'] = np.arange(df.shape[0])

    # file_name = table_details['pending_sc_data'] + ".csv"
    # df.to_csv(file_name, index=False)
    save_to_db(table_details['pending_sc_data'], "append", cfg('db'), df)


def insert_monthly_gross(files):
    # files = os.path.join(folder_path, files)
    df = pd.read_excel(files, sheet_name=sheet_names['sheet_10'])
    cur_timestamp = get_mod_time(files)
    df['modified_time'] = cur_timestamp

    df.rename(columns={
        "Inventory Org": "inventory_org",
        "Product Line": "product_line",
        "Buyer": "buyer",
        "Item": "item",
        "Item Description": "item_description",
        "Supplier Category": "supplier_category",
        "Net Onhand": "net_onhand",
        "Gross Rqmts Past Due": "gross_rqmts_past_due",
        "Gross Rqmts Bucket 1": "gross_rqmts_bucket_1",
        "Gross Rqmts Bucket 2": "gross_rqmts_bucket_2",
        "Gross Rqmts Bucket 3": "gross_rqmts_bucket_3",
        "Gross Rqmts Bucket 4": "gross_rqmts_bucket_4",
        "Gross Rqmts Bucket 5": "gross_rqmts_bucket_5",
        "Gross Rqmts Bucket 6": "gross_rqmts_bucket_6",
        "Gross Rqmts Bucket 7": "gross_rqmts_bucket_7",
        "Gross Rqmts Bucket 8": "gross_rqmts_bucket_8",
        "Gross Rqmts Bucket 9": "gross_rqmts_bucket_9",
        "Gross Rqmts Bucket 10": "gross_rqmts_bucket_10",
        "Gross Rqmts Bucket 11": "gross_rqmts_bucket_11",
        "Gross Rqmts Bucket 12": "gross_rqmts_bucket_12",
        "Gross Rqmts Bucket 13": "gross_rqmts_bucket_13",
        "Gross Rqmts Bucket 14": "gross_rqmts_bucket_14",
        "Gross Rqmts Bucket 15": "gross_rqmts_bucket_15",
        "Gross Rqmts Bucket 16": "gross_rqmts_bucket_16",
        "Gross Rqmts Bucket 17": "gross_rqmts_bucket_17",
        "Gross Rqmts Bucket 18": "gross_rqmts_bucket_18",
        "Gross Rqmts Bucket 19": "gross_rqmts_bucket_19",
        "Gross Rqmts Bucket 20": "gross_rqmts_bucket_20",
        "Gross Rqmts Bucket 21": "gross_rqmts_bucket_21",
        "Gross Rqmts Bucket 22": "gross_rqmts_bucket_22",
        "Gross Rqmts Bucket 23": "gross_rqmts_bucket_23",
        "Gross Rqmts Bucket 24": "gross_rqmts_bucket_24",
        "Beyond Requirement": "beyond_requirement",
        "Last Receipt Date": "last_receipt_date",
        "Received, Not Inspected Qty": "received_not_inspected_qty",
        "Inspected, Not Delivered Qty": "inspected_not_delivered_qty",
        "Item Status": "item_status",
        "Open PO Qty": "open_po_qty",
        "Item UOM": "item_uom",
        "Frozen Unit Cost": "frozen_unit_cost",
        "Rundate": "rundate",
        "modified_time": "file_modified_time"
    }, inplace=True)

    df['file_row_no'] = np.arange(df.shape[0])

    # file_name = table_details['monthly_gross_data'] + ".csv"
    # df.to_csv(file_name, index=False)
    save_to_db(table_details['monthly_gross_data'], "append", cfg('db'), df)


def insert_weekly_gross(files):
    # files = os.path.join(folder_path, files)
    df = pd.read_excel(files, sheet_name=sheet_names['sheet_11'])
    cur_timestamp = get_mod_time(files)
    df['modified_time'] = cur_timestamp

    df.rename(columns={
        "Inventory Org": "inventory_org",
        "Product Line": "product_line",
        "Buyer": "buyer",
        "Item": "item",
        "Item Description": "item_description",
        "Supplier Category": "supplier_category",
        "Net Onhand": "net_onhand",
        "Gross Rqmts Past Due": "gross_rqmts_past_due",
        "Gross Rqmts Bucket 1": "gross_rqmts_bucket_1",
        "Gross Rqmts Bucket 2": "gross_rqmts_bucket_2",
        "Gross Rqmts Bucket 3": "gross_rqmts_bucket_3",
        "Gross Rqmts Bucket 4": "gross_rqmts_bucket_4",
        "Gross Rqmts Bucket 5": "gross_rqmts_bucket_5",
        "Gross Rqmts Bucket 6": "gross_rqmts_bucket_6",
        "Gross Rqmts Bucket 7": "gross_rqmts_bucket_7",
        "Gross Rqmts Bucket 8": "gross_rqmts_bucket_8",
        "Gross Rqmts Bucket 9": "gross_rqmts_bucket_9",
        "Gross Rqmts Bucket 10": "gross_rqmts_bucket_10",
        "Gross Rqmts Bucket 11": "gross_rqmts_bucket_11",
        "Gross Rqmts Bucket 12": "gross_rqmts_bucket_12",
        "Gross Rqmts Bucket 13": "gross_rqmts_bucket_13",
        "Gross Rqmts Bucket 14": "gross_rqmts_bucket_14",
        "Gross Rqmts Bucket 15": "gross_rqmts_bucket_15",
        "Gross Rqmts Bucket 16": "gross_rqmts_bucket_16",
        "Gross Rqmts Bucket 17": "gross_rqmts_bucket_17",
        "Gross Rqmts Bucket 18": "gross_rqmts_bucket_18",
        "Gross Rqmts Bucket 19": "gross_rqmts_bucket_19",
        "Gross Rqmts Bucket 20": "gross_rqmts_bucket_20",
        "Gross Rqmts Bucket 21": "gross_rqmts_bucket_21",
        "Gross Rqmts Bucket 22": "gross_rqmts_bucket_22",
        "Gross Rqmts Bucket 23": "gross_rqmts_bucket_23",
        "Gross Rqmts Bucket 24": "gross_rqmts_bucket_24",
        "Beyond Requirement": "beyond_requirement",
        "Last Receipt Date": "last_receipt_date",
        "Received, Not Inspected Qty": "received_not_inspected_qty",
        "Inspected, Not Delivered Qty": "inspected_not_delivered_qty",
        "Item Status": "item_status",
        "Open PO Qty": "open_po_qty",
        "Item UOM": "item_uom",
        "Frozen Unit Cost": "frozen_unit_cost",
        "Rundate": "rundate",
        "modified_time": "file_modified_time"
    }, inplace=True)

    df['file_row_no'] = np.arange(df.shape[0])

    # file_name = table_details['weekly_gross_data'] + ".csv"
    # df.to_csv(file_name, index=False)
    save_to_db(table_details['weekly_gross_data'], "append", cfg('db'), df)


def insert_pso_bldc(files):
    # files = os.path.join(folder_path, files)
    df = pd.read_excel(files, sheet_name=sheet_names['sheet_12'])
    cur_timestamp = get_mod_time(files)
    df['modified_time'] = cur_timestamp

    df.rename(columns={
        "Warehouse": "warehouse",
        "Customer": "customer",
        "Item": "item",
        "Item Description": "item_desc",
        "Ordered Item": "ordered_item",
        "Assembly Cell": "assembly_cell",
        "Order #": "order_no",
        "PO Number": "po_no",
        "Line #": "line_no",
        "Line Creation Date": "line_creation_date",
        "Booked_Date": "booked_date",
        "Request Date": "request_date",
        "Scheduled Date": "scheduled_date",
        "Ordered Quantity": "ordered_qty",
        "Shipped Quantity": "shipped_qty",
        "Open Quantity": "open_qty",
        "Currency (Entered)": "currency_entered",
        "Selling Price (Entered)": "selling_price_entered",
        "Extended Value (Entered)": "extd_value_entered",
        "SELLING_PRICE_EXTENDED_USD": "selling_price_extd_usd",
        "Line Type": "line_type",
        "Work Order Number": "work_order_no",
        "Ship to Customer Addressee": "ship_to_cust_addr",
        "Line Status": "line_status",
        "Customer Service Rep": "customer_service_rep",
        "Order Hold": "order_hold",
        "Line Hold": "line_hold",
        "Ship Set": "ship_set",
        "Promise Date": "promise_date",
        "Requested Days": "requested_days",
        "FOB": "fob",
        "SHIP_TO_COUNTRY": "ship_to_country",
        "BILL_TO_COUNTRY": "bill_to_country",
        "SALESPERSON": "salesperson",
        "Pick_Release_Code": "pick_release_code",
        "Pick_Release_Date": "pick_release_date",
        "ONHAND_FG": "onhand_fg",
        "Freight_Carrier": "freight_carrier",
        "HEAD$AR_Invoice_Comments": "header_invoice_comments",
        "SHIPPING_INSTRUCTIONS": "shipping_instructions",
        "Legacy_Category_Description": "legacy_category_desc",
        "HEAD$Legacy_Order_Number": "head_legacy_order_no",
        "A$Customer": "customer_a",
        "A$Customer_Number": "customer_no_a",
        "CAT$ITEM_CATEGORY": "cat_item_category",
        "CUST$ATTRIBUTE_CATEGORY": "cust_attribute_category",
        "CUST$Supplier_ID": "cust_supplier_id",
        "Customer_Number": "customer_no",
        "Customer_Requested_Date": "customer_requested_date",
        "HEAD$China_Government_Invoice": "head_china_govt_invoice",
        "ITEM$Proprietary_Customer": "item_proprietary_customer",
        "LINES$Customer_Return_Reason": "lines_customer_return_reason",
        "Shipment_Priority_Code": "shipment_priority_code",
        "Customer_Item_Number": "customer_item_no",
        "HEAD$Customer_Source_Inspectio": "head_customer_source_inspectio",
        "Customer_Class_Code": "customer_class_code",
        "SHIP_TO_ADDRESS4": "ship_to_address4",
        "SHIP_TO_POSTAL_CODE": "ship_to_postal_code",
        "SHIP_TO_COUNTRY_CODE": "ship_to_country_code",
        "BILL_TO_POSTAL_CODE": "bill_to_postal_code",
        "BILL_TO_COUNTRY_CODE": "bill_to_country_code",
        "CUST$KOLLMORGEN_ABC_CODE": "cust_kollmorgen_abc_code",
        "CUST$THOMSON_ABC_CODE": "cust_thomson_abc_code",
        "LINES$Proto_Type": "lines_proto_type",
        "Order_Type": "order_type",
        "LINES$Promise_Date_Notes": "lines_promise_date_notes",
        "modified_time": "file_modified_time",

    }, inplace=True)

    df['file_row_no'] = np.arange(df.shape[0])

    # file_name = table_details['pso_bldc_dc_data'] + ".csv"
    # df.to_csv(file_name, index=False)
    save_to_db(table_details['pso_bldc_dc_data'], "append", cfg('db'), df)


def insert_pso_stepper(files):
    # files = os.path.join(folder_path, files)
    df = pd.read_excel(files, sheet_name=sheet_names['sheet_13'])
    cur_timestamp = get_mod_time(files)
    df['modified_time'] = cur_timestamp

    df.rename(columns={
        "Customer": "customer",
        "Item": "item",
        "Item Description": "item_desc",
        "Assembly Cell": "assembly_cell",
        "Order #": "order_no",
        "PO Number": "po_no",
        "Line_Number": "line_no",
        "Line Creation Date": "line_creation_date",
        "Booked_Date": "booked_date",
        "Request Date": "request_date",
        "Promise_Date": "promise_date",
        "Scheduled Date": "scheduled_date",
        "Open Quantity": "open_qty",
        "Currency (Entered)": "currency_entered",
        "Selling Price (Entered)": "selling_price_entered",
        "Selling_Price_Extended": "selling_price_extd",
        "FOB": "fob",
        "Pick_Release_Code": "pick_release_code",
        "Pick_Release_Date": "pick_release_date",
        "ONHAND_FG": "onhand_fg",
        "Work Order Number": "work_order_no",
        "Line Type": "line_type",
        "Line Status": "line_status",
        "modified_time": "file_modified_time",

    }, inplace=True)

    df['file_row_no'] = np.arange(df.shape[0])

    # file_name = table_details['pso_stepper_data'] + ".csv"
    # df.to_csv(file_name, index=False)
    save_to_db(table_details['pso_stepper_data'], "append", cfg('db'), df)


def insert_oracle_price_master(files):
    # files = os.path.join(folder_path, files)
    df = pd.read_excel(files, sheet_name=sheet_names['sheet_14'])
    cur_timestamp = get_mod_time(files)
    df['modified_time'] = cur_timestamp

    df.rename(columns={
        "Ship to Location": "ship_to_loc",
        "Vendor": "vendor",
        "Site": "site",
        "Status": "status",
        "Quatation#": "quatation_no",
        "Quote Line#": "quote_line_no",
        "Price Break#": "price_break_no",
        "Item": "item",
        "Description": "description",
        "UOM": "uom",
        "Currency": "currency",
        "Quote Line Price": "quote_line_price",
        "Break Qty": "break_qty",
        "Break Price": "break_price",
        "Quote Comment": "quote_comment",
        "Quote Status": "quote_status",
        "Quote Start Date": "quote_start_date",
        "Quote End Date": "quote_end_date",
        "Break Start Date": "break_start_date",
        "Break End Date": "break_end_date",
        "Approval Status": "approval_status",
        "Match": "match",
        "modified_time": "file_modified_time",

    }, inplace=True)

    df['file_row_no'] = np.arange(df.shape[0])

    # file_name = table_details['oracle_price_master_data'] + ".csv"
    # df.to_csv(file_name, index=False)
    save_to_db(table_details['oracle_price_master_data'], "append", cfg('db'), df)


def insert_consumption_query(files):
    # files = os.path.join(folder_path, files)
    df = pd.read_excel(files, sheet_name=sheet_names['sheet_15'])
    cur_timestamp = get_mod_time(files)
    df['modified_time'] = cur_timestamp

    df.rename(columns={
        "Product Line": "product_line",
        "Item": "item",
        "Description": "description",
        "Item Type": "item_type",
        "UOM": "uom",
        "GL Period": "gl_period",
        "Total Usage": "total_usage",
        "Item_Cost": "item_cost",
        "BU": "bu",
        "Organization_Code": "org_code",
        "modified_time": "file_modified_time",

    }, inplace=True)

    df['file_row_no'] = np.arange(df.shape[0])

#     file_name = table_details['consumption_query_data'] + ".csv"
    # df.to_csv(file_name, index=False)
    save_to_db(table_details['consumption_query_data'], "append", cfg('db'), df)


def insert_mrp_receipts(files):
    # files = os.path.join(folder_path, files)
    df = pd.read_excel(files, sheet_name=sheet_names['sheet_16'])
    cur_timestamp = get_mod_time(files)
    df['modified_time'] = cur_timestamp

    df.rename(columns={
        "Item": "item",
        "Product Line": "product_line",
        "Transaction Date": "transaction_date",
        "Qty Received": "qty_received",
        "Receipt_Number": "receipt_no",
        "Vendor_Name": "vendor_name",
        "Inventory_Organization": "inventory_org",
        "Item_Description": "item_desc",
        "modified_time": "file_modified_time",

    }, inplace=True)

    df['file_row_no'] = np.arange(df.shape[0])

    # file_name = table_details['mrp_receipts_data'] + ".csv"
    # df.to_csv(file_name, index=False)

    save_to_db(table_details['mrp_receipts_data'], "append", cfg('db'), df)


def insert_open_close_indent(files):
    # files = os.path.join(folder_path, files)
    df = pd.read_excel(files, sheet_name=sheet_names['sheet_17'])
    cur_timestamp = get_mod_time(files)
    df['modified_time'] = cur_timestamp

    df.rename(columns={
        "Requestor_Name": "requestor_name",
        "Req Creation Date": "req_creation_date",
        "Approval_Date": "approval_date",
        "Requisition Number": "requisition_no",
        "Req Line Number": "req_line_no",
        "Need by Date": "need_by_date",
        "Item": "item",
        "Item Description": "item_desc",
        "Item Revision": "item_revision",
        "UOM": "uom",
        "Suggested Buyer Name": "suggested_buyer_name",
        "Unit Price": "unit_price",
        "Req Quantity": "req_qty",
        "Req Total": "req_total",
        "Currency": "currency",
        "PO Number": "po_no",
        "PO Supplier": "po_supplier",
        "Po_Buyer_Name": "po_buyer_name",
        "Quantity Received": "qty_received",
        "PO Unit Price": "po_unit_price",
        "Supply Status": "supply_status",
        "Destination Organization": "destination_org",
        "Deliver To Location": "deliver_to_loc",
        "Authorization Status": "auth_status",
        "Charge Account": "charge_account",
        "PO Release Number": "po_release_no",
        "PO Line Number": "po_line_no",
        "PO Line Creation Date": "po_line_creation_date",
        "PO Line Type": "po_line_type",
        "Destination Subinventory": "dest_subinventory",
        "Org Item Class": "org_item_class",
        "Kanban?": "kanban",
        "Po_Approved_Code": "po_approved_code",
        "Po_Approved_Date": "po_approved_date",
        "Shipment_Approved_Code": "shipment_approved_code",
        "Shipment_Closed_Code": "shipment_closed_code",
        "Po_Currency": "po_currency",
        "Po_Buying_Price": "po_buying_price",
        "Po_Buying_Price_Base": "po_buying_price_base",
        "modified_time": "file_modified_time",

    }, inplace=True)

    df['file_row_no'] = np.arange(df.shape[0])

    # file_name = table_details['open_close_indent_data'] + ".csv"
    # df.to_csv(file_name, index=False)
    save_to_db(table_details['open_close_indent_data'], "append", cfg('db'), df)


def insert_pending_gir(files):
    # files = os.path.join(folder_path, files)
    df = pd.read_excel(files, sheet_name=sheet_names['sheet_18'])
    cur_timestamp = get_mod_time(files)
    df['modified_time'] = cur_timestamp

    df.rename(columns={
        "Item": "item",
        "Item Description": "item_desc",
        "Supplier / Customer": "supplier_customer",
        "Transaction Date": "transaction_date",
        "Receipt": "receipt",
        "Sum of Quantity": "sum_of_qty",
        "Extended Cost - INR": "extended_cost_inr",
        "PO Line Type": "po_line_type",
        "Inspection Comment": "inspection_comment",
        "Packing Slip": "packing_slip",
        "Location Reference": "loc_reference",
        "Receipt Creation Date": "rct_creation_date",
        "Buyer": "buyer",
        "Organization": "organization",
        "Product Line": "product_line",
        "Ordered UOM": "ordered_uom",
        "Aging Days": "aging_days",
        "modified_time": "file_modified_time",

    }, inplace=True)

    df['file_row_no'] = np.arange(df.shape[0])

    # file_name = table_details['pending_gir_data'] + ".csv"
    # df.to_csv(file_name, index=False)
    save_to_db(table_details['pending_gir_data'], "append", cfg('db'), df)


def insert_picklist_details(files):
    # files = os.path.join(folder_path, files)
    df = pd.read_excel(files, sheet_name=sheet_names['sheet_19'])
    cur_timestamp = get_mod_time(files)
    df['modified_time'] = cur_timestamp

    df.rename(columns={
        "Job #": "job",
        "Job Qty": "job_qty",
        "Qty Completed": "qty_completed",
        "Job Start Date": "job_start_date",
        "Job Sched. Complete Date": "job_sched_complete_dte",
        "Component": "component",
        "Component Description": "component_description",
        "Component_Unit_Of_Measure": "component_unit_of_measure",
        "On Hand Qty": "on_hand_qty",
        "Comp$location": "comp_location",
        "Component Qty Per Assembly": "component_qty_per_assembly",
        "Qty Reqd": "qty_reqd",
        "Component Qty Issued": "component_qty_issued",
        "Qty Open": "qty_open",
        "Assy Item": "assy_item",
        "Description": "description",
        "Status": "status",
        "Shortage": "shortage",
        "WIP Supply Type": "wip_supply_type",
        "Component Item Type": "component_item_type",
        "modified_time": "file_modified_time"
    }, inplace=True)

    df['file_row_no'] = np.arange(df.shape[0])

    # file_name = table_details['picklist_detail_report_data'] + ".csv"
    # df.to_csv(file_name, index=False)
    save_to_db(table_details['picklist_detail_report_data'], "append", cfg('db'), df)


def insert_supplier_master(files):
    # files = os.path.join(folder_path, files)
    df = pd.read_excel(files, sheet_name=sheet_names['sheet_20'])
    cur_timestamp = get_mod_time(files)
    df['modified_time'] = cur_timestamp

    df.rename(columns={
        "Supplier Number": "supplier_number",
        "Supplier": "supplier",
        "Site Code": "site_code",
        "Address Line 1": "address_line_1",
        "Address Line 2": "address_line_2",
        "Address Line 3": "address_line_3",
        "City": "city",
        "State": "state",
        "Zip": "zip",
        "Province": "province",
        "Country": "country",
        "Area Code": "area_code",
        "Phone": "phone",
        "Fax Area Code": "fax_area_code",
        "Fax": "fax",
        "Email": "email",
        "Pay Site Flag": "pay_site_flag",
        "Freight Terms": "freight_terms",
        "Invoice Currency": "invoice_currency",
        "Pay Group": "pay_group",
        "Payment Currency": "payment_currency",
        "Payment Method": "payment_method",
        "Duns": "duns",
        "Vat Code": "vat_code",
        "Vat Registration": "vat_registration",
        "Supplier End Date": "supplier_end_date",
        "Site End Date": "site_end_date",
        "Ariba Supplier": "ariba_supplier",
        "Supplier Notification Method": "supplier_notification_method",
        "Open PO Summary Report Flag": "open_po_summary_report_flag",
        "Payment Priority": "payment_priority",
        "Payment Term": "payment_term",
        "Pay Date Basis": "pay_date_basis",
        "Term Date Basis": "term_date_basis",
        "Vendor Bank Name": "vendor_bank_name",
        "Vendor Bank Acct#": "vendor_bank_acct",
        "Bank Branch Name": "bank_branch_name",
        "Acct. Liability Code": "acct._liability_code",
        "Prepay Liability Code": "prepay_liability_code",
        "Auto Tax Calc?": "auto_tax_calc",
        "Auto Tax Calc Override": "auto_tax_calc_override",
        "AWT?": "awt",
        "AWT Witholding Tax Group": "awt_witholding_tax_group",
        "Active?": "active",
        "Minority Group Lookup Code": "minority_group_lookup_code",
        "Alternate Supplier Name": "alternate_supplier_name",
        "Country of Origin Code": "country_of_origin_code",
        "Language": "language",
        "Intercompany Accrual Account": "intercompany_accrual_account",
        "PO Report Text Note": "po_report_text_note",
        "PO Transit Leadtime": "po_transit_leadtime",
        "Open PO Summary CSV Form": "open_po_summary_csv_form",
        "Supplier ID Number": "supplier_id_number",
        "Print Drop Ship Document": "print_drop_ship_document",
        "Supplier Creation Date": "supplier_creation_date",
        "Supplier Created By": "supplier_created_by",
        "Supplier Site Creation Date": "supplier_site_creation_date",
        "Supplier Site Created By": "supplier_site_created_by",
        "Operating Unit": "operating_unit",
        "Named Place": "named_place",
        "Supplier Classification": "supplier_classification",
        "Taxpayer ID": "taxpayer_id",
        "Ship Via": "ship_via",
        "Site Record Last Updated On": "site_record_last_updated_on",
        "Site Record Last Updated By": "site_record_last_updated_by",
        "Country Code": "country_code",
        "Invoice Match Option": "invoice_match_option",
        "Supplier Scorecard": "supplier_scorecard",
        "modified_time": "file_modified_time"

    }, inplace=True)

    df['file_row_no'] = np.arange(df.shape[0])

    # file_name = table_details['picklist_detail_report_data'] + ".csv"
    # df.to_csv(file_name, index=False)
    save_to_db(table_details['supplier_master_data'], "append", cfg('db'), df)


def insert_kanban_replenishment(files):
    # files = os.path.join(folder_path, files)
    df = pd.read_excel(files, sheet_name=sheet_names['sheet_21'])
    cur_timestamp = get_mod_time(files)
    df['modified_time'] = cur_timestamp

    df.rename(columns={
        "Kanban Card Number": "kanban_card_number",
        "Supply Status": "supply_status",
        "Kanban Card Status Ageing": "kanban_card_status_ageing",
        "KB Size": "kb_size",
        "KB Card Last Update Date": "kb_card_last_update_date",
        "KB Card Last Update By": "kb_card_last_update_by",
        "Req Creation Date": "req_creation_date",
        "Requisition Number": "requisition_number",
        "Item": "item",
        "PO Number": "po_number",
        "PO Approval Status": "po_approval_status",
        "Qty Received": "qty_received",
        "Qty Outstanding": "qty_outstanding",
        "Ship To Organization Name": "ship_to_organization_name",
        "modified_time": "file_modified_time"
    }, inplace=True)

    df['file_row_no'] = np.arange(df.shape[0])

    # file_name = table_details['picklist_detail_report_data'] + ".csv"
    # df.to_csv(file_name, index=False)
    save_to_db(table_details['kanban_in_replenishment_data'], "append", cfg('db'), df)


def insert_usage_since_fc(files):
    # files = os.path.join(folder_path, files)
    df = pd.read_excel(files, sheet_name=sheet_names['sheet_22'])
    cur_timestamp = get_mod_time(files)
    df['modified_time'] = cur_timestamp

    df.rename(columns={
        "Product Line": "product_line",
        "Item": "item",
        "Description": "description",
        "Total Usage": "total_usage",
        "Item Type": "item_type",
        "modified_time": "file_modified_time"

    }, inplace=True)

    df['file_row_no'] = np.arange(df.shape[0])

    # file_name = table_details['picklist_detail_report_data'] + ".csv"
    # df.to_csv(file_name, index=False)
    save_to_db(table_details['usage_since_fc_data'], "append", cfg('db'), df)


def insert_kb_card_summary(files):
    # files = os.path.join(folder_path, files)
    df = pd.read_excel(files, sheet_name=sheet_names['sheet_23'])
    cur_timestamp = get_mod_time(files)
    df['modified_time'] = cur_timestamp

    df.rename(columns={
        "Item": "item",
        "Description": "description",
        "# Cards": "cards",
        "QPC": "qpc",
        "Kanban LT": "kanban_lt",
        "Buyer": "buyer",
        "Subinventory": "subinventory",
        "Product Line": "product_line",
        "modified_time": "file_modified_time"
    }, inplace=True)

    df['file_row_no'] = np.arange(df.shape[0])

    # file_name = table_details['kb_card_summary_data'] + ".csv"
    # df.to_csv(file_name, index=False)
    save_to_db(table_details['kb_card_summary_data'], "append", cfg('db'), df)


def insert_pou_oh(files):
    # files = os.path.join(folder_path, files)
    df = pd.read_excel(files, sheet_name=sheet_names['sheet_24'])
    cur_timestamp = get_mod_time(files)
    df['modified_time'] = cur_timestamp

    df.rename(columns={
        "Org": "org",
        "Subinventory": "subinventory",
        "Locator": "locator",
        "Item": "item",
        "Item Revision ": "item_revision",
        "Item Description": "item_description",
        "Location Reference": "location_reference",
        "UOM": "uom",
        "Quantity": "quantity",
        "Supplier Category": "supplier_category",
        "Final Assembly Cell": "final_assembly_cell",
        "Item Cost": "item_cost",
        "Extended Cost": "extended_cost",
        "Org Item Class": "org_item_class",
        "modified_time": "file_modified_time"
    }, inplace=True)

    df['file_row_no'] = np.arange(df.shape[0])

    # file_name = table_details['kb_card_summary_data'] + ".csv"
    # df.to_csv(file_name, index=False)
    save_to_db(table_details['pou_oh_data'], "append", cfg('db'), df)


def insert_kb_card_status(files):
    # files = os.path.join(folder_path, files)
    df = pd.read_excel(files, sheet_name=sheet_names['sheet_25'])
    cur_timestamp = get_mod_time(files)
    df['modified_time'] = cur_timestamp

    df.rename(columns={
        "Organization Item Class": "organization_item_class",
        "Kanban Card Number": "kanban_card_number",
        "Card Type": "card_type",
        "Item": "item",
        "Description": "description",
        "Item Status": "item_status",
        "Supply Status": "supply_status",
        "Card Status": "card_status",
        "Supplier Category": "supplier_category",
        "Default Buyer Name": "default_buyer_name",
        "modified_time": "file_modified_time"
    }, inplace=True)

    df['file_row_no'] = np.arange(df.shape[0])

    # file_name = table_details['kb_card_summary_data'] + ".csv"
    # df.to_csv(file_name, index=False)
    save_to_db(table_details['kb_card_status_data'], "append", cfg('db'), df)


def insert_kb_status_refreshable(files):
    # files = os.path.join(folder_path, files)
    df = pd.read_excel(files, sheet_name=sheet_names['sheet_26'])
    cur_timestamp = get_mod_time(files)
    df['modified_time'] = cur_timestamp

    df.rename(columns={
        "Destination_Organization_Name": "destination_organization_name",
        "Supply Status": "supply_status",
        "Kanban Card Status Aging": "kanban_card_status_aging",
        "Suggested Buyer Name": "suggested_buyer_name",
        "Item": "item",
        "Item Description": "item_description",
        "PO Approval Status": "po_approval_status",
        "PO Supplier": "po_supplier",
        "modified_time": "file_modified_time"
    }, inplace=True)

    df['file_row_no'] = np.arange(df.shape[0])

    # file_name = table_details['kb_card_summary_data'] + ".csv"
    # df.to_csv(file_name, index=False)
    save_to_db(table_details['kb_status_refreshable_data'], "append", cfg('db'), df)


def insert_non_kb_po_refreshable(files):
    # files = os.path.join(folder_path, files)
    df = pd.read_excel(files, sheet_name=sheet_names['sheet_27'])
    cur_timestamp = get_mod_time(files)
    df['modified_time'] = cur_timestamp

    df.rename(columns={
        "Organization Name": "organization_name",
        "Buyer Name": "buyer_name",
        "Supplier Category": "supplier_category",
        "Compression Days": "compression_days",
        "Days to Place PO": "days_to_place_po",
        "Item": "item",
        "Item Description": "item_description",
        "Order Quantity": "order_quantity",
        "Amount": "amount",
        "Suggested Order Date": "suggested_order_date",
        "Suggested Dock Date": "suggested_dock_date",
        "Suggested_Due_Date": "suggested_due_date",
        "Planner Code": "planner_code",
        "Order Type": "order_type",
        "Item_Inventory_Status": "item_inventory_status",
        "Item_Type": "item_type",
        "PROCESSING_LEAD_TIME": "processing_lead_time",
        "POSTPROCESSING_LEAD_TIME": "postprocessing_lead_time",
        "FIXED_DAYS_SUPPLY": "fixed_days_supply",
        "MINIMUM_ORDER_QUANTITY": "minimum_order_quantity",
        "NETTABLE_INVENTORY_QUANTITY": "nettable_inventory_quantity",
        "KANBAN_SIZE": "kanban_size",
        "Kanban?": "kanban",
        "Business": "business",
        "modified_time": "file_modified_time"
    }, inplace=True)

    df['file_row_no'] = np.arange(df.shape[0])

    # file_name = table_details['kb_card_summary_data'] + ".csv"
    # df.to_csv(file_name, index=False)
    save_to_db(table_details['non_kb_po_refreshable_data'], "append", cfg('db'), df)


def insert_open_sales_order(files):
    # files = os.path.join(folder_path, files)
    df = pd.read_excel(files)
    cur_timestamp = get_mod_time(files)
    df['modified_time'] = cur_timestamp

    df.rename(columns={
        "Customer": "customer",
        "Item": "item",
        "Assembly Cell": "assembly_cell",
        "Order #": "order",
        "PO Number": "po_number",
        "Line_Number": "line_number",
        "Request Date": "request_date",
        "Scheduled Date": "scheduled_date",
        "Line_Ordered_Quantity": "line_ordered_quantity",
        "Open Quantity": "open_quantity",
        "Currency (Entered)": "currency_(entered)",
        "Selling Price (Entered)": "selling_price_(entered)",
        "Selling_Price_Extended": "selling_price_extended",
        "SELLING_PRICE_EXTENDED_USD": "selling_price_extended_usd",
        "ONHAND_FG": "onhand_fg",
        "Work Order Number": "work_order_number",
        "FOB": "fob",
        "Shipment_Priority_Code": "shipment_priority_code",
        "Warehouse": "warehouse",
        "Order_Type": "order_type",
        "modified_time": "file_modified_time"
    }, inplace=True)

    df['file_row_no'] = np.arange(df.shape[0])

    # file_name = table_details['kb_card_summary_data'] + ".csv"
    # df.to_csv(file_name, index=False)
    save_to_db(table_details['open_sales_order_data'], "append", cfg('db'), df)


def check_update(file_name):
    modificationtime = get_mod_time(file_name)
    if file_name == files_name['file_1']:
        last_sav_dte = read_from_db("select max(file_modified_time) from " + table_details['blanket_data'], cfg('db'))

    elif file_name == files_name['file_2']:
        last_sav_dte = read_from_db("select max(file_modified_time) from "+ table_details['item_master_data'],
                                    cfg('db'))

    elif file_name == files_name['file_3']:
        last_sav_dte = read_from_db("select max(file_modified_time) from " + table_details['inventory_onhand_data'],
                                    cfg('db'))

    elif file_name == files_name['file_4']:
        last_sav_dte = read_from_db(
            "select max(file_modified_time) from " + table_details['past_12_months_shipment_data'], cfg('db'))

    elif file_name == files_name['file_5']:
        last_sav_dte = read_from_db("select max(file_modified_time) from " + table_details['blanket_po_data'],
                                    cfg('db'))

    elif file_name == files_name['file_6']:
        last_sav_dte = read_from_db(
            "select max(file_modified_time) from " + table_details['standard_release_po_data'], cfg('db'))

    elif file_name == files_name['file_7']:
        last_sav_dte = read_from_db("select max(file_modified_time) from " + table_details['pending_iqa_data'],
                                    cfg('db'))

    elif file_name == files_name['file_8']:
        last_sav_dte = read_from_db("select max(file_modified_time) from " + table_details['pending_sc_data'],
                                    cfg('db'))

    elif file_name == files_name['file_9']:
        last_sav_dte = read_from_db("select max(file_modified_time) from " + table_details['monthly_gross_data'],
                                    cfg('db'))

    elif file_name == files_name['file_10']:
        last_sav_dte = read_from_db("select max(file_modified_time) from " + table_details['weekly_gross_data'],
                                    cfg('db'))

    # elif file_name == files_name['file_11']:
    #     last_sav_dte = read_from_db("select max(file_modified_time) from " + table_details['pso_bldc_dc_data'],
    #                                 cfg('db'))
    #
    # elif file_name == files_name['file_12']:
    #     last_sav_dte = read_from_db("select max(file_modified_time) from " + table_details['pso_stepper_data'],
    #                                 cfg('db'))

    elif file_name == files_name['file_13']:
        last_sav_dte = read_from_db(
            "select max(file_modified_time) from " + table_details['oracle_price_master_data'], cfg('db'))

    elif file_name == files_name['file_14']:
        last_sav_dte = read_from_db(
            "select max(file_modified_time) from " + table_details['consumption_query_data'], cfg('db'))

    elif file_name == files_name['file_15']:
        last_sav_dte = read_from_db("select max(file_modified_time) from " + table_details['mrp_receipts_data'],
                                    cfg('db'))

    elif file_name == files_name['file_16']:
        last_sav_dte = read_from_db(
            "select max(file_modified_time) from " + table_details['open_close_indent_data'], cfg('db'))

    elif file_name == files_name['file_17']:
        last_sav_dte = read_from_db("select max(file_modified_time) from " + table_details['pending_gir_data'],
                                    cfg('db'))

    elif file_name == files_name['file_18']:
        last_sav_dte = read_from_db("select max(file_modified_time) from " +
                                    table_details['picklist_detail_report_data'], cfg('db'))

    elif file_name == files_name['file_19']:
        last_sav_dte = read_from_db("select max(file_modified_time) from " + table_details['supplier_master_data'],
                                    cfg('db'))

    elif file_name == files_name['file_20']:
        last_sav_dte = read_from_db("select max(file_modified_time) from " +
                                    table_details['kanban_in_replenishment_data'], cfg('db'))

    elif file_name == files_name['file_21']:
        last_sav_dte = read_from_db("select max(file_modified_time) from " + table_details['usage_since_fc_data'],
                                    cfg('db'))

    elif file_name == files_name['file_22']:
        last_sav_dte = read_from_db("select max(file_modified_time) from " + table_details['kb_card_summary_data'],
                                    cfg('db'))

    elif file_name == files_name['file_23']:
        last_sav_dte = read_from_db("select max(file_modified_time) from " + table_details['pou_oh_data'],
                                    cfg('db'))

    elif file_name == files_name['file_24']:
        last_sav_dte = read_from_db("select max(file_modified_time) from " + table_details['kb_card_status_data'],
                                    cfg('db'))

    elif file_name == files_name['file_25']:
        last_sav_dte = read_from_db("select max(file_modified_time) from " + table_details['kb_status_refreshable_data']
                                    , cfg('db'))

    elif file_name == files_name['file_26']:
        last_sav_dte = read_from_db("select max(file_modified_time) from " + table_details['non_kb_po_refreshable_data']
                                    , cfg('db'))

    elif file_name == files_name['file_27']:
        last_sav_dte = read_from_db("select max(file_modified_time) from " + table_details['open_sales_order_data']
                                    , cfg('db'))

    if modificationtime != last_sav_dte['max'][0]:
        return True
    else:
        return False


def job():
    folder_path = f_path['folder_path']
    files = os.listdir(str(folder_path))
    for i in files:
        if not i.startswith('~'):
            if i == files_name['file_1']:
                fil_path = folder_path + "/" + i
                if check_update(i):
                    insert_blanket_sales(fil_path)

            elif i == files_name['file_2']:
                fil_path = folder_path + "/" + i
                if check_update(i):
                    insert_item_master(fil_path)
            
            elif i == files_name['file_3']:
                fil_path = folder_path + "/" + i
                if check_update(i):
                    insert_inventory_onhand(fil_path)
            
            elif i == files_name['file_4']:
                fil_path = folder_path + "/" + i
                if check_update(i):
                    insert_past_12_shipment(fil_path)
            
            elif i == files_name['file_5']:
                fil_path = folder_path + "/" + i
                if check_update(i):
                    insert_blanket_po(fil_path)
            
            elif i == files_name['file_6']:
                fil_path = folder_path + "/" + i
                if check_update(i):
                    insert_standard_release_po(fil_path)
            
            elif i == files_name['file_7']:
                fil_path = folder_path + "/" + i
                if check_update(i):
                    insert_pending_iqa(fil_path)
            
            elif i == files_name['file_8']:
                fil_path = folder_path + "/" + i
                if check_update(i):
                    insert_pending_sc(fil_path)

            elif i == files_name['file_9']:
                fil_path = folder_path + "/" + i
                if check_update(i):
                    insert_monthly_gross(fil_path)

            elif i == files_name['file_10']:
                fil_path = folder_path + "/" + i
                if check_update(i):
                    insert_weekly_gross(fil_path)

            # elif i == files_name['file_11']:
            #     fil_path = folder_path + "/" + i
            #     if check_update(i):
            #         insert_pso_bldc(fil_path)
            #

            # elif i == files_name['file_12']:
            #     fil_path = folder_path + "/" + i
            #     if check_update(i):
            #         insert_pso_stepper(fil_path)

            elif i == files_name['file_13']:
                fil_path = folder_path + "/" + i
                if check_update(i):
                    insert_oracle_price_master(fil_path)
            
            elif i == files_name['file_14']:
                fil_path = folder_path + "/" + i
                if check_update(i):
                    insert_consumption_query(fil_path)
            
            elif i == files_name['file_15']:
                fil_path = folder_path + "/" + i
                if check_update(i):
                    insert_mrp_receipts(fil_path)
            
            elif i == files_name['file_16']:
                fil_path = folder_path + "/" + i
                if check_update(i):
                    insert_open_close_indent(fil_path)
            
            elif i == files_name['file_17']:
                fil_path = folder_path + "/" + i
                if check_update(i):
                    insert_pending_gir(fil_path)
            
            elif i == files_name['file_18']:
                fil_path = folder_path + "/" + i
                if check_update(i):
                    insert_picklist_details(fil_path)
            
            elif i == files_name['file_19']:
                fil_path = folder_path + "/" + i
                if check_update(i):
                    insert_supplier_master(fil_path)
            
            elif i == files_name['file_20']:
                fil_path = folder_path + "/" + i
                if check_update(i):
                    insert_kanban_replenishment(fil_path)
            
            elif i == files_name['file_21']:
                fil_path = folder_path + "/" + i
                if check_update(i):
                    insert_usage_since_fc(fil_path)
            
            elif i == files_name['file_22']:
                fil_path = folder_path + "/" + i
                if check_update(i):
                    insert_kb_card_summary(fil_path)
            
            elif i == files_name['file_23']:
                fil_path = folder_path + "/" + i
                if check_update(i):
                    insert_pou_oh(fil_path)
            
            elif i == files_name['file_24']:
                fil_path = folder_path + "/" + i
                if check_update(i):
                    insert_kb_card_status(fil_path)
            
            elif i == files_name['file_25']:
                fil_path = folder_path + "/" + i
                if check_update(i):
                    insert_kb_status_refreshable(fil_path)
            
            elif i == files_name['file_26']:
                fil_path = folder_path + "/" + i
                if check_update(i):
                    insert_non_kb_po_refreshable(fil_path)
            
            elif i == files_name['file_27']:
                fil_path = folder_path + "/" + i
                if check_update(i):
                    insert_open_sales_order(fil_path)

            else:
                print(i, ' File processed')
        else:
            print(i, ' File not processed')

# schedule.every(5).seconds.do(job)
#
# while True:
#     schedule.run_pending()
#     time.sleep(1)
