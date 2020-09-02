import mws
import pathlib
from ruamel.yaml import YAML
import sys
import re
import datetime
import time
from contextlib import suppress
import pandas as pd
from io import StringIO
import os


def xml_element_value(xml_response, element_name):
    """Returns text value for given xml element."""
    element_value = re.findall(
        f"<{element_name}>(.+?)</{element_name}>", xml_response)
    return element_value


def csv_filepath(integrations):
    """Retuns filepath for csv file report"""
    filepath = ""
    integration = ""

    if integrations["file_id"].startswith("cc"):
        integration = "cc"
    else:
        integration = "mpv"

    if integrations["enumeration_value"] == "_GET_FBA_MYI_ALL_INVENTORY_DATA_":
        filepath = f"/Users/wylecordero/OneDrive - Daasity/scripts/mws/files/{integration}/GET_FBA_MYI_ALL_INVENTORY_DATA/{integrations['file_id']}-{integrations['marketplace_id']}-fba-inventory-data.csv"
    elif integrations["enumeration_value"] == "_GET_MERCHANT_LISTINGS_DATA_":
        filepath = f"/Users/wylecordero/OneDrive - Daasity/scripts/mws/files/{integration}/GET_MERCHANT_LISTINGS_DATA/{integrations['file_id']}-{integrations['marketplace_id']}-merchant-listings-data.csv"
    elif integrations["enumeration_value"] == "_GET_FLAT_FILE_ORDERS_DATA_":
        filepath = f"/Users/wylecordero/OneDrive - Daasity/scripts/mws/files/{integration}/GET_FLAT_FILE_ORDERS_DATA/{integrations['file_id']}-{integrations['marketplace_id']}-orders-data.csv"

    return filepath


def delete_files(directory, pattern):
    """Delete files in directory based on the pattern"""
    for parent, dirnames, filenames in os.walk(directory):
        for fn in filenames:
            if fn.lower().endswith(pattern):
                os.remove(os.path.join(parent, fn))


def start_end_dates(action, days):
    """Returns start & end date based on the number of days and action endtered ('-', '+')"""
    startdate = ""
    enddate = ""

    if action == "+":
        startdate = datetime.date.today()
        enddate = startdate + datetime.timedelta(days=days)
    else:
        startdate = datetime.date.today() - datetime.timedelta(days=days)
        enddate = datetime.date.today()

    return str(startdate), str(enddate)


def files_directory(path, pattern):
    """Returns a list of files in a directory based on the pattern"""
    directory = pathlib.Path(path)
    return directory.glob(pattern)


def yaml_file_loader(filename):
    """Loads yaml file"""
    yaml_filename = filename
    yaml = YAML()
    fp = pathlib.Path(yaml_filename)
    data = yaml.load(fp)
    return data


def yaml_file_writter(filename, request_data):
    """Writes to yaml file"""
    yaml_filename = filename
    yaml = YAML()
    fp = pathlib.Path(yaml_filename)
    yaml.dump(request_data, fp)


def mws_integrations(config_file):
    """Returns a list of the integrations listed in the yaml file."""
    integrations = config_file.get("integrations")

    for i in integrations:
        yield dict(i)


def mws_connection(config_file, integrations):
    """Returns connection aws object."""
    # Daasity, constant values.
    access_key = config_file["daasity_mws"]["access_key"]
    secret_key = config_file["daasity_mws"]["secret_key"]

    # Merchant
    merchant_id = integrations["merchant_id"]
    access_token = integrations["access_token"]
    conn = mws.Reports(access_key=access_key, secret_key=secret_key,
                       account_id=merchant_id, auth_token=access_token)
    return conn


def mws_request_report(conn, integrations):
    """Request reports for the report type per merchant/marketplace."""
    response = ""

    if integrations["enumeration_value"] == "_GET_FLAT_FILE_ORDERS_DATA_":
        dates = start_end_dates("-", 60)
        response = conn.request_report(
            report_type=integrations["enumeration_value"], start_date=dates[0], end_date=dates[1], marketplaceids=integrations["marketplace_id"]).original
    else:
        response = conn.request_report(
            report_type=integrations["enumeration_value"], marketplaceids=integrations["marketplace_id"]).original

    request_id = xml_element_value(response, "ReportRequestId")
    status = xml_element_value(response, "ReportProcessingStatus")
    request_date = xml_element_value(response, "SubmittedDate")
    integrations["report_request_id"] = request_id[0]
    integrations["status"] = status[0]
    integrations["request_date"] = request_date[0]
    return integrations


def mws_report_id(conn, integrations):
    """Returns report id"""
    response = conn.get_report_list(
        requestids=integrations["report_request_id"], types=integrations["enumeration_value"]).original
    report_id = xml_element_value(response, "ReportId")
    return report_id[0]


def mws_report_data(conn, report_id):
    """Returns report data output."""
    response = conn.get_report(report_id=report_id).original
    data = StringIO(str(response, "mac_roman"))
    return data


def mws_create_csv(integrations, report_data):
    """Creates a csv file based on the data that is passed"""
    data = pd.read_csv(report_data, sep="\t", dtype="str", header=0)
    data["marketplace_id"] = integrations["marketplace_id"]
    data["seller_id"] = integrations["merchant_id"]
    filepath = csv_filepath(integrations)
    data.to_csv(filepath, sep="\t", encoding="utf-8",
                index=False, line_terminator="\n")


def mws_run_requests():
    """Runs all of the logic to request reports"""
    config_file = yaml_file_loader("mws_config.yaml")
    integrations = mws_integrations(config_file)
    lst = []

    for i in integrations:
        with suppress(Exception):
            conn = mws_connection(config_file, i)

        with suppress(Exception):
            request_report = mws_request_report(conn, i)
            lst.append(request_report)

    yaml_file_writter("mws_requests.yaml", lst)


def mws_create_reports():
    """Runs all of the logic to create files."""
    config_file = yaml_file_loader("mws_config.yaml")
    request_file = yaml_file_loader("mws_requests.yaml")
    intergrations = mws_integrations(request_file)

    for i in intergrations:
        conn = mws_connection(config_file, i)
        report_id = mws_report_id(conn, i)
        report_data = mws_report_data(conn, report_id)
        mws_create_csv(i, report_data)


def mws_combine_reports(integration, folder_name, filename):
    """Combine csv files into one file"""
    files = files_directory(f"./files/{integration}/{folder_name}", "*.csv")
    lst = []
    filepath = f"/Users/wylecordero/OneDrive - Daasity/scripts/mws/files/{integration}/{folder_name}/{filename}"

    for f in files:
        df = pd.read_csv(f, sep="\t", dtype="str", header=0)
        lst.append(df)

    with suppress(Exception):
        df = pd.concat(lst)
        df2 = df.sort_index(axis=1)  # Sort headers alphabetically.
        df2.to_csv(filepath, sep="\t", encoding="utf-8",
                   index=False, line_terminator="\n")


def main():
    if __name__ == "__main__":
        request_type = sys.argv[1]
        if request_type == "request":
            mws_run_requests()
        elif request_type == "create":
            mws_create_reports()
        elif request_type == "combine":
            # GET_FBA_MYI_ALL_INVENTORY_DATA report
            mws_combine_reports("cc", "GET_FBA_MYI_ALL_INVENTORY_DATA",
                                "asc_report_fba_inventory.csv")
            mws_combine_reports("mpv", "GET_FBA_MYI_ALL_INVENTORY_DATA",
                                "asc_report_fba_inventory.csv")

            # GET_MERCHANT_LISTINGS_DATA report
            mws_combine_reports("cc", "GET_MERCHANT_LISTINGS_DATA",
                                "asc_report_merchant_lisntings.csv")
            mws_combine_reports("mpv", "GET_MERCHANT_LISTINGS_DATA",
                                "asc_report_merchant_lisntings.csv")

            # GET_FLAT_FILE_ORDERS_DATA
            mws_combine_reports("cc", "GET_FLAT_FILE_ORDERS_DATA",
                                "asc_report_all_orders.csv")
            mws_combine_reports("mpv", "GET_FLAT_FILE_ORDERS_DATA",
                                "asc_report_all_orders.csv")
        elif request_type == "delete":
            delete_files("./files", ".csv")


main()
