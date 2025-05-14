# Import necessary libraries
import json
import time
import pandas as pd
import requests
import uuid
import logging
import csv
import os
from datetime import datetime

# Configure logging with timestamps and file output
# Logs will be saved in the 'logs' directory with a timestamped filename
log_directory = "logs"
os.makedirs(log_directory, exist_ok=True)
log_filename = f"{log_directory}/invoice_processing_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)

# Create data directory if it doesn't exist
os.makedirs("data", exist_ok=True)

# Read invoice list from CSV file
# invoice_list = pd.read_csv('voltera_pending_invoice_ids.csv')
# for this streamlit app we will take the data from the user input, enter saparated and save it in a list invoice_list.

# For testing purposes, limit the processing to the first invoice
# invoice_list = invoice_list.iloc[0:1]  # Adjust this range as needed
logging.info(f"Processing {len(invoice_list)} invoices")

# Function to create a template for the API payload
def create_payload_template():
    return {
        "id": "<random objectid>",
        "action": "WRITE",
        "type": "INVOICES",
        "payload": {
            "purchase_invoice": "{//response from above get invoice api}",
            "purchase_orders": {},
            "purchase_receipts": {}
        },
        "context": {
            "orgId": f"{get_orgid()}",
            "number": "<invoice_number>",
            "vendor_key": "1107",
            "user_id": "87f50a59-b24c-4550-bd67-7c5eaec9755d",
            "purchase_invoice_id": "<purchase_invoice_id>"
        }
    }


def get_token():
    pass
    # get the fucntion from user input and save it in a file, when the user add a new token save that too, so that user can choose the token and never have to add the token again and again
    return 'eyJraWQiOiJUYWxPdk5xMXB1QmRXVHo3Um5rQzRseUhObG1CYzZHWUVaSks5c0RyZEg4PSIsImFsZyI6IlJTMjU2In0.eyJzdWIiOiI5NGM4ODRiOC0yMDcxLTcwOTAtYzM2Zi0xMzQ0ODgwZmIwYjUiLCJpc3MiOiJodHRwczpcL1wvY29nbml0by1pZHAudXMtZWFzdC0xLmFtYXpvbmF3cy5jb21cL3VzLWVhc3QtMV9LY1N3Y2t6YlkiLCJjbGllbnRfaWQiOiJoYzdzZmFpdDZrajV2aXV2NTNncHM0a2MxIiwib3JpZ2luX2p0aSI6IjE5OGNhZGYyLWY0YzktNDUxNS04NmJlLTM1YzRmMWViMzZhNCIsImV2ZW50X2lkIjoiY2E3ZGY4Y2YtNWY2Zi00NmE3LTk2NzktMjY1YzAxOWM5MDk4IiwidG9rZW5fdXNlIjoiYWNjZXNzIiwic2NvcGUiOiJhd3MuY29nbml0by5zaWduaW4udXNlci5hZG1pbiIsImF1dGhfdGltZSI6MTcyMjQwNDY3MiwiZXhwIjoxNzIyNDA4MjcyLCJpYXQiOjE3MjI0MDQ2NzIsImp0aSI6IjQwZDVhNGFmLTc0MmEtNGM0NC04MzFiLTNmNWJhZDVhZDllMCIsInVzZXJuYW1lIjoiOTRjODg0YjgtMjA3MS03MDkwLWMzNmYtMTM0NDg4MGZiMGI1In0.AoNLrvGXaSwuELF9EdxGpzKiHZWUpIja-5aOOWbWp_3PYKkUkjAOF0mhsM7gphqJl-IPocXUVXiPz5Dzm5dfdDHdq1J09xYZ88YGabKPwzybjM7r0RJ3BXICtPBaTsqH8MdFH4qTz8R-x65_QpXRuj9Urhs676mmYljhJfpiiwZR5EaPWfaNHxy8Yz8fvMr1YFBZHktWpCXokFEjM6vlKmFFl_VUElkkorHW0Nz23yM45oE3_4D5csL8PX-spl2fuM_3L0LQ91z22vNMUHWynaXViv0oPlFN8Ye5RfdvD4Al-8GViqLOtYCwTkcOt-Ga9NLvOBNFAExJU_C-r49scw',

def get_orgid():
    pass
    # get the fucntion from user input and save it in a file, when the user add a new token save that too, so that user can choose the token and never have to add the token again and again
    return 'da295928-a147-4f4d-abfc-9de385985d23'

# Function to send data to the Hyperbots API
def push_to_hbp_cmds(payload, invoice_id):
    """Send data to Hyperbots API and track the request"""
    url = f"https://api.hyperbots.com/data-streams/test/HYPRBOTS-COMMANDS-{get_orgid()}"

    headers = {
        'token': get_token(),
        'Content-Type': 'application/json',
        'Cookie': 'HYPRBOTS_TOKEN=eyJraWQiOiJrZXktaWQiLCJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJkOWIxYzc0Yi04ZTQzLTQwZGYtYTcwZi1hZjY5MTllYzIwNGUiLCJicm93c2VyX2lkIjoid3djbjJ0Iiwib3JnX2lkIjoiYTFlYTJjOTQtZjA0OC00NjMyLThlMTUtMmVjNGZhOGMzNTdiIiwiZXhwIjoxNzQ2NDMwMzY5LCJpYXQiOjE3NDY0MjY3Njl9.RAH5RVMFQjJRmUKpkjlRxa5CnsJMVdqmhMlD3OiyeX7mowx7F4JsNH_Xf4ne6Be_xLmjAyC_gBBrkoQ-DpPHt8-S5kp3A8Rtiyk74CGnZYZ05SmqxtQzf4tqvXMDKVPPr0pMwWgl3Zz4l2taZRqo8ONrcSLeoCljVEduwMiK15A; HYPRBOTS_TOKEN=eyJraWQiOiJrZXktaWQiLCJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJlNTFjNjIxOC1hNThkLTRkMDMtYWU4YS01ZDI3YTE3YjMyMDUiLCJicm93c2VyX2lkIjoid3djbjJ0Iiwib3JnX2lkIjoiZGRiNWJhYTQtMzEwYy00YWFlLWEwMWQtZTczZGE4ZDhjMWI5IiwiZXhwIjoxNzQ2NjA1NTAwLCJpYXQiOjE3NDY2MDE5MDB9.j0hkyugXAyG3ymgGdmN_U2MHxGbed-DBSbqK75nPyyQgevULEamz3NUZA0bTYVkfSUssQfhSzvd4umi9nx3k9jgA4GjkaznEJb-vSou1yG-X63B3wnzGZp9umBF4LeLW0mV64okgTnn_ngQgTAUa7nlMOKOT4k3MYgC-6J1FIpk; HYPRBOTS_TOKEN=eyJraWQiOiJrZXktaWQiLCJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJiZTQ1OTRmZS1iZDE1LTRlMWMtYjY5Zi1jNDg1YWUxNzBlNDMiLCJicm93c2VyX2lkIjoibXA0eGdyIiwib3JnX2lkIjoiZGEyOTU5MjgtYTE0Ny00ZjRkLWFiZmMtOWRlMzg1OTg1ZDIzIiwiZXhwIjoxNzQ2NjE2NTM5LCJpYXQiOjE3NDY2MTI5Mzl9.EORbRZJp3pIXV6EbzRe40rfB4Rci8X-CxzHfk9VQCc8qmvrz6S3_NxIZkgVjUBOxwmG9ev7nMvZ6ZftB8R9Z1fy_ORVu9qwwiUNMKh5Ke9NJVWnkk6JcHbuBZs7qpCmMdGKcUbO6Y7hnMEYMZaACRsKh2Dlwe1QeOHDX_158qXo'
    }

    logging.info(f"Pushing invoice ID {invoice_id} to HBP commands API")
    try:
        # Send POST request to the API
        response = requests.request("POST", url, headers=headers, data=payload)
        logging.info(f"API response for invoice {invoice_id}: Status {response.status_code}")

        # Save the API response for debugging purposes
        with open(f"data/{invoice_id}_api_response.json", "w") as json_file:
            json_file.write(response.text)

        return response
    except Exception as e:
        logging.error(f"API call failed for invoice {invoice_id}: {e}")
        return None

def get_po_details(invoice_number, po, ):
    """Retrieve purchase order details for a given invoice"""
    logging.info(f"Retrieving PO details for invoice {invoice_number}, PO: {po}")
    url = f'https://api.hyperbots.com/data_hub/{get_orgid()}/purchase_order/v1/invoice-number/{invoice_number}'
    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'origin': 'https://p2p.hyperbots.com',
        'priority': 'u=1, i',
        'referer': 'https://p2p.hyperbots.com/',
        'sec-ch-ua': '"Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
        'x-browser-id': 'mp4xgr',
        'x-session-id': '63800cb3-5bc6-40cb-8965-12ebaff44f2d',
        'Cookie': 'AMP_MKTG_ffa2a025be=JTdCJTdE; HYPRBOTS_TOKEN=eyJraWQiOiJrZXktaWQiLCJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJiZTQ1OTRmZS1iZDE1LTRlMWMtYjY5Zi1jNDg1YWUxNzBlNDMiLCJicm93c2VyX2lkIjoibXA0eGdyIiwib3JnX2lkIjoiZGEyOTU5MjgtYTE0Ny00ZjRkLWFiZmMtOWRlMzg1OTg1ZDIzIiwiZXhwIjoxNzQ2NjE2NTM5LCJpYXQiOjE3NDY2MTI5Mzl9.EORbRZJp3pIXV6EbzRe40rfB4Rci8X-CxzHfk9VQCc8qmvrz6S3_NxIZkgVjUBOxwmG9ev7nMvZ6ZftB8R9Z1fy_ORVu9qwwiUNMKh5Ke9NJVWnkk6JcHbuBZs7qpCmMdGKcUbO6Y7hnMEYMZaACRsKh2Dlwe1QeOHDX_158qXo; AMP_ffa2a025be=JTdCJTIyZGV2aWNlSWQlMjIlM0ElMjIxNzE3ZWNiMi0wMWM2LTQxZDUtODU4Zi1mNDBjNGJhNWZkMWMlMjIlMkMlMjJzZXNzaW9uSWQlMjIlM0ExNzQ2NjEzNTcwNDQ2JTJDJTIyb3B0T3V0JTIyJTNBZmFsc2UlMkMlMjJsYXN0RXZlbnRUaW1lJTIyJTNBMTc0NjYxNTA2MzcwOSUyQyUyMmxhc3RFdmVudElkJTIyJTNBNDAyOCUyQyUyMnBhZ2VDb3VudGVyJTIyJTNBMyU3RA==; HYPRBOTS_TOKEN=eyJraWQiOiJrZXktaWQiLCJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJiZTQ1OTRmZS1iZDE1LTRlMWMtYjY5Zi1jNDg1YWUxNzBlNDMiLCJicm93c2VyX2lkIjoibXA0eGdyIiwib3JnX2lkIjoiZGEyOTU5MjgtYTE0Ny00ZjRkLWFiZmMtOWRlMzg1OTg1ZDIzIiwiZXhwIjoxNzQ2NjE2NTM5LCJpYXQiOjE3NDY2MTI5Mzl9.EORbRZJp3pIXV6EbzRe40rfB4Rci8X-CxzHfk9VQCc8qmvrz6S3_NxIZkgVjUBOxwmG9ev7nMvZ6ZftB8R9Z1fy_ORVu9qwwiUNMKh5Ke9NJVWnkk6JcHbuBZs7qpCmMdGKcUbO6Y7hnMEYMZaACRsKh2Dlwe1QeOHDX_158qXo'
    }
    
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            po_data = response.json()
            # Save PO data for debugging
            with open(f"data/{invoice_number}_{po}_podata.json", "w") as json_file:
                json.dump(po_data, json_file, indent=4)
            logging.debug(f"Successfully retrieved PO data for invoice {invoice_number}, PO: {po}")
            return po_data
        else:
            logging.error(f"Failed to retrieve PO details for invoice {invoice_number}, PO: {po}. Status code: {response.status_code}")
            return None
    except Exception as e:
        logging.error(f"Exception retrieving PO details for invoice {invoice_number}, PO: {po}: {e}")
        return None

# Create or update the tracking files
tracking_dir = "tracking"
os.makedirs(tracking_dir, exist_ok=True)

# Initialize tracking files
checklist_file = f'{tracking_dir}/voltera_pending_invoice_checklist.csv'
id_mapping_file = f'{tracking_dir}/inv-del-ids.csv'

# Initialize the checklist file with headers if it doesn't exist
if not os.path.exists(checklist_file):
    with open(checklist_file, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Invoice ID', 'Status', 'Error Message', 'Timestamp'])

# Initialize the ID mapping file with headers if it doesn't exist
if not os.path.exists(id_mapping_file):
    with open(id_mapping_file, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['UUID', 'Invoice Number', 'Invoice ID', 'Timestamp'])

# Function to update the checklist
def update_checklist(invoice_id, status, error_message=None):
    with open(checklist_file, 'a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([invoice_id, status, error_message, datetime.now().strftime('%Y-%m-%d %H:%M:%S')])

# Function to record ID mappings
def record_id_mapping(uuid_value, invoice_number, invoice_id):
    with open(id_mapping_file, 'a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([uuid_value, invoice_number, invoice_id, datetime.now().strftime('%Y-%m-%d %H:%M:%S')])

# Main processing function
def process_invoice(invoice_id):
    """Process a single invoice ID through the entire workflow"""
    logging.info(f"===== Starting processing for invoice ID: {invoice_id} =====")
    
    # Create new payload for this invoice
    current_payload = create_payload_template()
    
    # Step 1: Fetch invoice data
    url = f'https://api.hyperbots.com/data_hub/{get_orgid()}/purchase_invoice/v1/connector/{invoice_id}'
    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'origin': 'https://p2p.hyperbots.com',
        'priority': 'u=1, i',
        'referer': 'https://p2p.hyperbots.com/',
        'sec-ch-ua': '"Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
        'x-browser-id': 'mp4xgr',
        'x-session-id': 'cea836e0-a076-4c4e-901e-7d337747bac2',
        'Cookie': 'AMP_MKTG_ffa2a025be=JTdCJTdE; HYPRBOTS_TOKEN=eyJraWQiOiJrZXktaWQiLCJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJlMTZlN2VhZS01NzM5LTQ5MDgtYWNiMS1kYzc2YWJjMzMzMDkiLCJicm93c2VyX2lkIjoibXA0eGdyIiwib3JnX2lkIjoiZjQ2YjEzMTktNzhjMi00ZWFiLWI1NzYtZDc3YTg0ZGY4MTE2IiwiZXhwIjoxNzQ2NjA5MzE3LCJpYXQiOjE3NDY2MDU3MTd9.lL_zhAgZKkKuhblIPa7RIKixEqTorz5hEwyF9bLP8slDKnLQuMtTq6aTVBRIKCeXfTqgKXmw8qqhsXGiKFI4g3NO6qa81BfGl2u4k4o_8FE4RNu9DDf6EN0ASCzlJjWjSoizRxRT9oScd1tjMNyhq1XAEBMnuvnADvlVhVHR-1I; AMP_ffa2a025be=JTdCJTIyZGV2aWNlSWQlMjIlM0ElMjIxNzE3ZWNiMi0wMWM2LTQxZDUtODU4Zi1mNDBjNGJhNWZkMWMlMjIlMkMlMjJzZXNzaW9uSWQlMjIlM0ExNzQ2NjExODU0OTczJTJDJTIyb3B0T3V0JTIyJTNBZmFsc2UlMkMlMjJsYXN0RXZlbnRUaW1lJTIyJTNBMTc0NjYxMjUwMDAwNiUyQyUyMmxhc3RFdmVudElkJTIyJTNBNDAzMSUyQyUyMnBhZ2VDb3VudGVyJTIyJTNBMyU3RA==; HYPRBOTS_TOKEN=eyJraWQiOiJrZXktaWQiLCJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJlMTZlN2VhZS01NzM5LTQ5MDgtYWNiMS1kYzc2YWJjMzMzMDkiLCJicm93c2VyX2lkIjoibXA0eGdyIiwib3JnX2lkIjoiZjQ2YjEzMTktNzhjMi00ZWFiLWI1NzYtZDc3YTg0ZGY4MTE2IiwiZXhwIjoxNzQ2NjA5MzE3LCJpYXQiOjE3NDY2MDU3MTd9.lL_zhAgZKkKuhblIPa7RIKixEqTorz5hEwyF9bLP8slDKnLQuMtTq6aTVBRIKCeXfTqgKXmw8qqhsXGiKFI4g3NO6qa81BfGl2u4k4o_8FE4RNu9DDf6EN0ASCzlJjWjSoizRxRT9oScd1tjMNyhq1XAEBMnuvnADvlVhVHR-1I'
    }

    try:
        logging.debug(f"Fetching invoice data for ID: {invoice_id}")
        response = requests.get(url, headers=headers)
        
        if response.status_code != 200:
            logging.error(f"Failed to retrieve invoice {invoice_id}. Status code: {response.status_code}")
            update_checklist(invoice_id, 'Failed', f"HTTP {response.status_code} - Could not fetch invoice data")
            return False
        
        # Generate a UUID for this transaction
        new_id = str(uuid.uuid4())
        invoice_data = response.json()
        
        # Save invoice data for debugging
        with open(f"data/{invoice_id}_invoice_response.json", "w") as json_file:
            json.dump(invoice_data, json_file, indent=4)
            
        # Record the mapping between UUID and invoice ID
        record_id_mapping(new_id, invoice_data["number"], invoice_id)
        
        # Update payload with invoice information
        current_payload["id"] = new_id
        current_payload["context"]["number"] = "TEST" + invoice_data["number"]
        current_payload["payload"]["purchase_invoice"] = invoice_data
        current_payload["context"]["purchase_invoice_id"] = invoice_id
        
        # Step 2: Process purchase orders
        po_payload = {}
        item_list = invoice_data.get("line_items", [])
        
        for j in range(len(item_list)):
            po = item_list[j].get("line_item/purchase_order_number", "")
            if po and len(po) > 0:
                logging.debug(f"Processing PO: {po} for invoice {invoice_id}")
                po_data = get_po_details(invoice_id, po)
                if po_data and len(po_data) > 0:
                    po_payload[po] = po_data[0]
                else:
                    logging.warning(f"No PO data returned for PO: {po}, invoice: {invoice_id}")
            else:
                logging.debug(f"No PO number for line item {j} in invoice {invoice_id}")
                
        current_payload["payload"]["purchase_orders"] = po_payload
        
        # Modify invoice number for testing
        current_payload["payload"]["purchase_invoice"]["number"] = "TEST" + current_payload["payload"]["purchase_invoice"]["number"]
        
        # Step 3: Save the complete payload for debugging
        with open(f"data/{invoice_id}_main_payload.json", "w") as json_file:
            json.dump(current_payload, json_file, indent=4)
        
        # Step 4: Push to API
        api_response = push_to_hbp_cmds(json.dumps(current_payload), invoice_id)
        
        if api_response and api_response.status_code == 200:
            logging.info(f"✅ Successfully processed invoice ID {invoice_id}")
            update_checklist(invoice_id, 'Success')
            return True
        else:
            status_code = api_response.status_code if api_response else "No response"
            logging.error(f"❌ Failed to push invoice ID {invoice_id} to API. Status: {status_code}")
            update_checklist(invoice_id, 'Failed', f"API push failed with status {status_code}")
            return False
            
    except Exception as e:
        logging.error(f"Exception processing invoice ID {invoice_id}: {str(e)}", exc_info=True)
        update_checklist(invoice_id, 'Error', str(e))
        return False

# Main execution block
logging.info("====== STARTING INVOICE PROCESSING JOB ======")
logging.info(f"Will process {len(invoice_list)} invoices")

# Ensure current_payload is defined before the loop
current_payload = None

# Process each invoice
success_count = 0
error_count = 0

for i, invoice_id in enumerate(invoice_list["id"]):
    logging.info(f"Processing invoice {i+1} of {len(invoice_list)}: {invoice_id}")
    
    try:
        if process_invoice(invoice_id):
            success_count += 1
        else:
            error_count += 1
    except Exception as e:
        logging.error(f"Unhandled exception processing invoice {invoice_id}: {e}", exc_info=True)
        update_checklist(invoice_id, 'Error', f"Unhandled exception: {str(e)}")
        error_count += 1
    
    # Add delay between requests to avoid rate limiting
    time.sleep(2)

    # Push to API after processing each invoice
    try:
        if current_payload:
            response = push_to_hbp_cmds(json.dumps(current_payload), invoice_id)
            if response and response.status_code == 200:
                logging.info(f"✅ Successfully pushed invoice ID {invoice_id} to API")
                update_checklist(invoice_id, 'Success')
            else:
                status_code = response.status_code if response else "No response"
                logging.error(f"❌ Failed to push invoice ID {invoice_id} to API. Status: {status_code}")
                update_checklist(invoice_id, 'Failed', f"API push failed with status {status_code}")
    except Exception as e:
        logging.error(f"Exception during API push for invoice ID {invoice_id}: {str(e)}", exc_info=True)
        update_checklist(invoice_id, 'Error', f"API push exception: {str(e)}")

# Log summary statistics
logging.info("====== COMPLETED INVOICE PROCESSING JOB ======")
logging.info(f"Total processed: {len(invoice_list)}")
logging.info(f"Successful: {success_count}")
logging.info(f"Failed: {error_count}")
logging.info(f"Log file: {log_filename}")

# Pseudo-code for the script flow:
# 1. Configure logging and create necessary directories (logs, data).
# 2. Read the list of invoices from 'voltera_pending_invoice_ids.csv'.
# 3. Limit the number of invoices to process for testing purposes.
# 4. Define a function to create a template for the API payload.
# 5. Define a function to send data to the Hyperbots API.
# 6. For each invoice in the list:
#    a. Fetch invoice data from the API.
#    b. Generate a unique ID for the transaction.
#    c. Update the payload with invoice and purchase order details.
#    d. Save the payload for debugging.
#    e. Push the payload to the Hyperbots API.
#    f. Log the success or failure of the operation.
# 7. Log the summary of the processing job (total, successful, failed invoices).