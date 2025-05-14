import streamlit as st
import json
import time
import pandas as pd
import requests
import uuid
import logging
import csv
import os
from datetime import datetime
import traceback

# ---------- Configuration ----------
LOG_DIR = "logs"
DATA_DIR = "data"
TRACK_DIR = "tracking"
DATA_DUMP_DIR = "data_dump"  # New directory for data dumps

# Create necessary directories
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(TRACK_DIR, exist_ok=True)
os.makedirs(DATA_DUMP_DIR, exist_ok=True)  # Create data_dump directory if it doesn't exist

# Generate a unique log ID for this run
LOG_ID = f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
DATA_DUMP_SESSION_DIR = os.path.join(DATA_DUMP_DIR, LOG_ID)
os.makedirs(DATA_DUMP_SESSION_DIR, exist_ok=True)  # Create session-specific data dump directory

# Sidebar inputs
st.sidebar.title("Configuration")
# Load previously saved org IDs
orgid_file = os.path.join(os.path.dirname(__file__), "orgid.csv")
saved_orgids = []

if os.path.exists(orgid_file):
    with open(orgid_file, "r") as f:
        reader = csv.reader(f)
        saved_orgids = [row[0] for row in reader]

# Organization ID input and selection
org_id = st.sidebar.selectbox("Select a saved Organization ID", options=[""] + saved_orgids, key="org_id_select")
if not org_id:
    org_id = st.sidebar.text_input("Enter new Organization ID", key="org_id_input")
    if st.sidebar.button("Save Organization ID"):
        if org_id:
            with open(orgid_file, "a", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([org_id])
            st.sidebar.success("Organization ID saved successfully!")
        else:
            st.sidebar.error("Organization ID cannot be empty.")
# Load previously saved tokens
token_file = os.path.join(os.path.dirname(__file__), "token.csv")
saved_tokens = []

if os.path.exists(token_file):
    with open(token_file, "r") as f:
        reader = csv.reader(f)
        saved_tokens = [row[0] for row in reader]

# Token input and selection
token = st.sidebar.selectbox("Select a saved API Token", options=[""] + saved_tokens, key="api_token_select")
if not token:
    token = st.sidebar.text_input("Enter new API Token", key="api_token_input")
    if st.sidebar.button("Save Token"):
        if token:
            with open(token_file, "a", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([token])
            st.sidebar.success("Token saved successfully!")
        else:
            st.sidebar.error("Token cannot be empty.")

# Input of invoice IDs
st.title("Dummy Invoice Writing test")
st.markdown("Enter invoice IDs (one per line) or upload a CSV with a column 'id'.")

uploaded_file = st.file_uploader("Upload CSV", type="csv")
if uploaded_file:
    df_in = pd.read_csv(uploaded_file)
    invoice_list = df_in["id"].astype(str).tolist()
else:
    raw_ids = st.text_area("Invoice IDs")
    invoice_list = [i.strip() for i in raw_ids.splitlines() if i.strip()]

# Initialize logs with better formatting
log_filename = f"{LOG_DIR}/invoice_processing_{LOG_ID}.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s",
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger()

# Display current run info
logger.info(f"Starting new processing run with LOG_ID: {LOG_ID}")
logger.info(f"Data dump directory: {DATA_DUMP_SESSION_DIR}")

# Tracking files
checklist_file = os.path.join(TRACK_DIR, f'invoices_checklist_{LOG_ID}.csv')
id_mapping_file = os.path.join(TRACK_DIR, f'inv_del_ids_{LOG_ID}.csv')

# Create tracking files with headers
with open(checklist_file, 'w', newline='') as f:
    csv.writer(f).writerow(['Invoice ID', 'Status', 'Error Message', 'Timestamp', 'Duration_ms', 'Log_ID'])

with open(id_mapping_file, 'w', newline='') as f:
    csv.writer(f).writerow(['UUID', 'Invoice Number', 'Invoice ID', 'Timestamp', 'Log_ID'])

# Utility functions
def get_orgid():
    return org_id

def get_token():
    return token

def update_checklist(invoice_id, status, error_message=None, duration_ms=None):
    with open(checklist_file, 'a', newline='') as f:
        csv.writer(f).writerow([
            invoice_id, 
            status, 
            error_message or '', 
            datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            duration_ms or '',
            LOG_ID
        ])
    logger.info(f"Updated checklist: Invoice {invoice_id} - Status: {status} - Duration: {duration_ms}ms")

def record_id_mapping(uuid_value, invoice_number, invoice_id):
    with open(id_mapping_file, 'a', newline='') as f:
        csv.writer(f).writerow([
            uuid_value, 
            invoice_number, 
            invoice_id, 
            datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            LOG_ID
        ])
    logger.info(f"Recorded ID mapping: UUID {uuid_value} -> Invoice {invoice_number} (ID: {invoice_id})")

def save_to_data_dump(data, file_prefix, file_suffix, invoice_id):
    """Save fetched data to the data dump directory with a unique filename"""
    filename = f"{file_prefix}_{invoice_id}_{file_suffix}_{datetime.now().strftime('%Y%m%d%H%M%S')}.json"
    filepath = os.path.join(DATA_DUMP_SESSION_DIR, filename)
    try:
        with open(filepath, 'w') as f:
            if isinstance(data, dict) or isinstance(data, list):
                json.dump(data, f, indent=2)
            else:
                f.write(str(data))
        logger.debug(f"Saved data to dump: {filepath}")
        return filepath
    except Exception as e:
        logger.error(f"Failed to save data dump {filepath}: {e}")
        return None

# API call functions
def push_to_hbp_cmds(payload, invoice_id):
    url = f"https://api.hyperbots.com/data-streams/test/HYPRBOTS-COMMANDS-{get_orgid()}"
    headers = {
        'token': get_token(),
        'Content-Type': 'application/json',
    }
    start_time = time.time()
    try:
        logger.info(f"Pushing payload to API for invoice {invoice_id}")
        resp = requests.post(url, headers=headers, data=payload, timeout=30)
        duration_ms = int((time.time() - start_time) * 1000)
        logger.info(f"API response received in {duration_ms}ms with status code {resp.status_code}")
        
        # Save response to both locations
        with open(f"{DATA_DIR}/{invoice_id}_api_response.json", 'w') as f:
            f.write(resp.text)
        
        save_to_data_dump(resp.text, "api_response", "push", invoice_id)
        
        return resp, duration_ms
    except Exception as e:
        duration_ms = int((time.time() - start_time) * 1000)
        logger.error(f"API push error for {invoice_id} after {duration_ms}ms: {e}")
        logger.error(f"Stack trace: {traceback.format_exc()}")
        return None, duration_ms


def get_po_details(invoice_number, po):
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
        'x-browser-id': 'ohl5c9',
        'x-session-id': '06cebcc2-7ffe-4243-8f7c-d7d8fac07a8e',
        'Cookie': f'AMP_MKTG_ffa2a025be=JTdCJTdE; HYPRBOTS_TOKEN={get_token()}'
    }
    start_time = time.time()
    try:
        logger.info(f"Fetching PO details for invoice {invoice_number} with PO {po}")
        resp = requests.get(url, headers=headers, timeout=30)
        duration_ms = int((time.time() - start_time) * 1000)
        
        if resp.status_code == 200:
            data = resp.json()
            logger.info(f"Successfully fetched PO details in {duration_ms}ms")
            
            # Save to both locations
            with open(f"{DATA_DIR}/{invoice_number}_{po}_podata.json", 'w') as f:
                json.dump(data, f, indent=2)
            
            save_to_data_dump(data, "po_details", f"po_{po}", invoice_number)
            
            return data, duration_ms
        else:
            logger.warning(f"Failed to fetch PO details: status {resp.status_code} in {duration_ms}ms")
            save_to_data_dump({"status_code": resp.status_code, "text": resp.text}, 
                             "po_details_error", f"po_{po}", invoice_number)
            return None, duration_ms
    except Exception as e:
        duration_ms = int((time.time() - start_time) * 1000)
        logger.error(f"PO details error for {invoice_number}-{po} after {duration_ms}ms: {e}")
        logger.error(f"Stack trace: {traceback.format_exc()}")
        return None, duration_ms

# Payload template
def create_payload_template():
    return {
        'id': '',
        'action': 'WRITE',
        'type': 'INVOICES',
        'payload': {'purchase_invoice': {}, 'purchase_orders': {}, 'purchase_receipts': {}},
        'context': {'orgId': get_orgid(), 'number': '', 'vendor_key': '', 'user_id': '', 'purchase_invoice_id': ''}
    }

# Invoice processing
def process_invoice(invoice_id):
    process_start_time = time.time()
    logger.info(f"=== Starting processing of invoice {invoice_id} ===")
    try:
        # Fetch invoice
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
            'x-browser-id': 'ohl5c9',
            'x-session-id': '06cebcc2-7ffe-4243-8f7c-d7d8fac07a8e',
            'Cookie': f'AMP_MKTG_ffa2a025be=JTdCJTdE; HYPRBOTS_TOKEN={get_token()}'
        }
        
        fetch_start_time = time.time()
        logger.info(f"Fetching invoice data for ID: {invoice_id}")
        resp = requests.get(url, headers=headers, timeout=30)
        fetch_duration_ms = int((time.time() - fetch_start_time) * 1000)
        
        if resp.status_code != 200:
            error_msg = f'Fetch error {resp.status_code} in {fetch_duration_ms}ms'
            logger.error(error_msg)
            save_to_data_dump({"status_code": resp.status_code, "text": resp.text}, 
                             "invoice_fetch_error", "response", invoice_id)
            total_duration_ms = int((time.time() - process_start_time) * 1000)
            update_checklist(invoice_id, 'Failed', error_msg, total_duration_ms)
            return False
        
        invoice_data = resp.json()
        logger.info(f"Successfully fetched invoice data in {fetch_duration_ms}ms")
        
        # Save the fetched invoice data
        save_to_data_dump(invoice_data, "invoice_data", "raw", invoice_id)
        
        # Create UUID and record mapping
        new_uuid = str(uuid.uuid4())
        invoice_number = invoice_data.get('number', '')
        logger.info(f"Generated UUID {new_uuid} for invoice {invoice_number} (ID: {invoice_id})")
        record_id_mapping(new_uuid, invoice_number, invoice_id)

        # Create payload
        payload = create_payload_template()
        payload['id'] = new_uuid
        payload['context']['number'] = 'TEST'+invoice_number
        payload['payload']['purchase_invoice'] = invoice_data
        payload['payload']['purchase_invoice']['number'] = 'TEST'+invoice_number
        payload['context']['purchase_invoice_id'] = invoice_id
        
        # Additional context info if available
        if 'vendor' in invoice_data and isinstance(invoice_data['vendor'], dict):
            payload['context']['vendor_key'] = invoice_data['vendor'].get('key', '')
            logger.debug(f"Added vendor key {payload['context']['vendor_key']} to payload context")

        # Process POs
        logger.info(f"Processing line items and POs for invoice {invoice_id}")
        items = invoice_data.get('line_items', [])
        logger.info(f"Found {len(items)} line items")
        po_payload = {}
        for idx, item in enumerate(items):
            po_no = item.get('line_item/purchase_order_number')
            if po_no:
                logger.info(f"Processing line item {idx+1}/{len(items)} with PO: {po_no}")
                po_data, po_duration = get_po_details(invoice_number, po_no)
                if po_data:
                    logger.info(f"Successfully retrieved PO data for {po_no}")
                    po_payload[po_no] = po_data[0]
                else:
                    logger.error(f"No PO data found for {po_no}")
                    break
            else:
                logger.debug(f"Line item {idx+1} has no PO number")
                
        logger.info(f"Completed PO processing: {len(po_payload)} POs retrieved")
        payload['payload']['purchase_orders'] = po_payload

        # Save payload to both locations
        logger.info(f"Saving final payload for invoice {invoice_id}")
        with open(f"{DATA_DIR}/{invoice_id}_payload.json", 'w') as f:
            json.dump(payload, f, indent=2)
        
        payload_file = save_to_data_dump(payload, "final_payload", "prepared", invoice_id)
        logger.info(f"Payload saved to {payload_file}")

        # Push to API
        logger.info(f"Pushing payload to API for invoice {invoice_id}")
        resp_push, push_duration = push_to_hbp_cmds(json.dumps(payload), invoice_id)
        
        total_duration_ms = int((time.time() - process_start_time) * 1000)
        
        if resp_push and resp_push.status_code == 200:
            logger.info(f"API push successful for invoice {invoice_id} in {push_duration}ms")
            update_checklist(invoice_id, 'Success', None, total_duration_ms)
            return True, new_uuid
        else:
            status = resp_push.status_code if resp_push else 'No response'
            error_msg = f'Push failed with status {status} in {push_duration}ms'
            logger.error(error_msg)
            update_checklist(invoice_id, 'Failed', error_msg, total_duration_ms)
            return False, new_uuid
            
    except Exception as e:
        total_duration_ms = int((time.time() - process_start_time) * 1000)
        error_trace = traceback.format_exc()
        logger.error(f"Error processing {invoice_id} after {total_duration_ms}ms: {e}")
        logger.error(f"Stack trace: {error_trace}")
        
        # Save error information
        save_to_data_dump({
            "error": str(e),
            "traceback": error_trace,
            "duration_ms": total_duration_ms
        }, "processing_error", "exception", invoice_id)
        
        update_checklist(invoice_id, 'Error', str(e), total_duration_ms)
        return False
    finally:
        logger.info(f"=== Completed processing of invoice {invoice_id} in {int((time.time() - process_start_time) * 1000)}ms ===")

# Main app logic
st.sidebar.write(f"Current Log ID: {LOG_ID}")
st.sidebar.write(f"Data Dump Path: {DATA_DUMP_SESSION_DIR}")

if st.button("Start Processing"):
    if not org_id or not token or not invoice_list:
        st.error("Please fill Org ID, Token and at least one Invoice ID.")
        logger.warning("Processing attempt with missing required fields")
    else:
        progress = st.progress(0)
        results = []
        total = len(invoice_list)
        
        logger.info(f"Starting batch processing of {total} invoices")
        
        start_time = time.time()
        success_count = 0
        
        for idx, inv in enumerate(invoice_list):
            st.write(f"Processing invoice {idx + 1} of {total}: {inv}")
            success, new_id = process_invoice(inv)
            if success:
                success_count += 1
                
            results.append({'invoice_id': inv, 'new_invoice_ID':new_id, 'status': 'Success' if success else 'Failed'})
            progress.progress((idx + 1) / total)
            
            remaining = total - (idx + 1)
            elapsed_time = time.time() - start_time
            avg_time_per_invoice = elapsed_time / (idx + 1) if idx > 0 else 0
            estimated_remaining_time = avg_time_per_invoice * remaining
            
            st.write(f"Invoices processed: {idx + 1}, Succeeded: {success_count}, Failed: {idx + 1 - success_count}")
            st.write(f"Remaining: {remaining}, Est. time remaining: {estimated_remaining_time:.1f} seconds")
            
            # Small delay to avoid rate limiting
            time.sleep(1)
        
        total_time = time.time() - start_time
        logger.info(f"Batch processing completed in {total_time:.1f} seconds")
        logger.info(f"Results: {success_count} succeeded, {total - success_count} failed")
        
        st.success(f"Processing completed in {total_time:.1f} seconds")
        results_df = pd.DataFrame(results)
        
        # Add a download button for the results table
        csv_data = results_df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="Download Results as CSV",
            data=csv_data,
            file_name=f"results_summary_{LOG_ID}.csv",
            mime="text/csv"
        )
        st.table(results_df)
        
        # Display summary metrics
        success_rate = (success_count / total) * 100 if total > 0 else 0
        st.write(f"Success rate: {success_rate:.1f}%")
        
        # Save results to CSV
        results_csv = os.path.join(DATA_DUMP_SESSION_DIR, f"results_summary_{LOG_ID}.csv")
        results_df.to_csv(results_csv, index=False)
        st.write(f"Results saved to {results_csv}")

st.sidebar.markdown(f"Log file: {log_filename}")
st.sidebar.markdown(f"Data dumps: {DATA_DUMP_SESSION_DIR}")

# Add a download button for the log file
if os.path.exists(log_filename):
    with open(log_filename, "r") as file:
        log_content = file.read()
    st.sidebar.download_button(
        label="Download Log File",
        data=log_content,
        file_name=f"invoice_processing_{LOG_ID}.log",
        mime="text/plain"
    )