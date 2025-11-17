import pandas as pd
import random
from faker import Faker
from datetime import datetime, timedelta
import os
import logging

# --- Configuration ---
# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('generator.log')
    ]
)

# File paths from your .env (or define here)
BASE_PATH = 'data_files/' 
file_paths = {
    'crm_cust_info': os.path.join(BASE_PATH, 'cust_info.csv'),
    'crm_sales_details': os.path.join(BASE_PATH, 'sales_details.csv'),
    'crm_prd_info': os.path.join(BASE_PATH, 'prd_info.csv'),
    'erp_cust_az12': os.path.join(BASE_PATH, 'CUST_AZ12.csv'),
    'erp_loc_a101': os.path.join(BASE_PATH, 'LOC_A101.csv'),
    'erp_px_cat_g1v2': os.path.join(BASE_PATH, 'PX_CAT_G1V2.csv')
}

# Simulation settings
NUM_NEW_CUSTOMERS = 100
NUM_CUSTOMER_UPDATES = 25
NUM_CUSTOMER_MOVES = 15     # NEW: Simulate customers changing location
NUM_NEW_PRODUCTS = 25       # NEW: Simulate new products being added
NUM_PRODUCT_UPDATES = 125   # Price changes
NUM_PRODUCT_DISCONTINUES = 5 # NEW: Simulate products being discontinued
NUM_NEW_SALES = 1000

# Initialize Faker
fake = Faker()

# --- Helper Functions ---

def get_today():
    """Returns today's date in YYYYMMDD format for sales/dates."""
    return datetime.now().strftime('%Y%m%d')

def get_today_iso():
    """Returns today's date in YYYY-MM-DD format for create dates."""
    return datetime.now().strftime('%Y-%m-%d')

def get_future_date(base_date_str, days):
    """Adds days to a YYYYMMDD date string."""
    base_date = datetime.strptime(base_date_str, '%Y%m%d')
    return (base_date + timedelta(days=days)).strftime('%Y%m%d')

def generate_new_sales_order_id(max_id):
    """Increments the max sales order ID (e.g., SO75117 -> SO75118)."""
    prefix = max_id[:2]
    number = int(max_id[2:]) + 1
    return f"{prefix}{number}"

def generate_new_customer_id(max_id):
    """Increments the max customer ID (e.g., 29465 -> 29466)."""
    return int(max_id) + 1

def generate_new_customer_key(max_key):
    """Increments the max customer key (e.g., AW00029465 -> AW00029466)."""
    prefix = max_key[:5]
    number = int(max_key[5:]) + 1
    return f"{prefix}{number}"

def generate_new_product_key(category_id):
    """Generates a new, fake product key based on a category ID (e.g., 'BI_RB' -> 'BI-RB-FK-0001')."""
    prefix = category_id.replace('_', '-')
    suffix = fake.bothify(text='??-####').upper()
    return f"{prefix}-{suffix}"

# --- Main Data Generation Logic ---

def generate_data():
    """Main function to run the data generation simulation."""
    logging.info("Starting daily data generation simulation...")
    
    try:
        # --- 1. Load Base Data ---
        logging.info("Loading base CSV files...")
        # We use keep_default_na=False to treat empty strings as '', not NaN
        df_cust = pd.read_csv(file_paths['crm_cust_info'], keep_default_na=False)
        df_sales = pd.read_csv(file_paths['crm_sales_details'], keep_default_na=False)
        df_prod = pd.read_csv(file_paths['crm_prd_info'], keep_default_na=False)
        df_cust_az12 = pd.read_csv(file_paths['erp_cust_az12'], keep_default_na=False)
        df_loc_a101 = pd.read_csv(file_paths['erp_loc_a101'], keep_default_na=False)
        df_px_cat = pd.read_csv(file_paths['erp_px_cat_g1v2'], keep_default_na=False)
        
        # We need valid pools of IDs to create realistic sales
        valid_cust_ids = df_cust['cst_id'].tolist()
        valid_prod_keys = df_prod['prd_key'].tolist()
        valid_categories = df_px_cat['ID'].tolist()
        
        # --- 2. Simulate Customer Updates ---
        logging.info(f"Simulating {NUM_CUSTOMER_UPDATES} customer marital status updates...")
        update_indices = random.sample(range(len(df_cust)), NUM_CUSTOMER_UPDATES)
        for idx in update_indices:
            new_status = random.choice(['M', 'S'])
            old_status = df_cust.at[idx, 'cst_marital_status']
            df_cust.at[idx, 'cst_marital_status'] = new_status
            logging.debug(f"Updating customer {df_cust.at[idx, 'cst_id']}: Marital Status {old_status} -> {new_status}")
            
        logging.info(f"Simulating {NUM_CUSTOMER_MOVES} customer location moves...")
        move_indices = random.sample(range(len(df_cust)), NUM_CUSTOMER_MOVES)
        # Use a list of countries for moves
        country_list = ['Australia', 'US', 'Canada', 'Germany', 'France', 'United Kingdom']
        for idx in move_indices:
            customer_key = df_cust.at[idx, 'cst_key']
            # Find this customer in the location table
            loc_index = df_loc_a101.index[df_loc_a101['CID'] == customer_key].tolist()
            if loc_index:
                loc_idx = loc_index[0]
                old_country = df_loc_a101.at[loc_idx, 'CNTRY']
                new_country = random.choice([c for c in country_list if c != old_country])
                df_loc_a101.at[loc_idx, 'CNTRY'] = new_country
                logging.debug(f"Updating customer {customer_key}: Country {old_country} -> {new_country}")
            
        # --- 3. Simulate Product Updates ---
        logging.info(f"Simulating {NUM_PRODUCT_UPDATES} product price updates...")
        update_indices = random.sample(range(len(df_prod)), NUM_PRODUCT_UPDATES)
        for idx in update_indices:
            old_cost_str = df_prod.at[idx, 'prd_cost']
            
            try:
                old_cost_float = float(old_cost_str)
                new_cost = round(old_cost_float * random.uniform(0.95, 1.05), 2) # +/- 5%
                df_prod.at[idx, 'prd_cost'] = new_cost
                logging.debug(f"Updating product {df_prod.at[idx, 'prd_id']}: Cost {old_cost_str} -> {new_cost}")
            except ValueError:
                logging.warning(f"Skipping product update for {df_prod.at[idx, 'prd_id']}: invalid cost value '{old_cost_str}'")

        logging.info(f"Simulating {NUM_PRODUCT_DISCONTINUES} product discontinuations...")
        # Find products that are not already discontinued (no end date)
        active_prod_indices = df_prod.index[df_prod['prd_end_dt'] == ''].tolist()
        if len(active_prod_indices) > NUM_PRODUCT_DISCONTINUES:
            discontinue_indices = random.sample(active_prod_indices, NUM_PRODUCT_DISCONTINUES)
            for idx in discontinue_indices:
                df_prod.at[idx, 'prd_end_dt'] = get_today_iso()
                logging.debug(f"Discontinuing product {df_prod.at[idx, 'prd_id']}")

        # --- 4. Simulate New Products ---
        logging.info(f"Simulating {NUM_NEW_PRODUCTS} new products...")
        valid_prd_ids = pd.to_numeric(df_prod['prd_id'], errors='coerce')
        max_prd_id = int(valid_prd_ids.dropna().max())
        
        new_products_list = []
        for _ in range(NUM_NEW_PRODUCTS):
            max_prd_id += 1
            new_id = max_prd_id
            category = random.choice(valid_categories)
            new_key = generate_new_product_key(category)
            
            new_product_row = {
                'prd_id': new_id,
                'prd_key': new_key,
                'prd_nm': fake.bs().title(), # Fake product name
                'prd_cost': round(random.uniform(10, 1000), 2),
                'prd_line': random.choice(['M', 'R', 'T', 'S']), # Random line
                'prd_start_dt': get_today_iso(),
                'prd_end_dt': ''
            }
            new_products_list.append(new_product_row)
            # INTEGRATION: Add new key to the pool for today's sales
            valid_prod_keys.append(new_key)
            
        df_prod = pd.concat([df_prod, pd.DataFrame(new_products_list)], ignore_index=True)
        logging.info(f"Added {len(new_products_list)} new product records.")

        # --- 5. Simulate New Customers ---
        logging.info(f"Simulating {NUM_NEW_CUSTOMERS} new customers...")
        valid_cst_ids = pd.to_numeric(df_cust['cst_id'], errors='coerce')
        max_cst_id = int(valid_cst_ids.dropna().max())
        
        valid_cst_keys_series = df_cust[
            pd.notna(df_cust['cst_key']) & 
            (df_cust['cst_key'].str.startswith('AW000')) &
            (df_cust['cst_key'].str.len() > 5)
        ]['cst_key']
        
        if valid_cst_keys_series.empty:
            logging.error("CRITICAL: No valid 'cst_key' records found starting with 'AW000'. Cannot generate new keys.")
            raise ValueError("No valid 'AW000' customer keys found to determine max key.")
        max_cst_key = valid_cst_keys_series.max()
        
        new_cust_rows = []
        new_az12_rows = []
        new_loc_rows = []
        
        for _ in range(NUM_NEW_CUSTOMERS):
            max_cst_id = generate_new_customer_id(max_cst_id)
            max_cst_key = generate_new_customer_key(max_cst_key)
            gender = random.choice(['M', 'F'])
            
            new_cust_rows.append({
                'cst_id': max_cst_id,
                'cst_key': max_cst_key,
                'cst_firstname': fake.first_name(),
                'cst_lastname': fake.last_name(),
                'cst_marital_status': random.choice(['M', 'S']),
                'cst_gndr': gender,
                'cst_create_date': get_today_iso()
            })
            
            new_az12_rows.append({
                'CID': max_cst_key,
                'BDATE': fake.date_of_birth(minimum_age=18, maximum_age=70).isoformat(),
                'GEN': 'Male' if gender == 'M' else 'Female'
            })
            
            new_loc_rows.append({
                'CID': max_cst_key,
                'CNTRY': random.choice(country_list)
            })
            
            # INTEGRATION: Add new customer ID to the valid pool for today's sales
            valid_cust_ids.append(max_cst_id)
        
        # Append new rows to the dataframes
        df_cust = pd.concat([df_cust, pd.DataFrame(new_cust_rows)], ignore_index=True)
        df_cust_az12 = pd.concat([df_cust_az12, pd.DataFrame(new_az12_rows)], ignore_index=True)
        df_loc_a101 = pd.concat([df_loc_a101, pd.DataFrame(new_loc_rows)], ignore_index=True)
        logging.info(f"Added {len(new_cust_rows)} new customer records.")

        # --- 6. Simulate New Sales ---
        logging.info(f"Simulating {NUM_NEW_SALES} new sales orders...")
        
        valid_sales_ord_nums = df_sales[
            pd.notna(df_sales['sls_ord_num']) &
            (df_sales['sls_ord_num'].str.startswith('SO')) &
            (df_sales['sls_ord_num'].str.len() > 2)
        ]['sls_ord_num']
        
        if valid_sales_ord_nums.empty:
            logging.error("CRITICAL: No valid 'sls_ord_num' records found starting with 'SO'. Cannot generate new sales.")
            raise ValueError("No valid 'SO' sales order numbers found to determine max.")
            
        max_sales_ord_num = valid_sales_ord_nums.max()
        
        new_sales_rows = []
        today_str = get_today()
        
        for _ in range(NUM_NEW_SALES):
            max_sales_ord_num = generate_new_sales_order_id(max_sales_ord_num)
            price = round(random.uniform(5, 500), 2)
            quantity = random.randint(1, 5)
            
            new_sales_rows.append({
                'sls_ord_num': max_sales_ord_num,
                'sls_prd_key': random.choice(valid_prod_keys),
                'sls_cust_id': random.choice(valid_cust_ids),
                'sls_order_dt': today_str,
                'sls_ship_dt': get_future_date(today_str, random.randint(1, 5)),
                'sls_due_dt': get_future_date(today_str, random.randint(7, 14)),
                'sls_sales': price * quantity,
                'sls_quantity': quantity,
                'sls_price': price
            })
            
        # Append new sales
        df_sales = pd.concat([df_sales, pd.DataFrame(new_sales_rows)], ignore_index=True)
        logging.info(f"Added {len(new_sales_rows)} new sales records.")

        # --- 7. Overwrite CSVs ---
        logging.info("Saving generated data back to CSV files...")
        
        # We save all 5 files that were modified.
        # PX_CAT_G1V2.csv was read-only, so we don't save it.
        
        df_cust.to_csv(file_paths['crm_cust_info'], index=False, lineterminator='\r\n')
        df_sales.to_csv(file_paths['crm_sales_details'], index=False, lineterminator='\r\n')
        df_prod.to_csv(file_paths['crm_prd_info'], index=False, lineterminator='\r\n')
        df_cust_az12.to_csv(file_paths['erp_cust_az12'], index=False, lineterminator='\r\n')
        df_loc_a101.to_csv(file_paths['erp_loc_a101'], index=False, lineterminator='\r\n')
        
        logging.info("Data generation simulation finished successfully.")

    except Exception as e:
        logging.error(f"Data generation FAILED: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    generate_data()