import pandas as pd
import sqlite3
import logging

CSV_FILE = 'Tyroo-dummy-data.csv.gz'
DB_FILE = 'Tyroo-dummy-data.db'
SCHEMA_FILE = 'schema.sql'
CHUNK_SIZE = 1000

TABLE_PRODUCTS = 'products'
TABLE_CATEGORIES = 'categories'
TABLE_SELLERS = 'sellers'

logging.basicConfig(
    filename='data_processing_relational_validated.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

TEXT_COLS = [
    'venture_category3_name_en', 'availability', 'venture_category2_name_en',
    'venture_category1_name_en', 'brand_name', 'business_type', 'business_area',
    'seller_name', 'venture_category_name_local', 'product_name', 'description'
]

URL_COLS = [
    'product_small_img', 'deeplink', 'image_url_5', 'product_medium_img',
    'image_url_4', 'seller_url', 'image_url_2', 'product_url', 'product_big_img',
    'image_url_3'
]

NUMERIC_COLS = [
    'platform_commission_rate', 'number_of_reviews', 'is_free_shipping',
    'promotion_price', 'current_price', 'product_commission_rate', 'sku_id',
    'seller_rating', 'bonus_commission_rate', 'discount_percentage',
    'product_id', 'rating_avg_value', 'price'
]

# def is_valid_url(url):
#     try:
#         if pd.isna(url):
#             return False
#         return str(url).startswith(('http://', 'https://', 'lazada://'))
#     except Exception as e:
#         logging.error(f"Error in is_valid_url: {e}")
#         return False

def clean_text_columns(df):
    try:
        for col in TEXT_COLS:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()
        # if 'availability' in df.columns:
        #     df['availability'] = df['availability'].str.lower()
        return df
    except Exception as e:
        logging.error(f"Error in clean_text_columns: {e}")
        return df

def clean_numeric_columns(df):
    try:
        for col in NUMERIC_COLS:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                df.loc[df[col] < 0, col] = pd.NA
                if col not in ['sku_id', 'product_id']:
                    median_val = df[col].median(skipna=True)
                    df[col] = df[col].fillna(median_val)
        return df
    except Exception as e:
        logging.error(f"Error in clean_numeric_columns: {e}")
        return df

# def filter_invalid_urls(df):
#     try:
#         total_invalid_rows = 0
#         for col in URL_COLS:
#             if col in df.columns:
#                 invalid_mask = ~df[col].apply(is_valid_url)
#                 invalid_count = invalid_mask.sum()
#                 if invalid_count > 0:
#                     logging.warning(f"{invalid_count} invalid URLs found in column '{col}'")
#                     print(f"\nInvalid URLs in column '{col}':")
#                     print(df.loc[invalid_mask, col])
#                     total_invalid_rows += invalid_count
#         return df, total_invalid_rows
#     except Exception as e:
#         logging.error(f"Error in filter_invalid_urls: {e}")
#         return df, 0

def remove_duplicates(df):
    try:
        if 'sku_id' in df.columns and 'product_id' in df.columns:
            before = len(df)
            df = df.drop_duplicates(subset=['sku_id', 'product_id'])
            removed = before - len(df)
            if removed > 0:
                logging.info(f"Removed {removed} duplicate rows based on sku_id and product_id")
            return df, removed
        return df, 0
    except Exception as e:
        logging.error(f"Error in remove_duplicates: {e}")
        return df, 0

def create_tables_from_file(conn, schema_file):
    try:
        with open(schema_file, 'r') as f:
            schema_sql = f.read()
        conn.executescript(schema_sql)
        logging.info("Database schema created from schema.sql")
    except Exception as e:
        logging.error(f"Error in create_tables_from_file: {e}")

def get_or_create_category(conn, category_name, parent_id=None):
    try:
        if not category_name or pd.isna(category_name) or category_name == '':
            return None
        cursor = conn.cursor()
        cursor.execute(f'''
            SELECT category_id FROM {TABLE_CATEGORIES}
            WHERE category_name = ? AND (parent_category_id IS ? OR parent_category_id = ?)
        ''', (category_name, parent_id, parent_id))
        row = cursor.fetchone()
        if row:
            return row[0]
        cursor.execute(f'''
            INSERT INTO {TABLE_CATEGORIES} (category_name, parent_category_id)
            VALUES (?, ?)
        ''', (category_name, parent_id))
        conn.commit()
        return cursor.lastrowid
    except Exception as e:
        logging.error(f"Error in get_or_create_category: {e}")
        return None

def get_or_create_seller(conn, seller_name, seller_url, seller_rating):
    try:
        if not seller_name or pd.isna(seller_name) or seller_name == '':
            return None
        cursor = conn.cursor()
        cursor.execute(f'''
            SELECT seller_id FROM {TABLE_SELLERS} WHERE seller_name = ?
        ''', (seller_name,))
        row = cursor.fetchone()
        if row:
            return row[0]
        cursor.execute(f'''
            INSERT INTO {TABLE_SELLERS} (seller_name, seller_url, seller_rating)
            VALUES (?, ?, ?)
        ''', (seller_name, seller_url, seller_rating))
        conn.commit()
        return cursor.lastrowid
    except Exception as e:
        logging.error(f"Error in get_or_create_seller: {e}")
        return None

def process_and_insert_chunk(df, conn):
    try:
        cursor = conn.cursor()
        inserted = 0
        for _, row in df.iterrows():
            cat1_id = get_or_create_category(conn, row.get('venture_category1_name_en'))
            cat2_id = get_or_create_category(conn, row.get('venture_category2_name_en'), cat1_id)
            cat3_id = get_or_create_category(conn, row.get('venture_category3_name_en'), cat2_id)
            seller_id = get_or_create_seller(conn, row.get('seller_name'), row.get('seller_url'), row.get('seller_rating'))

            product_data = {
                'sku_id': row.get('sku_id'),
                'product_id': row.get('product_id'),
                'product_name': row.get('product_name'),
                'venture_category1_id': cat1_id,
                'venture_category2_id': cat2_id,
                'venture_category3_id': cat3_id,
                'brand_name': row.get('brand_name'),
                'availability': row.get('availability'),
                'price': row.get('price'),
                'current_price': row.get('current_price'),
                'promotion_price': row.get('promotion_price'),
                'number_of_reviews': row.get('number_of_reviews'),
                'is_free_shipping': row.get('is_free_shipping'),
                'product_commission_rate': row.get('product_commission_rate'),
                'platform_commission_rate': row.get('platform_commission_rate'),
                'bonus_commission_rate': row.get('bonus_commission_rate'),
                'discount_percentage': row.get('discount_percentage'),
                'rating_avg_value': row.get('rating_avg_value'),
                'product_small_img': row.get('product_small_img'),
                'deeplink': row.get('deeplink'),
                'image_url_5': row.get('image_url_5'),
                'product_medium_img': row.get('product_medium_img'),
                'image_url_4': row.get('image_url_4'),
                'description': row.get('description'),
                'product_big_img': row.get('product_big_img'),
                'image_url_3': row.get('image_url_3'),
                'image_url_2': row.get('image_url_2'),
                'product_url': row.get('product_url'),
                'business_type': row.get('business_type'),
                'business_area': row.get('business_area'),
                'seller_id': seller_id
            }

            placeholders = ', '.join(['?'] * len(product_data))
            columns = ', '.join(product_data.keys())
            values = list(product_data.values())

            cursor.execute(f'''
                INSERT OR IGNORE INTO {TABLE_PRODUCTS} ({columns})
                VALUES ({placeholders})
            ''', values)
            inserted += 1
        conn.commit()
        logging.info(f"Inserted {inserted} products in this chunk.")
    except Exception as e:
        logging.error(f"Error in process_and_insert_chunk: {e}")

def process_chunk(df):
    try:
        initial_len = len(df)
        df = clean_text_columns(df)
        df = clean_numeric_columns(df)
        # df, invalid_urls = filter_invalid_urls(df)
        df, duplicates_removed = remove_duplicates(df)
        dropped = initial_len - len(df)
        return df, duplicates_removed, dropped
    except Exception as e:
        logging.error(f"Error in process_chunk: {e}")
        return df, 0, 0, 0

def process_csv():
    try:
        with sqlite3.connect(DB_FILE) as conn:
            create_tables_from_file(conn, SCHEMA_FILE)

            total_rows = total_inserted = total_duplicates = total_dropped = 0

            for chunk_num, chunk in enumerate(pd.read_csv(CSV_FILE, chunksize=CHUNK_SIZE), start=1):
                logging.info(f"Processing chunk {chunk_num} with {len(chunk)} rows")
                total_rows += len(chunk)
                cleaned, duplicates, dropped = process_chunk(chunk)
                total_duplicates += duplicates
                total_dropped += dropped

                if cleaned.empty:
                    logging.warning(f"Chunk {chunk_num} empty after cleaning, skipping insert")
                    continue

                process_and_insert_chunk(cleaned, conn)
                total_inserted += len(cleaned)

            logging.info("=== Processing Summary ===")
            logging.info(f"Total rows read: {total_rows}")
            logging.info(f"Total rows inserted: {total_inserted}")
            logging.info(f"Total rows dropped: {total_dropped}")
            logging.info(f" Duplicate rows: {total_duplicates}")
            logging.info(f" Other drops (numeric/cleaning): {total_dropped - total_duplicates}")
    except Exception as e:
        logging.error(f"Fatal error in process_csv: {e}")

if __name__ == "__main__":
    process_csv()
