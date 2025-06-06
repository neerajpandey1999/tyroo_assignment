  -- Create categories table
CREATE TABLE IF NOT EXISTS categories (
    category_id INTEGER PRIMARY KEY AUTOINCREMENT,
    category_name TEXT NOT NULL,
    parent_category_id INTEGER,
    UNIQUE(category_name, parent_category_id),
    FOREIGN KEY(parent_category_id) REFERENCES categories(category_id)
);

-- Create sellers table
CREATE TABLE IF NOT EXISTS sellers (
    seller_id INTEGER PRIMARY KEY AUTOINCREMENT,
    seller_name TEXT UNIQUE,
    seller_url TEXT,
    seller_rating REAL
);

-- Create products table
CREATE TABLE IF NOT EXISTS products (
    sku_id INTEGER,
    product_id INTEGER,
    product_name TEXT,
    venture_category1_id INTEGER,
    venture_category2_id INTEGER,
    venture_category3_id INTEGER,
    brand_name TEXT,
    availability TEXT,
    price REAL,
    current_price REAL,
    promotion_price REAL,
    number_of_reviews INTEGER,
    is_free_shipping INTEGER,
    product_commission_rate REAL,
    platform_commission_rate REAL,
    bonus_commission_rate REAL,
    discount_percentage REAL,
    rating_avg_value REAL,
    product_small_img TEXT,
    deeplink TEXT,
    image_url_5 TEXT,
    product_medium_img TEXT,
    image_url_4 TEXT,
    description TEXT,
    product_big_img TEXT,
    image_url_3 TEXT,
    image_url_2 TEXT,
    product_url TEXT,
    business_type TEXT,
    business_area TEXT,
    seller_id INTEGER,
    PRIMARY KEY (sku_id, product_id),
    FOREIGN KEY (venture_category1_id) REFERENCES categories(category_id),
    FOREIGN KEY (venture_category2_id) REFERENCES categories(category_id),
    FOREIGN KEY (venture_category3_id) REFERENCES categories(category_id),
    FOREIGN KEY (seller_id) REFERENCES sellers(seller_id)
);
