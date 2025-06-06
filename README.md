# Tyroo Assignment

This project processes a large compressed CSV file (`Tyroo-dummy-data.csv.gz`) in chunks, cleans and transforms the data, and stores it in a normalized SQLite database (`Tyroo-dummy-data.db`).

---

## Requirements

* Python 3.10
* pandas

Install dependencies:

```bash
pip install -r requirements.txt
```

**requirements.txt**

```
pandas
```

---

## Files

* `Tyroo-dummy-data.csv.gz`: Input data file (Download and store it in the same directory where main_script present).
* `schema.sql`: SQL script defining database schema.
* `Tyroo-dummy-data.db`: Output SQLite database (Automatically create).
* `process_csv.log`: Log file capturing all steps and errors (Automatically create).
* `main_script.py`: Main processing script.

---

## Usage

```bash
python main_script.py
```

---

## Data Processing Overview

### 1. Chunk Reading

Data is processed in chunks of 1000 rows for memory efficiency.

### 2. Text Cleaning

* Strips whitespace.
* Converts availability to lowercase.
* Formats brand names with `.title()`.

### 3. Numeric Cleaning

* Converts values to numeric.
* Removes negatives.
* Fills missing values (except for ID fields) with column median.

### 4. URL Validation

* Checks for valid URL prefixes (`http`, `https`, `lazada`).
* Drops rows with invalid URLs.

### 5. Duplicate Removal

* Drops duplicates based on `sku_id` and `product_id`.

### 6. Relational Mapping

* Maintains category hierarchy across three levels.
* Associates products with sellers.
* Uses helper functions to insert or retrieve category/seller records.

---

## Schema Design

**categories**

* `category_id` (PK)
* `category_name`
* `parent_category_id` (FK to categories)

**sellers**

* `seller_id` (PK)
* `seller_name`
* `seller_url`
* `seller_rating`

**products**

* `sku_id`
* `product_id`
* Various product-related attributes
* FK to all 3 category levels and seller

Each product record:

* Belongs to a seller
* Is linked to 3 category levels (nested hierarchy)

---

## Logging

All activity is logged to `process_csv.log`:

* Row counts
* Warnings (e.g., invalid URLs, duplicates)
* Errors
* Inserted product counts

---

## Output Summary

At the end of execution:

* Total rows read
* Total rows inserted
* Invalid URL count
* Duplicate count
* Other dropped rows

---

## Features

| Feature            | Description                                        |
| ------------------ | -------------------------------------------------- |
| Chunked processing | Handles large files efficiently                    |
| Text cleanup       | Formatting and normalization of text fields        |
| URL filtering      | Removes rows with invalid URLs                     |
| Duplicate removal  | Based on `sku_id` and `product_id`                 |
| Relational storage | Categories and sellers stored in normalized tables |
| Logging            | Detailed logs for auditing and debugging           |

---

## Sample Execution

```bash
python main_script.py
```

Check:

* `Tyroo-dummy-data.db` for data
* `process_csv.log` for logs
