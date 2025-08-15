from __future__ import annotations
import sqlite3
from pathlib import Path
import pandas as pd
import logging

# -------------------- Paths --------------------
BASE_DIR = Path(__file__).resolve().parent
RAW_CSV = BASE_DIR / "data" / "raw" / "raw_sales_data.csv"
DB_PATH = BASE_DIR / "data" / "warehouse" / "sales.db"
TABLE_NAME = "sales_clean"

# -------------------- Logging --------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s | %(message)s"
)
log = logging.getLogger("etl")

# -------------------- Extract --------------------
def extract_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"CSV not found at {path}")
    log.info(f"Reading raw CSV: {path}")
    df = pd.read_csv(path)
    return df

# -------------------- Transform --------------------
def transform(df: pd.DataFrame) -> pd.DataFrame:
    log.info("Starting transform step")

    # Standardize column names
    df = df.rename(columns=lambda c: c.strip().lower())

    # Ensure required columns exist
    required = {"date", "region", "product", "sales", "quantity", "profit"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns in CSV: {missing}")

    # Parse date
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    # Coerce numeric columns
    for col in ["sales", "quantity", "profit"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Clean values
    df["region"] = df["region"].fillna("Unknown").replace({"": "Unknown"})
    df = df.drop_duplicates()

    # Drop impossible or incomplete rows
    before = len(df)
    df = df.dropna(subset=["date", "product", "quantity", "sales", "profit"])
    df = df[(df["quantity"] > 0) & (df["sales"] > 0)]
    after = len(df)
    log.info(f"Dropped {before - after} bad rows")

    # Add derived fields
    df["unit_price"] = (df["sales"] / df["quantity"]).round(2)
    df["profit_margin"] = (df["profit"] / df["sales"]).round(4)
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month

    # Order columns nicely
    cols = [
        "date", "year", "month", "region", "product",
        "quantity", "unit_price", "sales", "profit", "profit_margin"
    ]
    df = df[cols].sort_values(["date", "region", "product"]).reset_index(drop=True)

    log.info(f"Transform complete: {len(df)} rows")
    return df

# -------------------- Load --------------------
def load_to_sqlite(df: pd.DataFrame, db_path: Path, table: str) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    log.info(f"Loading into SQLite: {db_path} (table: {table})")
    with sqlite3.connect(db_path) as conn:
        df.to_sql(table, conn, if_exists="replace", index=False)
        # Helpful indices for query performance
        conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_date ON {table}(date);")
        conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_region ON {table}(region);")
        conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_product ON {table}(product);")
    log.info("Load complete")

# -------------------- Main --------------------

def main():
    df_raw = extract_csv(RAW_CSV)
    df_clean = transform(df_raw)
    load_to_sqlite(df_clean, DB_PATH, TABLE_NAME)
    log.info("ETL finished successfully ✅")
    log.info(f"Database: {DB_PATH}")
    log.info(f"Table: {TABLE_NAME} | Rows: {len(df_clean)}")

   
    log.info("Running example queries...")
    queries = {
        "total_sales_by_region": f"""
            SELECT region, ROUND(SUM(sales), 2) AS total_sales
            FROM {TABLE_NAME}
            GROUP BY region
            ORDER BY total_sales DESC;
        """,
        "top_5_products_by_profit": f"""
            SELECT product, ROUND(SUM(profit), 2) AS total_profit
            FROM {TABLE_NAME}
            GROUP BY product
            ORDER BY total_profit DESC
            LIMIT 5;
        """,
        "monthly_sales_summary": f"""
            SELECT year, month, ROUND(SUM(sales), 2) AS total_sales
            FROM {TABLE_NAME}
            GROUP BY year, month
            ORDER BY year, month;
        """,
        "average_profit_margin_per_product": f"""
            SELECT product, ROUND(AVG(profit_margin * 100), 2) AS avg_profit_margin_percent
            FROM {TABLE_NAME}
            GROUP BY product
            ORDER BY avg_profit_margin_percent DESC;
        """
    }

    with sqlite3.connect(DB_PATH) as conn:
        for name, sql in queries.items():
            df_result = pd.read_sql_query(sql, conn)
            print(f"\n--- {name.replace('_', ' ').title()} ---")
            print(df_result)
            # Save result to CSV
            output_file = BASE_DIR / "data" / "warehouse" / f"{name}.csv"
            df_result.to_csv(output_file, index=False)
            log.info(f"Saved query result: {output_file}")
