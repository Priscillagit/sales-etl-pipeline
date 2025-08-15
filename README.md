# Sales ETL Pipeline

A simple ETL (Extract, Transform, Load) pipeline for cleaning and storing sales data in a SQLite database.

## Features
- **Extract**: Reads raw sales CSV data.
- **Transform**: Cleans, formats, and enriches the data.
- **Load**: Stores the cleaned data into a SQLite database with indexes for fast queries.

## Tech Stack
- Python 3
- Pandas
- SQLite

## How to Run
1. Clone the repository:
   ```bash
   git clone https://github.com/Priscillagit/sales-etl-pipeline.git

## Install dependencies:

pip install -r requirements.txt


Run the ETL pipeline:

python etl.py

Example Output

After running, a sales.db file is created in data/warehouse/.

You can query it using:

SELECT * FROM sales_clean LIMIT 10;

Project Structure
sales-etl-pipeline/
│── data/
│   ├── raw/                # raw input data
│   ├── warehouse/          # SQLite database
│── etl.py                  # ETL pipeline script
│── README.md               # Documentation
│── requirements.txt        # Dependencies


---

**2️⃣ Create `requirements.txt`**  
Run:
```bash
pip freeze > requirements.txt

