# Sales ETL Pipeline

A simple **Extract, Transform, Load (ETL)** project built with Python and Pandas.  
The pipeline reads raw sales data from a CSV file, cleans and transforms it,  
and loads it into a SQLite database for easy querying and analysis.

---

## 📂 Project Structure
sales-etl-pipeline/
│
├── data/
│ ├── raw/ # Raw input CSV file
│ └── warehouse/ # SQLite database output
│
├── etl.py # Main ETL script
├── README.md # Project documentation
└── requirements.txt # Python dependencies

---

## 🚀 Features
- **Extract**: Reads raw sales CSV data.
- **Transform**: Cleans, validates, and enriches data.
- **Load**: Saves clean data into a SQLite database.
- Automatic **index creation** for faster queries.
- Handles missing or invalid data gracefully.

---

## 🛠 Installation & Usage

1. **Clone the repository**
```bash
git clone https://github.com/Priscillagit/sales-etl-pipeline.git
cd sales-etl-pipeline
Install dependencies

pip install -r requirements.txt
Run the ETL pipeline
python etl.py
Query the database
sqlite3 data/warehouse/sales.db

SELECT * FROM sales_clean LIMIT 5;
📊 Example Output
Cleaned sales data with:
unit_price (Sales ÷ Quantity)
profit_margin (%)
year & month extracted from date

📌 Requirements
Python 3.8+
Pandas
SQLite3 (comes pre-installed with Python)

📜 License
This project is licensed under the MIT License.

Author: Priscilla

