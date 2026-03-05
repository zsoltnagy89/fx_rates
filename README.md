# 📊 FX Data Pipeline with Python, DuckDB, dbt, and Jupyter

A lightweight data pipeline to collect, store, transform, and explore historical foreign exchange (FX) data — with a focus on estimating currency exposure in financial analysis.

This project demonstrates a small modern analytics stack:

**Python → DuckDB → dbt → Jupyter Notebooks**

---

# 🚨 Problem Statement

Historical FX data for many **exotic currencies** is often:

- difficult to obtain
- incomplete
- hidden behind expensive data providers

For financial analysis and trading, this becomes problematic.

In my case, the **base trading currency is SEK (Swedish Krona)**.  
When trading assets across global markets (US, EU, and Nordic markets), this creates **currency exposure**.

To properly analyze portfolio performance and risk, it is important to estimate:

- historical FX rates
- exposure magnitude across currencies
- the SEK-adjusted value of trades over time

---

# 💡 Solution

This project implements a small data pipeline that:

1. Fetches **historical FX data** from an API  
2. Fetches **new daily FX updates**  
3. Stores the raw data as **JSON files**  
4. Loads the data into a **DuckDB database**  
5. Uses **dbt transformations** to produce normalized FX tables  
6. Allows analysis and visualization via **Jupyter notebooks**

The final output is a clean FX dataset:

📅 **Daily exchange rates vs SEK since 1999-01-01**

This enables estimating historical currency exposure even when high-quality datasets are unavailable.

---

# 🏗️ Architecture

```
FX API
│
▼
Python Fetch Scripts
│
▼
JSON Storage
│
▼
DuckDB Database
│
▼
dbt Transformations
│
▼
Clean FX Tables
│
▼
Jupyter Notebooks (analysis & visualization)
```

---

# 📂 Project Structure

```
project-root
│
├── databases/
│   └── duckdb database files
│
├── jsons/
│   ├── historical FX data
│   └── daily FX updates
│
├── notebooks/
│   └── exploratory analysis and visualizations
│
├── src/
│   ├── fetch_historical_fx_data.py
│   ├── fetch_new_fx_data.py
│   └── load_json_to_duckdb.py
│
├── dbt/
│   ├── venv_dbt/
│   └── currency_dbt/
│       └── dbt project
│
├── .env.example
│
├── requirements_src.txt
├── requirements_notebooks.txt
└── requirements_dbt.txt
```

---

# ⚙️ Environment Setup

Each major layer uses a **separate Python virtual environment**.

This avoids dependency conflicts — especially with **dbt and DuckDB**.

## Why separate environments?

Recent versions of **dbt-core (1.10+) use the Fusion engine**, which **does not support DuckDB**.

Therefore the project uses:

- **dbt-core 1.8**
- **Python 3.11**
- pinned DuckDB connectors

The other layers (Python scripts and notebooks) are flexible.

---

# 🔑 Environment Variables

Create a `.env` file based on:

```
.env.example
```

Add your API key and configuration values there.

---

# 🚀 Usage

## 1. Fetch FX Data

Clone the repository:

```bash
git clone <repo_url>
cd <repo>
```

Create a virtual environment for the Python scripts:

```bash
python -m venv venv_src
source venv_src/bin/activate
pip install -r requirements_src.txt
```

Fetch **historical FX data**:

```bash
python src/fetch_historical_fx_data.py
```

Fetch **latest FX data**:

```bash
python src/fetch_new_fx_data.py
```

Load JSON data into DuckDB:

```bash
python src/load_json_to_duckdb.py
```

---

# 📦 dbt Transformations

Install **Python 3.11**.

Create a dbt environment:

```bash
python3.11 -m venv venv_dbt
source venv_dbt/bin/activate
pip install -r requirements_dbt.txt
```

Navigate to the dbt project:

```bash
cd dbt/currency_dbt
```

Run dbt:

```bash
dbt run
```

If you have a global dbt installation using Fusion, run explicitly from the venv:

```bash
../venv_dbt/bin/dbt run
```

---

# 📓 Jupyter Notebooks

Create the notebook environment:

```bash
python -m venv venv_notebooks
source venv_notebooks/bin/activate
pip install -r requirements_notebooks.txt
```

Register the kernel:

```bash
python -m ipykernel install --user --name fx-notebooks
```

Start Jupyter:

```bash
jupyter notebook
```

Use the notebooks for:

- exploratory analysis
- quick prototyping
- visualizations

---

# 📈 Example Use Cases

This dataset enables analysis such as:

- estimating **historical currency exposure**
- converting portfolio values to **SEK**
- comparing **market returns vs FX-adjusted returns**
- analyzing **FX volatility**

---

# ⚠️ Limitations

- The pipeline is **user-triggered** (not orchestrated)
- API reliability depends on the external provider
- Historical data availability may vary by currency
- dbt version is pinned due to DuckDB compatibility

---

# 🔮 Possible Future Improvements

- add **Airflow / Prefect orchestration**
- automate **daily FX ingestion**
- add **data quality tests in dbt**
- build a **dashboard layer (Streamlit / Superset / Sigma)**
- migrate to **dbt Fusion once DuckDB support is available**

---

# 🛠️ Tech Stack

| Layer | Tool |
|------|------|
| Data Fetching | Python |
| Storage | DuckDB |
| Transformations | dbt |
| Analysis | Jupyter |
| Data Format | JSON |

---

# 📜 License

MIT License

---

# 👤 Author

Personal data engineering project focused on building lightweight analytics pipelines using modern data tooling.