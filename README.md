# File-to-Model-to-Snowflake Loader

A production-grade Streamlit application that ingests user-uploaded data files, profiles them, generates normalized dimensional data models, materializes split table files, and loads everything into Snowflake with best practices.

## 🎯 Features

### 1. **File Upload & Validation**
- Supports multiple formats: CSV, JSON, JSONL, Parquet, Excel (xlsx)
- Handles large files (up to 1GB+) with chunked processing
- Auto-detects encoding, delimiter, and file structure
- Validates file integrity before processing

### 2. **Data Profiling**
- Automatic schema detection (columns, types, nullability)
- Comprehensive column profiling:
  - Row count, distinct count, null percentage
  - Min/max for numeric/date columns
  - Top N frequent values for categorical data
  - Outlier detection (IQR method)
  - Date parsing quality checks
- Candidate key detection (single and composite)
- Entity detection (dimensions, facts, IDs, dates)
- Grain detection (transaction, event, row-level)

### 3. **Data Modeling**
- Automatic model generation using best practices:
  - **Star Schema** for analytics-oriented data
  - **3NF** for transactional data
- Separates data into:
  - **FACT** tables (events/transactions with measures)
  - **DIM** tables (entities: customer, product, vendor, location, etc.)
- Surrogate keys for dimensions (`DIM_*_SK`)
- Natural keys retained as `*_NK`
- Automatic date dimension generation
- Metadata columns: `LOAD_TS`, `SOURCE_FILE_NAME`, `ROW_HASH`, `RECORD_SOURCE`
- Clustering key recommendations for large fact tables
- Generates:
  - Conceptual model (entity list + relationships)
  - Logical model (tables, PK/FK, SCD type recommendations)
  - Physical model (Snowflake DDL)
  - Mermaid ERD diagrams

### 4. **File Splitting**
- Creates separate CSV files per table:
  - `/output/dim_customer.csv`
  - `/output/dim_product.csv`
  - `/output/fact_transactions.csv`
  - etc.
- Deduplicates dimension rows
- Generates deterministic surrogate keys using SHA256
- Ensures FK columns in facts refer to dimension surrogate keys
- Supports nested JSON (flattens into child tables)

### 5. **Snowflake Loading**
- Creates database/schema if they don't exist
- Creates internal stages for file uploads
- Generates and executes Snowflake DDL
- Uploads files to stage
- Uses `COPY INTO` with error handling
- Validates row counts
- Supports both password and key-pair authentication
- Comprehensive audit logging:
  - `INGESTION_RUNS` table
  - `INGESTION_TABLE_STATUS` table
  - `INGESTION_ERRORS` table

### 6. **Data Quality Checks**
- Primary key uniqueness validation
- Foreign key referential integrity checks
- Null constraint validation
- Data type validation
- Comprehensive error reporting

## 📋 Requirements

- Python 3.11+
- Snowflake account (for loading data)
- 4GB+ RAM recommended for large files

## 🚀 Installation

1. **Clone the repository:**
```bash
git clone <repository-url>
cd ai_data_modeler
```

2. **Create a virtual environment:**
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. **Install dependencies:**
```bash
pip install -r requirements.txt
```

4. **Configure Snowflake (optional, for loading):**
```bash
cp .env.example .env
# Edit .env with your Snowflake credentials
```

## 🎮 Usage

1. **Start the Streamlit app:**
```bash
streamlit run streamlit_app.py
```

2. **Follow the workflow:**
   - **Step 1 - Upload**: Upload your data file
   - **Step 2 - Review**: Review profiling results and data preview
   - **Step 3 - Model**: Generate and review the data model
   - **Step 4 - Split Files**: Generate dimension and fact table files
   - **Step 5 - Load to Snowflake**: Configure connection and load data
   - **Step 6 - Logs**: View ingestion history and errors

## 📁 Project Structure

```
ai_data_modeler/
├── streamlit_app.py          # Main Streamlit application
├── app/
│   ├── pages/                # Streamlit pages
│   │   ├── 01_upload.py
│   │   ├── 02_review.py
│   │   ├── 03_model.py
│   │   ├── 04_split.py
│   │   ├── 05_load.py
│   │   └── 06_logs.py
│   ├── core/                 # Core modules
│   │   ├── utils.py          # File handling utilities
│   │   ├── profiling.py      # Data profiling
│   │   ├── modeling.py       # Data modeling
│   │   ├── splitting.py      # File splitting
│   │   ├── snowflake_loader.py  # Snowflake operations
│   │   └── dq_checks.py      # Data quality checks
│   ├── output/               # Generated files
│   │   ├── model/            # Model artifacts (DDL, ERD, etc.)
│   │   └── uploads/          # Temporary uploaded files
│   └── logs/                 # Application logs
├── requirements.txt
├── .env.example
└── README.md
```

## 🔧 Configuration

### Snowflake Connection

The app supports two authentication methods:

1. **Password Authentication:**
   - Set `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`

2. **Key-Pair Authentication:**
   - Set `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PRIVATE_KEY`
   - Private key can be a file path or PEM content

### Environment Variables

```bash
SNOWFLAKE_ACCOUNT=your_account.us-east-1
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password  # OR
SNOWFLAKE_PRIVATE_KEY=/path/to/key.pem
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=MY_DATABASE
SNOWFLAKE_SCHEMA=MY_SCHEMA
SNOWFLAKE_ROLE=ACCOUNTADMIN
```

## 📊 Data Model Outputs

The app generates several artifacts:

1. **Model Summary** (`app/output/model/model_summary.md`)
   - Overview of tables, relationships, and metadata

2. **Snowflake DDL** (`app/output/model/snowflake_ddl.sql`)
   - Complete CREATE TABLE statements
   - Primary key constraints
   - Clustering keys

3. **Mermaid ERD** (`app/output/model/erd_mermaid.md`)
   - Entity relationship diagram code
   - Can be visualized at https://mermaid.live/

4. **Split Table Files** (`app/output/*.csv`)
   - One CSV file per table
   - Ready for Snowflake loading

## 🛠️ Best Practices Implemented

### Data Modeling
- Star schema for analytics workloads
- Surrogate keys for dimensions
- Natural keys preserved
- Date dimension for time-based analysis
- Metadata columns for lineage tracking

### Snowflake Loading
- Internal stages for file uploads
- COPY INTO with error handling
- Proper data types (TEXT, NUMBER, FLOAT, TIMESTAMP_NTZ)
- Clustering keys for performance
- Comprehensive audit logging

### Data Quality
- Primary key uniqueness checks
- Foreign key referential integrity
- Null constraint validation
- Type validation

## 🐛 Troubleshooting

### Large File Issues
- The app uses chunked processing for files >50MB
- If memory issues occur, try processing smaller files or increase system RAM

### Snowflake Connection Issues
- Verify account identifier format: `account.region`
- Check network connectivity
- Ensure user has proper permissions
- Verify warehouse is running

### Encoding Issues
- The app auto-detects encoding, but you can manually specify
- Common encodings: UTF-8, Latin-1, ISO-8859-1

## 📝 License

This project is provided as-is for educational and commercial use.

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📧 Support

For issues and questions, please open an issue in the repository.

---

**Built with ❤️ using Streamlit, Pandas, and Snowflake**
