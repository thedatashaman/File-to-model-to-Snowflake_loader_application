# Quick Start Guide

## Prerequisites

1. Python 3.11+ installed
2. Snowflake account (for loading data - optional for testing)
3. Sample data file (CSV, JSON, Parquet, or Excel)

## Installation Steps

1. **Install dependencies:**
```bash
pip install -r requirements.txt
```

2. **Configure Snowflake (optional):**
```bash
cp .env.example .env
# Edit .env with your Snowflake credentials
```

3. **Run the application:**
```bash
streamlit run streamlit_app.py
```

The app will open in your browser at `http://localhost:8501`

## Basic Workflow

### Step 1: Upload
- Click "Choose a file" and select your data file
- The app will auto-detect encoding and delimiter
- Click "Validate & Load File"

### Step 2: Review
- Review the data profiling results
- Check column statistics, candidate keys, and entity detection
- Identify any data quality issues

### Step 3: Model
- Click "Generate Data Model"
- Review the generated tables (FACT and DIM tables)
- Optionally generate Snowflake DDL

### Step 4: Split Files
- Click "Generate Split Files"
- Review the generated CSV files for each table
- Optionally run data quality checks

### Step 5: Load to Snowflake
- Enter your Snowflake connection details
- Click "Test Connection" to verify
- Click "Load to Snowflake" to start the load process

### Step 6: Logs
- View ingestion history
- Check table load status
- Review any errors

## Sample Data

You can test with any CSV file. Example structure:

```csv
transaction_id,customer_id,product_id,amount,quantity,transaction_date
1,101,201,99.99,2,2024-01-15
2,102,202,149.50,1,2024-01-16
```

## Troubleshooting

### File Upload Issues
- Ensure file is < 1GB for best performance
- Check file encoding (try UTF-8)
- Verify file format is supported

### Snowflake Connection Issues
- Verify account format: `account.region`
- Check network connectivity
- Ensure warehouse is running
- Verify user permissions

### Memory Issues
- Use smaller files for testing
- Close other applications
- Consider processing in chunks

## Next Steps

- Review the generated DDL in `app/output/model/snowflake_ddl.sql`
- Check the model summary in `app/output/model/model_summary.md`
- Visualize the ERD at https://mermaid.live/
