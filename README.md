Snapshot Results Comparison Tool

A lightweight, web-based application for comparing two results data snapshots (typically report results). It allows users to:
- Compare two snapshots side by side by aggregated business dimensions
- Dynamically configurable group-by and aggregation columns
- Drill down to detailed records causing differences
- Supports CSV or database backends (via SQLAlchemy)
- Configurable to adapt to different table structures and column mappings
 
NOTE: This project is intended for demonstration and internal analysis purposes.

Tech Stack
- **Backend**: Python, Flask, SQLAlchemy, Pandas
- **Frontend**: HTML, JavaScript, DataTables.js
- **Data**: CSV or any SQLAlchemy-compatible database (e.g., SQLite, PostgreSQL)

Setup Instructions
1. **Clone the repository**
git clone https://github.com/yourusername/snapshot-comparison-tool.git
cd snapshot-comparison-tool

2. Install dependencies
pip install gunicorn Flask pandas SQLAlchemy Jinja2 sqlmodel-repository

4. Run the app
gunicorn --bind 0.0.0.0:$PORT app:app
   


