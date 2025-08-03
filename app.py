"""
Author: Low ChungKiat
Date: 01-Aug-2025
For personal project demostration only

This application is designed to compare two snapshots of a results table. It supports:
* Aggregated results comparison across configurable grouping and value columns
* Drill-down to view the underlying row-level data contributing to the differences

Key features include:
* Requires a snapshot identifier (e.g., report_date) and a unique record identifier (e.g., reference_id) for comparison
* Supports both CSV file input and database connections as interchangeable data sources
* Designed for flexibility: the table structure, group-by columns, and aggregation metrics are fully configurable via external settings

The tool is built to accommodate varying data schemas and allows users to customize how comparisons are made without modifying the application logic.
"""

import datetime
from flask import Flask, render_template, request
from sqlalchemy import text
import pandas as pd
from models import RegulatoryReport
from flask import Flask

from config_loader import ConfigLoader
from SQLRepository import SQLRepository
from data_factory import DataFactory

#Constants
CONFIG_FILE="config.ini"  
SQL_FOLDER="sql"
REQUIRED_SQL_TEMPLATES=["check_details_query.sql", "compare_results_query.sql","select_distinct_snapshot.sql"]

# ---- Initialization before Flask ----
def initialize_app_context():
    global config_loader, engine, app_sql_templates, snapshot_options
    global scfg_column, scfg_unique_reference_column
    global scfg_check_details_domain_columns, scfg_check_details_value_columns
    global groupby_columns_options, aggregate_columns_options

    #1. Load all config and validate, populate into variables
    config_loader = ConfigLoader(CONFIG_FILE)
    
    #2. Load all SQL queries, put them into collections
    sql_repo = SQLRepository(SQL_FOLDER)
    app_sql_templates={}
    for required_sql_template in REQUIRED_SQL_TEMPLATES:
        app_sql_templates[required_sql_template] = sql_repo.get(required_sql_template)

    #3 Initialize Connection, this can be either data file or database
    data_source = DataFactory.get_data_source(config_loader)
    engine = data_source.get_engine()
    
    #4 Load static config values
    scfg_column=config_loader.get("app","snapshot_column")
    scfg_unique_reference_column=config_loader.get("app","snapshot_unique_reference")
    scfg_check_details_domain_columns=config_loader.get("app","check_details_domain_columns")
    scfg_check_details_value_columns=config_loader.get("app","check_details_value_columns").split(",")
    snapshot_options = None
    with engine.connect() as conn:
        snapshot_result = conn.execute(text(app_sql_templates["select_distinct_snapshot.sql"]))
        snapshot_options = [row[0] for row in snapshot_result.fetchall()]
            
    groupby_columns_options =   config_loader.get("app","group_by_columns").split(',')
    aggregate_columns_options = config_loader.get("app","aggregate_columns").split(',')


def get_dynamic_compare_results_sql(selected_group_by_cols, selected_aggregate_cols, ):
        
        group_by_select_clause = ', '.join(selected_group_by_cols)
        
        table1_total_clause = '1 as total_1, 0 as total_2'
        table2_total_clause = '0 as total_1, 1 as total_2'
        
        table1_aggregate_columns1_clause =  ', '.join([f"{col} AS {col}_1" for col in selected_aggregate_cols])
        table1_aggregate_columns2_clause =  ', '.join([f"0 AS {col}_2" for col in selected_aggregate_cols])
        
        table2_aggregate_columns1_clause =  ', '.join([f"0 AS {col}_1" for col in selected_aggregate_cols])
        table2_aggregate_columns2_clause =  ', '.join([f"{col} AS {col}_2" for col in selected_aggregate_cols])
        
        group_by_total_clause = 'SUM(TOTAL_1) AS TOTAL_1, SUM(TOTAL_1) AS TOTAL_2'
        group_by_aggregate_columns_clause = ', '.join([f"SUM({col}_1) AS {col}_1, SUM({col}_2) AS {col}_2" for col in selected_aggregate_cols])
        
        main_total_diff_clause = 'TOTAL_1, TOTAL_2, TOTAL_1 - TOTAL_2 AS TOTAL_DIFF'
        main_aggregate_diff_clause = ', '.join([f"{col}_1, {col}_2, {col}_1 - {col}_2 AS {col}_DIFF" for col in selected_aggregate_cols])
        
        
        print(table1_aggregate_columns1_clause)
        print(table1_aggregate_columns2_clause)
        print(table2_aggregate_columns1_clause)
        print(table2_aggregate_columns2_clause)
        
        print(group_by_total_clause)
        print(group_by_aggregate_columns_clause)
        
        print(main_total_diff_clause)
        print(main_aggregate_diff_clause)
        
        compare_sql_template=app_sql_templates["compare_results_query.sql"]
        return compare_sql_template.format(
                group_by_select_clause=group_by_select_clause,
                group_by_total_clause=group_by_total_clause,
                main_total_diff_clause=main_total_diff_clause,
                main_aggregate_diff_clause=main_aggregate_diff_clause,
                group_by_aggregate_columns_clause=group_by_aggregate_columns_clause,
                table1_total_clause=table1_total_clause,
                table1_aggregate_columns1_clause=table1_aggregate_columns1_clause,
                table1_aggregate_columns2_clause=table1_aggregate_columns2_clause,
                table2_total_clause=table2_total_clause,
                table2_aggregate_columns1_clause=table2_aggregate_columns1_clause,
                table2_aggregate_columns2_clause=table2_aggregate_columns2_clause,
                snapshot_column=scfg_column
        ) 
    
def get_dynamic_check_details_sql(selectedrows, check_details_where_clause):
            
            check_details_value_columns_lead_lag=', '.join([f"LAG({col}) OVER (PARTITION BY {scfg_unique_reference_column} ORDER BY  {scfg_unique_reference_column}, TABLE_NO ) AS LAG_{col},\n LEAD({col}) OVER (PARTITION BY {scfg_unique_reference_column} ORDER BY  {scfg_unique_reference_column}, TABLE_NO ) AS LEAD_{col} \n" for col in scfg_check_details_value_columns])
            check_details_value_columns_diff=', '.join([f"{col}, \n {col} - (CASE WHEN TABLE_NO =1 THEN LEAD_{col} ELSE LAG_{col} END) AS {col}_DIFF \n  " for col in scfg_check_details_value_columns])
            
            check_details_sql_template=app_sql_templates["check_details_query.sql"]
            
            return check_details_sql_template.format(
                snapshot_column=scfg_column,
                UNIQUE_REFERENCE_ID=scfg_unique_reference_column,
                check_details_value_columns_diff=check_details_value_columns_diff,
                check_details_value_columns_lead_lag=check_details_value_columns_lead_lag,
                check_details_domain_columns=scfg_check_details_domain_columns,
                check_details_value_columns=", ".join(scfg_check_details_value_columns),
                check_details_where_clause=check_details_where_clause                
            )



#1. Initialize config, SQL, datasource, global variables
initialize_app_context()

#2. Create Flask App
app = Flask(__name__) 

#3. Define Routes, in this demostration it's only using single html page
@app.route('/', methods=['GET', 'POST'])
def home():
    
    selected_snapshot_1 = None
    selected_snapshot_2 = None    
    selected_group_by_cols = []
    selected_aggregate_cols = []   
    
    compare_sql_filled = None
    compare_df = None
    group_by_cols  = None
    agg_cols  = None
    
    check_details_sql_filled = None
    checkdetails_df = None
    
    check_details_where_clause = None
     
    if request.method == 'POST':
        selected_snapshot_1 = request.form['snapshop_1_selection']
        selected_snapshot_2 = request.form['snapshop_2_selection']
        selected_group_by_cols = request.form['selectedGroupByColumns'].split(", ") if request.form['selectedGroupByColumns'] else []
        selected_aggregate_cols = request.form['selectedAggregateColumns'].split(", ") if request.form['selectedGroupByColumns'] else []
        
        print("selected_group_by_cols", selected_group_by_cols);
        print("selected_aggregate_cols", selected_aggregate_cols);

        #Based on the submitted form data are ready, generate dynamic comparison query for 1st level comparison results table
        compare_sql_filled = get_dynamic_compare_results_sql(selected_group_by_cols, selected_aggregate_cols)
 
        print(compare_sql_filled)
        compare_df = pd.read_sql(compare_sql_filled, engine, params=(selected_snapshot_1, selected_snapshot_2) )

        selectedrows = request.form.getlist("selectedrows")
        print("selectedrows", selectedrows)
        if len(selectedrows) > 0:
            #Based on the selected rows from 1st level comparison tables, generate filtered conditions in where clause
            for index, selectedrow in enumerate(selectedrows) :
                row_columns = selectedrow.split(",")
                row_columns = row_columns[0:len(selected_group_by_cols)]
                
                if index == 0:
                    check_details_where_clause = '(' + ' AND '.join(f"{k}='{v}'" for k, v in zip(selected_group_by_cols, row_columns))  + ')'
                else:
                    check_details_where_clause = check_details_where_clause + '\n OR  (' + ' AND '.join(f"{k}='{v}'" for k, v in zip(selected_group_by_cols, row_columns)) + ' ) '
         
            print("check_details_where_clause", check_details_where_clause) 
            
            
            #Based on the selected rows from 1st level comparison tables, generate dynamic view details query for 2nd level results table
            check_details_sql_filled = get_dynamic_check_details_sql(selectedrows, check_details_where_clause)
            
            print(check_details_sql_filled)
            checkdetails_df = pd.read_sql(check_details_sql_filled, engine, params=(selected_snapshot_1, selected_snapshot_2) )         
   

    return render_template('form.html', 
                    snapshot_options=snapshot_options,
                    groupby_columns_options=groupby_columns_options,
                    aggregate_columns_options=aggregate_columns_options,
                    selected_snapshot_1=selected_snapshot_1, 
                    selected_snapshot_2=selected_snapshot_2, 
                    selected_group_by_cols=selected_group_by_cols,
                    selected_aggregate_cols=selected_aggregate_cols,
                    compare_sql=compare_sql_filled,
                    comparison_result=compare_df,
                    check_details_where_clause=check_details_where_clause,
                    checkdetails_sql=check_details_sql_filled,
                    checkdetails_result=checkdetails_df)

#4. Run the Flask
if __name__ == '__main__':
    app.run(debug=True)
