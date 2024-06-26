import streamlit as st
import snowflake.connector
import pandas as pd
import plotly.graph_objects as go

# Function to connect to Snowflake
def connect_snowflake():
    conn = st.connection("snowflake")
    return conn

# Function to fetch data from Snowflake table
def fetch_SF_data():
    conn = connect_snowflake()
    cursor = conn.cursor()

    # Fetch data from VW_PLOT_BusinessLogic view
    cursor.execute("SELECT RuleName, RowCount FROM VW_PLOT_BusinessLogic")
    data = cursor.fetchall()
    
    #conn.close()
    return data
    
    # # First business rule: Count of duplicate records based on INVOICE_ID and INVOICE_DATE
    # cursor.execute("SELECT COUNT(*) FROM (SELECT INVOICE_ID, INVOICE_DATE FROM VW_INVOICES GROUP BY INVOICE_ID, INVOICE_DATE HAVING COUNT(*) > 1)")
    # duplicate_records = cursor.fetchall()
    # total_duplicate_records = sum([record[0] for record in duplicate_records])
    
    # # Second business rule: Count of records where Invoice total not equals po total
    # cursor.execute("SELECT COUNT(*) FROM VW_INVOICES WHERE PO_TOTAL IS NULL OR PURCHASE_ORDER IS NULL")
    # missing_po = cursor.fetchone()[0]
       
    # # Third business rule: Count of records where PO is mismatch
    # cursor.execute("SELECT COUNT(*) FROM VW_INVOICES WHERE INVOICE_TOTAL <> PO_TOTAL")
    # po_mismatch = cursor.fetchone()[0]
    
    # # Fourth business rule: Count of records where TOTAL_TAX is greater than 10% of SUB_TOTAL or less than 8% of SUB_TOTAL
    # cursor.execute("""
    #     SELECT COUNT(*) FROM VW_INVOICES 
    #     WHERE 
    #     TRY_CAST(TRIM(TOTAL_TAX) AS FLOAT) > TRY_CAST(TRIM(SUB_TOTAL) AS FLOAT) * 0.15
    #     OR 
    #     TRY_CAST(TRIM(TOTAL_TAX) AS FLOAT) < TRY_CAST(TRIM(SUB_TOTAL) AS FLOAT) * 0.08
    # """)
    # total_tax_out_of_range_records = cursor.fetchone()[0]
    
    # # Total number of records in the table
    # cursor.execute("SELECT COUNT(*) FROM VW_INVOICES")
    # total_records = cursor.fetchone()[0]
    
    # # conn.close()
    # return total_duplicate_records, missing_po, po_mismatch, total_tax_out_of_range_records, total_records



    
      


