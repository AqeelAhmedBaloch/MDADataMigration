import streamlit as st
import os
import pandas as pd
import pyodbc
from dbfread import DBF, FieldParser

class CustomFieldParser(FieldParser):
    def parseL(self, field, data):
        try:
            return super().parseL(field, data)
        except:
            return None
    def parseD(self, field, data):
        try:
            return super().parseD(field, data)
        except:
            return None
    def parseN(self, field, data):
        try:
            return super().parseN(field, data)
        except:
            return None

st.set_page_config(page_title="MDA DBF to SQL Migrator", layout="centered")
st.title("📁 MDA DBF to SQL Server Full Migration")

server = 'localhost'
database = 'MdaDB'
conn_str = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;'

uploaded_files = st.file_uploader("Upload your .DBF and .FPT files", type=["dbf", "fpt"], accept_multiple_files=True)

if uploaded_files:
    os.makedirs("temp", exist_ok=True)
    for file in uploaded_files:
        with open(f"temp/{file.name}", "wb") as f:
            f.write(file.getbuffer())

    dbf_file = next((f for f in uploaded_files if f.name.lower().endswith(".dbf")), None)

    if dbf_file:
        dbf_path = os.path.join("temp", dbf_file.name)

        try:
            table = DBF(dbf_path, encoding='latin1', parserclass=CustomFieldParser, ignore_missing_memofile=True)
            df = pd.DataFrame(iter(table))
            st.success(f"✅ {len(df)} Records Loaded")
            st.dataframe(df, use_container_width=True)

            table_name = os.path.splitext(dbf_file.name)[0]  # Use file name as table name

            def get_sql_type(series):
                non_null = series.dropna()
                sample = non_null.iloc[0] if not non_null.empty else ""

                if series.map(type).nunique() == 1 and isinstance(sample, bool):
                    return "BIT"
                elif pd.api.types.is_integer_dtype(series):
                    return "INT"
                elif pd.api.types.is_float_dtype(series):
                    return "FLOAT"
                elif pd.api.types.is_datetime64_any_dtype(series):
                    return "DATETIME"
                else:
                    return "NVARCHAR(MAX)"

            if st.button("📦 Create Table and Insert Data in SQL Server"):
                try:
                    conn = pyodbc.connect(conn_str)
                    cursor = conn.cursor()

                    col_defs = []
                    sql_types = {}
                    for col in df.columns:
                        sql_type = get_sql_type(df[col])
                        sql_types[col] = sql_type
                        col_defs.append(f"[{col}] {sql_type}")

                    create_sql = f"""
                    IF OBJECT_ID('{table_name}', 'U') IS NOT NULL
                        DROP TABLE {table_name};

                    CREATE TABLE {table_name} (
                        {', '.join(col_defs)}
                    )
                    """
                    cursor.execute(create_sql)

                    for index, row in df.iterrows():
                        clean_row = [None if isinstance(val, bytes) else val for val in row]
                        values = []
                        for col, val in zip(df.columns, clean_row):
                            sql_type = sql_types[col]

                            if pd.isna(val) or str(val).lower() == "nan":
                                if sql_type in ["INT", "FLOAT"]:
                                    values.append("0")
                                else:
                                    values.append("NULL")
                            elif sql_type in ["INT", "FLOAT"]:
                                try:
                                    values.append(str(float(val)))
                                except:
                                    values.append("0")
                            elif sql_type == "BIT":
                                values.append("1" if str(val).lower() in ["true", "1", "yes"] else "0")
                            elif sql_type == "DATETIME":
                                try:
                                    values.append(f"'{pd.to_datetime(val)}'")
                                except:
                                    values.append("NULL")
                            else:
                                values.append(f"'{str(val).replace("'", "''")}'")

                        insert_sql = f"INSERT INTO {table_name} VALUES ({', '.join(values)})"
                        cursor.execute(insert_sql)

                    conn.commit()
                    st.success(f"✅ Table `{table_name}` created and {len(df)} records inserted successfully.")

                except Exception as e:
                    st.error(f"❌ Error migrating data to SQL Server: {e}")

        except Exception as e:
            st.error(f"❌ Error reading DBF file: {e}")
