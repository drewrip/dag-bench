import duckdb
import os
import xml.etree.ElementTree as ET
from datetime import datetime

con = duckdb.connect('tpcdi.duckdb')

def load_csv(table_name, pattern, sep='|', columns=None, batchid=None):
    print(f"Loading {table_name} from {pattern}...")
    if columns:
        col_def = ", ".join([f"{name} {type}" for name, type in columns.items()])
        con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({col_def})")
    
    # DuckDB's read_csv can handle globs and return filename
    # We use union_by_name if schemas might slightly differ or to handle multiple files
    filename_col = ", filename" if batchid is True else ""
    
    # If columns is None, we let DuckDB infer
    if batchid is True:
        # Create a temp table first
        con.execute(f"CREATE TEMP TABLE tmp_{table_name} AS SELECT * FROM read_csv('{pattern}', sep='{sep}', header=False, filename=True)")
        # Extract batchid: sf10/BatchN/... -> N
        con.execute(f"""
            INSERT INTO {table_name} 
            SELECT * EXCLUDE(filename), 
            CAST(regexp_extract(filename, 'Batch([0-9])', 1) AS INT) as batchid 
            FROM tmp_{table_name}
        """)
        con.execute(f"DROP TABLE tmp_{table_name}")
    else:
        con.execute(f"INSERT INTO {table_name} SELECT * FROM read_csv('{pattern}', sep='{sep}', header=False)")

# Define schemas based on models
schemas = {
    "raw_batchdate": {"batchdate": "DATE", "batchid": "INT"},
    "raw_date": {
        "sk_dateid": "BIGINT", "datevalue": "DATE", "datedesc": "VARCHAR",
        "calendaryearid": "INT", "calendaryeardesc": "VARCHAR", "calendarqtrid": "INT", "calendarqtrdesc": "VARCHAR",
        "calendarmonthid": "INT", "calendarmonthdesc": "VARCHAR", "calendarweekid": "INT", "calendarweekdesc": "VARCHAR",
        "dayofweeknum": "INT", "dayofweekdesc": "VARCHAR", "fiscalyearid": "INT", "fiscalyeardesc": "VARCHAR",
        "fiscalqtrid": "INT", "fiscalqtrdesc": "VARCHAR", "holidayflag": "BOOLEAN"
    },
    "raw_time": {
        "sk_timeid": "BIGINT", "timevalue": "VARCHAR", "hourid": "INT", "hourdesc": "VARCHAR",
        "minuteid": "INT", "minutedesc": "VARCHAR", "secondid": "INT", "seconddesc": "VARCHAR",
        "markethoursflag": "BOOLEAN", "officehoursflag": "BOOLEAN"
    },
    "raw_industry": {"in_id": "VARCHAR", "in_name": "VARCHAR", "in_sc_id": "VARCHAR"},
    "raw_statustype": {"st_id": "VARCHAR", "st_name": "VARCHAR"},
    "raw_taxrate": {"tx_id": "VARCHAR", "tx_name": "VARCHAR", "tx_rate": "FLOAT"},
    "raw_tradetype": {"tt_id": "VARCHAR", "tt_name": "VARCHAR", "tt_is_sell": "INT", "tt_is_mrkt": "INT"},
    "raw_hr": {
        "sk_brokerid": "BIGINT", "managerid": "BIGINT", "firstname": "VARCHAR", "lastname": "VARCHAR",
        "middleinitial": "VARCHAR", "jobcode": "INT", "branch": "VARCHAR", "office": "VARCHAR", "phone": "VARCHAR"
    },
    "raw_prospect": {
        "agencyid": "VARCHAR", "lastname": "VARCHAR", "firstname": "VARCHAR", "middleinitial": "VARCHAR",
        "gender": "VARCHAR", "addressline1": "VARCHAR", "addressline2": "VARCHAR", "postalcode": "VARCHAR",
        "city": "VARCHAR", "state": "VARCHAR", "country": "VARCHAR", "phone": "VARCHAR", "income": "VARCHAR",
        "numbercars": "INT", "numberchildren": "INT", "maritalstatus": "VARCHAR", "age": "INT", "creditrating": "INT",
        "ownorrentflag": "VARCHAR", "employer": "VARCHAR", "numbercreditcards": "INT", "networth": "INT", "batchid": "INT"
    },
    "raw_account": {
        "cdc_flag": "VARCHAR", "cdc_dsn": "BIGINT", "accountid": "BIGINT", "brokerid": "BIGINT", "customerid": "BIGINT",
        "accountdesc": "VARCHAR", "taxstatus": "TINYINT", "status": "VARCHAR", "batchid": "INT"
    },
    "raw_customer": {
        "cdc_flag": "VARCHAR", "cdc_dsn": "BIGINT", "customerid": "BIGINT", "taxid": "VARCHAR", "status": "VARCHAR",
        "lastname": "VARCHAR", "firstname": "VARCHAR", "middleinitial": "VARCHAR", "gender": "VARCHAR", "tier": "TINYINT",
        "dob": "DATE", "addressline1": "VARCHAR", "addressline2": "VARCHAR", "postalcode": "VARCHAR", "city": "VARCHAR",
        "stateprov": "VARCHAR", "country": "VARCHAR", "c_ctry_1": "VARCHAR", "c_area_1": "VARCHAR", "c_local_1": "VARCHAR",
        "c_ext_1": "VARCHAR", "c_ctry_2": "VARCHAR", "c_area_2": "VARCHAR", "c_local_2": "VARCHAR", "c_ext_2": "VARCHAR",
        "c_ctry_3": "VARCHAR", "c_area_3": "VARCHAR", "c_local_3": "VARCHAR", "c_ext_3": "VARCHAR", "email1": "VARCHAR",
        "email2": "VARCHAR", "lcl_tx_id": "VARCHAR", "nat_tx_id": "VARCHAR", "batchid": "INT"
    },
    "raw_trade": {
        "cdc_flag": "VARCHAR", "cdc_dsn": "BIGINT", "tradeid": "BIGINT", "t_dts": "TIMESTAMP", "status": "VARCHAR",
        "t_tt_id": "VARCHAR", "cashflag": "TINYINT", "t_s_symb": "VARCHAR", "quantity": "INT", "bidprice": "DOUBLE",
        "t_ca_id": "BIGINT", "executedby": "VARCHAR", "tradeprice": "DOUBLE", "fee": "DOUBLE", "commission": "DOUBLE",
        "tax": "DOUBLE", "batchid": "INT"
    },
    "raw_tradehistory": {"tradeid": "BIGINT", "th_dts": "TIMESTAMP", "status": "VARCHAR"},
    "raw_cashtransaction": {
        "accountid": "BIGINT", "ct_dts": "TIMESTAMP", "ct_amt": "DOUBLE", "ct_name": "VARCHAR", "batchid": "INT"
    },
    "raw_holdinghistory": {
        "hh_h_t_id": "BIGINT", "hh_t_id": "BIGINT", "hh_before_qty": "INT", "hh_after_qty": "INT", "batchid": "INT"
    },
    "raw_dailymarket": {
        "dm_date": "DATE", "dm_s_symb": "VARCHAR", "dm_close": "DOUBLE", "dm_high": "DOUBLE", "dm_low": "DOUBLE",
        "dm_vol": "INT", "batchid": "INT"
    },
    "raw_watchhistory": {
        "customerid": "BIGINT", "symbol": "VARCHAR", "w_dts": "TIMESTAMP", "w_action": "VARCHAR", "batchid": "INT"
    }
}

# Initialize tables
for table, cols in schemas.items():
    col_def = ", ".join([f"{name} {type}" for name, type in cols.items()])
    con.execute(f"CREATE OR REPLACE TABLE {table} ({col_def})")

# Load data
load_csv("raw_batchdate", "sf10/Batch*/BatchDate.txt", batchid=True)
load_csv("raw_date", "sf10/Batch1/Date.txt")
load_csv("raw_time", "sf10/Batch1/Time.txt")
load_csv("raw_industry", "sf10/Batch1/Industry.txt")
load_csv("raw_statustype", "sf10/Batch1/StatusType.txt")
load_csv("raw_taxrate", "sf10/Batch1/TaxRate.txt")
load_csv("raw_tradetype", "sf10/Batch1/TradeType.txt")
load_csv("raw_hr", "sf10/Batch1/HR.csv", sep=',')
load_csv("raw_prospect", "sf10/Batch*/Prospect.csv", sep=',', batchid=True)

# Incremental files (Batch2, Batch3)
load_csv("raw_account", "sf10/Batch[23]/Account.txt", batchid=True)
load_csv("raw_customer", "sf10/Batch[23]/Customer.txt", batchid=True)

# Trade and others
print("Checking Batch1/Trade.txt format...")
print(con.execute("SELECT * FROM read_csv('sf10/Batch1/Trade.txt', sep='|', header=False) LIMIT 5").fetchall())

con.execute("CREATE TEMP TABLE tmp_trade1 AS SELECT * FROM read_csv('sf10/Batch1/Trade.txt', sep='|', header=False)")
con.execute("""
    INSERT INTO raw_trade (tradeid, t_dts, status, t_tt_id, cashflag, t_s_symb, quantity, bidprice, t_ca_id, executedby, tradeprice, fee, commission, tax, batchid)
    SELECT column00, column01, column02, column03, column04, column05, column06, column07, column08, column09, column10, column11, column12, column13, 1
    FROM tmp_trade1
""")

con.execute("CREATE TEMP TABLE tmp_trade23 AS SELECT *, filename FROM read_csv('sf10/Batch[23]/Trade.txt', sep='|', header=False, filename=True)")
con.execute("""
    INSERT INTO raw_trade
    SELECT column00, column01, column02, column03, column04, column05, column06, column07, column08, column09, column10, column11, column12, column13, column14, column15,
    CAST(regexp_extract(filename, 'Batch([0-9])', 1) AS INT)
    FROM tmp_trade23
""")

load_csv("raw_tradehistory", "sf10/Batch1/TradeHistory.txt")

# CashTransaction
con.execute("CREATE TEMP TABLE tmp_ct1 AS SELECT * FROM read_csv('sf10/Batch1/CashTransaction.txt', sep='|', header=False)")
con.execute("INSERT INTO raw_cashtransaction SELECT column0, column1, column2, column3, 1 FROM tmp_ct1")
con.execute("CREATE TEMP TABLE tmp_ct23 AS SELECT *, filename FROM read_csv('sf10/Batch[23]/CashTransaction.txt', sep='|', header=False, filename=True)")
con.execute("INSERT INTO raw_cashtransaction SELECT column2, column3, column4, column5, CAST(regexp_extract(filename, 'Batch([0-9])', 1) AS INT) FROM tmp_ct23")

# HoldingHistory
con.execute("CREATE TEMP TABLE tmp_hh1 AS SELECT * FROM read_csv('sf10/Batch1/HoldingHistory.txt', sep='|', header=False)")
con.execute("INSERT INTO raw_holdinghistory SELECT column0, column1, column2, column3, 1 FROM tmp_hh1")
con.execute("CREATE TEMP TABLE tmp_hh23 AS SELECT *, filename FROM read_csv('sf10/Batch[23]/HoldingHistory.txt', sep='|', header=False, filename=True)")
con.execute("INSERT INTO raw_holdinghistory SELECT column2, column3, column4, column5, CAST(regexp_extract(filename, 'Batch([0-9])', 1) AS INT) FROM tmp_hh23")

# DailyMarket
con.execute("CREATE TEMP TABLE tmp_dm1 AS SELECT * FROM read_csv('sf10/Batch1/DailyMarket.txt', sep='|', header=False)")
con.execute("INSERT INTO raw_dailymarket SELECT column0, column1, column2, column3, column4, column5, 1 FROM tmp_dm1")
con.execute("CREATE TEMP TABLE tmp_dm23 AS SELECT *, filename FROM read_csv('sf10/Batch[23]/DailyMarket.txt', sep='|', header=False, filename=True)")
con.execute("INSERT INTO raw_dailymarket SELECT column2, column3, column4, column5, column6, column7, CAST(regexp_extract(filename, 'Batch([0-9])', 1) AS INT) FROM tmp_dm23")

# WatchHistory
con.execute("CREATE TEMP TABLE tmp_wh1 AS SELECT * FROM read_csv('sf10/Batch1/WatchHistory.txt', sep='|', header=False)")
con.execute("INSERT INTO raw_watchhistory SELECT column0, column1, column2, column3, 1 FROM tmp_wh1")
con.execute("CREATE TEMP TABLE tmp_wh23 AS SELECT *, filename FROM read_csv('sf10/Batch[23]/WatchHistory.txt', sep='|', header=False, filename=True)")
con.execute("INSERT INTO raw_watchhistory SELECT column2, column3, column4, column5, CAST(regexp_extract(filename, 'Batch([0-9])', 1) AS INT) FROM tmp_wh23")

# FinWire (Fixed Width)
print("Loading FinWire...")
con.execute("CREATE TABLE raw_finwire (line VARCHAR)")
con.execute("INSERT INTO raw_finwire SELECT * FROM read_csv('sf10/Batch1/FINWIRE[0-9][0-9][0-9][0-9]Q[1-4]', sep='\\b', header=False)") # \\b is a hack to read whole line

# CustomerMgmt.xml Parsing
print("Parsing CustomerMgmt.xml...")
def parse_customer_mgmt(file_path):
    tree = ET.parse(file_path)
    root = tree.getroot()
    data = []
    
    for action in root.findall('.//Action'):
        action_type = action.get('ActionType')
        action_ts = action.get('ActionTS')
        
        row = {
            'ActionType': action_type,
            'update_ts': action_ts,
            'customerid': None, 'taxid': None, 'status': None, 'lastname': None, 'firstname': None,
            'middleinitial': None, 'gender': None, 'tier': None, 'dob': None, 'addressline1': None,
            'addressline2': None, 'postalcode': None, 'city': None, 'stateprov': None, 'country': None,
            'phone1': None, 'phone2': None, 'phone3': None, 'email1': None, 'email2': None,
            'lcl_tx_id': None, 'nat_tx_id': None, 'accountid': None, 'accountdesc': None,
            'taxstatus': None, 'brokerid': None
        }
        
        cust = action.find('Customer')
        if cust is not None:
            row['customerid'] = cust.get('C_ID')
            row['taxid'] = cust.get('C_TAX_ID')
            row['tier'] = cust.get('C_TIER')
            
            name = cust.find('Name')
            if name is not None:
                row['lastname'] = name.findtext('C_L_NAME')
                row['firstname'] = name.findtext('C_F_NAME')
                row['middleinitial'] = name.findtext('C_M_NAME')
            
            row['gender'] = cust.findtext('C_GNDR')
            row['dob'] = cust.findtext('C_DOB')
            
            addr = cust.find('Address')
            if addr is not None:
                row['addressline1'] = addr.findtext('C_ADLINE1')
                row['addressline2'] = addr.findtext('C_ADLINE2')
                row['postalcode'] = addr.findtext('C_ZIPCODE')
                row['city'] = addr.findtext('C_CITY')
                row['stateprov'] = addr.findtext('C_STATE_PROV')
                row['country'] = addr.findtext('C_CTRY')
            
            contact = cust.find('ContactInfo')
            if contact is not None:
                phones = contact.findall('C_PHONE')
                for i, p in enumerate(phones[:3]):
                    c_ctry = p.findtext('C_CTRY_CODE')
                    c_area = p.findtext('C_AREA_CODE')
                    c_local = p.findtext('C_LOCAL')
                    c_ext = p.findtext('C_EXT')
                    
                    phone_str = ""
                    if c_local:
                        if c_ctry: phone_str += f"+{c_ctry} "
                        if c_area: phone_str += f"({c_area}) "
                        phone_str += c_local
                        if c_ext: phone_str += c_ext
                    row[f'phone{i+1}'] = phone_str
                
                emails = contact.findall('C_EMAIL')
                for i, e in enumerate(emails[:2]):
                    row[f'email{i+1}'] = e.text
            
            tax_id = cust.find('TaxInfo')
            if tax_id is not None:
                row['lcl_tx_id'] = tax_id.findtext('C_LCL_TX_ID')
                row['nat_tx_id'] = tax_id.findtext('C_NAT_TX_ID')
            
            acc = cust.find('Account')
            if acc is not None:
                row['accountid'] = acc.get('CA_ID')
                row['accountdesc'] = acc.findtext('CA_NAME')
                row['taxstatus'] = acc.get('CA_TAX_ID')
                row['brokerid'] = acc.findtext('CA_B_ID')
                row['status'] = acc.findtext('CA_ST_ID')
        
        data.append(row)
    return data

xml_data = parse_customer_mgmt('sf10/Batch1/CustomerMgmt.xml')
con.execute("""
    CREATE TABLE customermgmt_clean (
        ActionType VARCHAR, update_ts TIMESTAMP, customerid BIGINT, taxid VARCHAR, status VARCHAR,
        lastname VARCHAR, firstname VARCHAR, middleinitial VARCHAR, gender VARCHAR, tier TINYINT,
        dob DATE, addressline1 VARCHAR, addressline2 VARCHAR, postalcode VARCHAR, city VARCHAR,
        stateprov VARCHAR, country VARCHAR, phone1 VARCHAR, phone2 VARCHAR, phone3 VARCHAR,
        email1 VARCHAR, email2 VARCHAR, lcl_tx_id VARCHAR, nat_tx_id VARCHAR, accountid BIGINT,
        accountdesc VARCHAR, taxstatus TINYINT, brokerid BIGINT
    )
""")

for row in xml_data:
    cols = ", ".join(row.keys())
    placeholders = ", ".join(["?" for _ in row.values()])
    con.execute(f"INSERT INTO customermgmt_clean ({cols}) VALUES ({placeholders})", list(row.values()))

con.close()
print("Done!")
