import MagicBox as mb
import pandas as pd
from DBConnection import createEngine, dbCredentials


USR_NME = dbCredentials.USR_NME
PSWRD = dbCredentials.PSWRD
# DB_NME = dbCredentials.DB_NME
DB_CONCTR =  dbCredentials.dbused

# engn = createEngine.connect(USR_NME, PSWRD, DB_NME, dbused=DB_CONCTR)
DB_NME = "MRD"
mainEngn = createEngine.connect(USR_NME, PSWRD, DB_NME, dbused=DB_CONCTR)

parmCols = pd.read_csv("./PARAMCODES.csv").sort_values("D4 Paramcodes(All Files)", ignore_index=True)

paramList = parmCols['D4 Paramcodes(All Files)'].unique().tolist()


for name in ['created_timestamp']: #paramList: #['created_timestamp']:

    try:
        
        pd.read_sql_query(f'ALTER TABLE Meter_Dayprofile_Data_D4_New ADD `{name.strip()}` DATETIME NULL;', mainEngn)

        # pd.read_sql_query(f'ALTER TABLE Meter_Dayprofile_Data_D4_New ADD `{name.strip()}` DECIMAL(10,2) NULL;', mainEngn)

    except Exception as e:

        print(e)

# file = pd.read_sql_query("SELECT * FROM Meter_Dayprofile_Data_D4_New", engn)
#
# file.to_sql("Meter_Dayprofile_Data_D4_New", mainEngn, index=False, if_exists='append')
