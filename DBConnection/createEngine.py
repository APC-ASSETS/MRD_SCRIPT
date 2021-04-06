from DBConnection import dbCredentials as dbc # importing database credentials file

def connect(USR_NME, PSWRD, DB_NME, dbused='mysql', PORT_NMBR=None)->"RETURNS THE ENGINE":


    """ ReturnS engine object connected with the credentials provided """

    from sqlalchemy import create_engine # engine for connection with postgres/mysql etc

    connectTo = {
        "postgres":f"postgresql://{USR_NME}:{PSWRD}@localhost/{DB_NME}",
        "mysql":f"mysql://{USR_NME}:{PSWRD}@localhost/{DB_NME}",
    }

    engine = create_engine(connectTo[dbused])

    return engine

USR_NME = dbc.USR_NME
PSWRD = dbc.PSWRD
DB_NME = dbc.DB_NME
dbconnector = dbc.dbused

engn = connect(USR_NME, PSWRD, DB_NME, dbconnector, PORT_NMBR=None)
