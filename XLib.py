import sys, os, glob, warnings
import paramiko
import pymysql
import cx_Oracle
import psycopg2
import pandas as pd
import pandas.io.sql as sqlio
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import uuid
import urllib.parse
import urllib3
import requests
import json
from datetime import datetime
import rahas as rh

LibraryVersion = "0.1.0"
################## VERSION HISTORY #################
#   
#   0.1.0   :   12-Jan-23
#               Initial release
#
#   Version convention
#   A.B.C   
#               A = Major change 
#               B = New feature
#               C = Bug fix

## ============================================== ##
## This class is for MySQL database related tasks ##
## ============================================== ##
class MySQLDB:
    ## Init constructor, user have to input Host, Database, Username, Password
    def __init__(self, Host, Database, PwdFile, Port=3306):
        if not sys.warnoptions:
            warnings.simplefilter("ignore")     # Disable warning message

        self.LIBRARY_VERSION = LibraryVersion
        self.host = Host
        self.port = Port
        self.database = Database

        with open(PwdFile, "r") as f:
            ec = f.read()

        self.__rahas = ec

    ## Function for MySQL database query and return as List/Tuple
    def Query(self, QueryString):
        result = False, ""
        try:
            x = rh.decrypt(self.__rahas)
            
            connection = pymysql.connect(host = self.host,
                                        port = self.port,
                                        user = x[0],
                                        password = x[1],
                                        db = self.database)

            cursor = connection.cursor()
            
            cursor.execute(QueryString)
            rows = cursor.fetchall()

            result = True, rows
        except Exception as ex:
            result = False, "Error while connecting to and query on MySQL server : " + self.host + "\n" + str(ex)

        finally:
            #closing database connection.
            if 'connection' in locals():
                if(connection):
                    #cursor.close()
                    connection.close()

        return result

    ## Function for MySQL database query and return as Pandas Dataframe
    ## Calling to this function have to input List of Column Name 
    ##   and length of Column Name have to match with number of return columns from the query
    ## For example :  if SQL like "select TESTER_ID,IP_ADDRESS,DATA from ..." --> 3 columns
    ##   then ColumnsName list length must be 3 --> ['TESTER_ID', 'IP_ADDRESS', 'DATA'] or ['AA', 'BB', 'CC'] if you like to rename columns name
    def Query_AsDataFrame_RenameColumnsName(self, QueryString, ColumnsName=""):
        result = False, ""

        # Check to ensure the ColumnsName is List or Tuple
        if isinstance(ColumnsName, (list, tuple)):
            res, data = self.Query(QueryString)
            if res:
                result = True, pd.DataFrame(self.Query(QueryString), columns=ColumnsName) 
            else:
                result = res, data
        
        return result

    def Query_AsDataFrame(self, QueryString):
        result = False, ""

        try:
            x = rh.decrypt(self.__rahas)
            
            connection = pymysql.connect(host = self.host,
                                        port = self.port,
                                        user = x[0],
                                        password = x[1],
                                        db = self.database)

            rows = sqlio.read_sql_query(QueryString, connection)
            result = True, rows

        except Exception as ex:
            result = False, "Error while connecting to and query on MySQL server : " + self.host + "\n" + str(ex)

        finally:
            #closing database connection.
            if 'connection' in locals():
                if(connection):
                    #cursor.close()
                    connection.close()

        return result

    ## Function for MySQL database data insert or update
    ## It will return True if execution success, else retunr False
    ## No data return
    def ExecuteNonQuery(self, SQLString):
        result = False, "Not OK"
        try:
            x = rh.decrypt(self.__rahas)
            
            connection = pymysql.connect(host = self.host,
                                        port = self.port,
                                        user = x[0],
                                        password = x[1],
                                        db = self.database)

            cursor = connection.cursor()
            
            cursor.execute(SQLString)
            connection.commit()
            result = True, "OK"

        except Exception as ex:
            result = False, "Error while executing command on MySQL server : " + self.host + "\n" + str(ex)

        finally:
            #closing database connection.
            if 'connection' in locals():
                if(connection):
                    #cursor.close()
                    connection.close()
                    
        return result

    ## Function for MySQL database
    ## Just open the connection
    def OpenConnection(self):
        result = False, "Not OK"
        try:
            x = rh.decrypt(self.__rahas)
            
            connection = pymysql.connect(host = self.host,
                                        port = self.port,
                                        user = x[0],
                                        password = x[1],
                                        db = self.database)

            #self.cursor = connection.cursor()
            result = True, "OK"

        except Exception as ex:
            result = False, "Error while connecting to MySQL server : " + self.host + "\n" + str(ex)

        return result

    ## Function for MySQL database
    ## Just Execute the SQL command
    ## Need to call OpenConnection before able to use this function
    def ExecuteCommand(self, SQLString):
        result = False, "Not OK"
        try:
            self.connection.cursor().execute(SQLString)
            self.connection.commit()
            result = True, "OK"

        except Exception as ex:
            result = False, "Error while executing SQL command on MySQL server : " + self.host + "\n" + str(ex)

        return result
    
    def CloseConnection(self):
        result = False, "Not OK"
        try:
            if(self.connection):
                self.connection.cursor().close()
                self.connection.close()
            result = True, "OK"

        except Exception as ex:
            result = False, "Error while closing connection from MySQL server : " + self.host + "\n" + str(ex)

        return result

## =============================================== ##
## This class is for Oracle database related tasks ##
## =============================================== ##
class OracleDB:
    ## Init constructor, user have to input Host, Database, Username, Password
    def __init__(self, Host, ServiceName, PwdFile, Port=1521):
        self.LIBRARY_VERSION = LibraryVersion
        self.host = Host
        self.dsn = cx_Oracle.makedsn(Host, Port, service_name=ServiceName)
        self.encoding = 'UTF-8'

        with open(PwdFile, "r") as f:
            ec = f.read()

        self.__rahas = ec

    ## Function for Oracle database query and return as List/Tuple
    def Query(self, QueryString):
        result = False, ""
        try:
            x = rh.decrypt(self.__rahas)

            connection = cx_Oracle.connect(
                user = x[0],
                password = x[1],
                dsn = self.dsn,
                encoding = self.encoding)

            cursor = connection.cursor()
            cursor.execute(QueryString)
            rows = cursor.fetchall()
        
            result = True, rows

        except Exception as ex:
            result = False, "Error while connecting to and query on Oracle server : " + self.host + "\n" + str(ex)

        finally:
            #closing database connection.
            if 'connection' in locals():
                if(connection):
                    #cursor.close()
                    connection.close()

        return result

    ## Function for Oracle database query and return as Pandas Dataframe
    ## Calling to this function have to input List of Column Name 
    ##   and length of Column Name have to match with number of return columns from the query
    ## For example :  if SQL like "select TESTER_ID,IP_ADDRESS,DATA from ..." --> 3 columns
    ##   then ColumnsName list length must be 3 --> ['TESTER_ID', 'IP_ADDRESS', 'DATA'] or ['AA', 'BB', 'CC'] if you like to rename columns name
    def Query_AsDataFrame_RenameColumnsName(self, QueryString, ColumnsName=""):
        result = False, ""
        
        # Check to ensure the ColumnsName is List or Tuple
        if isinstance(ColumnsName, (list, tuple)):
            res, data = self.Query(QueryString)
            if res:
                result = True, pd.DataFrame(self.Query(QueryString), columns=ColumnsName) 
            else:
                result = res, data
        
        return result

    def Query_AsDataFrame(self, QueryString):
        result = False, ""

        try:
            x = rh.decrypt(self.__rahas)

            connection = cx_Oracle.connect(
                user = x[0],
                password = x[1],
                dsn = self.dsn,
                encoding = self.encoding)

            rows = sqlio.read_sql_query(QueryString, connection)
            result = True, rows

        except Exception as ex:
            result = False, "Error while connecting to and query on Oracle server : " + self.host + "\n" + str(ex)

        finally:
            #closing database connection.
            if 'connection' in locals():
                if(connection):
                    #cursor.close()
                    connection.close()

        return result

    ## Function for Oracle database data insert or update
    ## It will return True if execution success, else return False
    ## No data return
    def ExecuteNonQuery(self, SQLString):
        result = False, "Not OK"
        try:
            x = rh.decrypt(self.__rahas)

            connection = cx_Oracle.connect(
                user = x[0],
                password = x[1],
                dsn = self.dsn,
                encoding = self.encoding)

            self.cursor = connection.cursor()
            self.cursor.execute(SQLString)
            self.connection.commit()

            result = True, "OK"

        except Exception as ex:
            result = False, "Error while executing command on Oracle server : " + self.host + "\n" + str(ex)

        return result
    
    def OpenConnection(self):
        result = False, "Not OK"
        try:
            x = rh.decrypt(self.__rahas)
            
            self.connection = cx_Oracle.connect(
                user = x[0],
                password = x[1],
                dsn = self.dsn,
                encoding = self.encoding)

            #self.cursor = connection.cursor()
            result = True, "OK"

        except Exception as ex:
            result = False, "Error while connecting to Oracle server : " + self.host + "\n" + str(ex)

        return result
    
    def ExecuteCommand(self, SQLString):
        result = False, "Not OK"
        try:
            self.connection.cursor().execute(SQLString)
            self.connection.commit()
            result = True, "OK"

        except Exception as ex:
            result = False, "Error while executing SQL command on Oracle server : " + self.host + "\n" + str(ex)

        return result
    
    def CloseConnection(self):
        result = False, "Not OK"
        try:
            if(self.connection):
                self.connection.cursor().close()
                self.connection.close()
            result = True, "OK"

        except Exception as ex:
            result = False, "Error while closing connection from Oracle server : " + self.host + "\n" + str(ex)

        return result

## ==================================================== ##
## This class is for PostgresSQL database related tasks ##
## ==================================================== ##
class PgSQLDB:
    ## Init constructor, user have to input Host, Database, Username, Password
    def __init__(self, Host, Database, PwdFile, Port=5432, Timeout=10):
        if not sys.warnoptions:
            warnings.simplefilter("ignore")     # Disable warning message

        self.LIBRARY_VERSION = LibraryVersion
        self.host = Host
        self.port = Port
        self.database = Database
        self.timeout = Timeout
        
        with open(PwdFile, "r") as f:
            ec = f.read()

        self.__rahas = ec

    ## Function for PostgresSQL database query and return as List/Tuple
    def Query(self, QueryString):
        result = False, ""
        try:
            x = rh.decrypt(self.__rahas)

            connection = psycopg2.connect(host = self.host,
                                        port = self.port,
                                        user = x[0],
                                        password = x[1],
                                        dbname = self.database)

            cursor = connection.cursor()
            
            cursor.execute(QueryString)
            rows = cursor.fetchall()
            
            result = True, rows

        except Exception as ex:
            result = False, "Error while connecting to and query on PostgreSQL server : " + self.host + "\n" + str(ex)

        finally:
            #closing database connection.
            if 'connection' in locals():
                if(connection):
                    #cursor.close()
                    connection.close()

        return rows

    ## Function for PostgresSQL database query and return as Pandas Dataframe
    ## Calling to this function have to input List of Column Name 
    ##   and length of Column Name have to match with number of return columns from the query
    ## For example :  if SQL like "select TESTER_ID,IP_ADDRESS,DATA from ..." --> 3 columns
    ##   then ColumnsName list length must be 3 --> ['TESTER_ID', 'IP_ADDRESS', 'DATA'] or ['AA', 'BB', 'CC'] if you like to rename columns name
    def Query_AsDataFrame_RenameColumnsName(self, QueryString, ColumnsName=""):
        result = False, ""

        # Check to ensure the ColumnsName is List or Tuple
        if isinstance(ColumnsName, (list, tuple)):
            res, data = self.Query(QueryString)
            if res:
                result = True, pd.DataFrame(self.Query(QueryString), columns=ColumnsName) 
            else:
                result = res, data

        return result

    def Query_AsDataFrame(self, QueryString):
        result = False, ""

        try:
            x = rh.decrypt(self.__rahas)

            connection = psycopg2.connect(host = self.host,
                                        port = self.port,
                                        user = x[0],
                                        password = x[1],
                                        dbname = self.database,
                                        connect_timeout = self.timeout)

            rows = sqlio.read_sql_query(QueryString, connection)
            result = True, rows

        except Exception as ex:
            result = False, "Error while connecting to and query on PostgreSQL server : " + self.host + "\n" + str(ex)

        finally:
            #closing database connection.
            if 'connection' in locals():
                if(connection):
                    #cursor.close()
                    connection.close()

        return result

    ## Function for PostgresSQL database data insert or update
    ## It will return True if execution success, else retunr False
    ## No data return
    def ExecuteNonQuery(self, SQLString):
        result = False, "Not OK"

        try:
            x = rh.decrypt(self.__rahas)

            connection = psycopg2.connect(host = self.host,
                                        port = self.port,
                                        user = x[0],
                                        password = x[1],
                                        dbname = self.database)

            cursor = connection.cursor()
            
            cursor.execute(SQLString)
            result = True, "OK"

        except Exception as ex:
            result = False, "Error while connecting to and query on PostgreSQL server : " + self.host + "\n" + str(ex)

        finally:
            #closing database connection.
            if 'connection' in locals():
                if(connection):
                    #cursor.close()
                    connection.close()

        return result

    ## Function for PostgresSQL database
    ## Just open the connection
    def OpenConnection(self):
        result = False, "Not OK"
        try:
            x = rh.decrypt(self.__rahas)

            self.connection = psycopg2.connect(host = self.host,
                                        port = self.port,
                                        user = x[0],
                                        password = x[1],
                                        dbname = self.database)

            #self.cursor = connection.cursor()
            result = True, "OK"

        except Exception as ex:
            result = False, "Error while connecting to and query on PostgreSQL server : " + self.host + "\n" + str(ex)

        return result

    ## Function for PostgresSQL database
    ## Just Execute the SQL command
    ## Need to call OpenConnection before able to use this function
    def ExecuteCommand(self, SQLString):
        result = False, "Not OK"

        try:
            self.connection.cursor().execute(SQLString)
            result = True

        except Exception as ex:
            result = False, "Error while executing SQL command on PostgreSQL server : " + self.host + "\n" + str(ex)

        return result
    
    def CloseConnection(self):
        result = False, "Not OK"
        try:
            if(self.connection):
                self.connection.cursor().close()
                self.connection.close()
            result = True, "OK"
        except Exception as ex:
            result = False, "Error while closing connection from PostgreSQL server : " + self.host + "\n" + str(ex)

        return result

## =================================================== ##
##   This class is for Drive Functions related tasks   ##
## =================================================== ##
class ElasticSearch:

    def __init__(self, Host, Port):
        self.ESClient = Elasticsearch(Host, port=Port, timeout=30)

    def ESBulkInsert_BatchSize_DataFrame(self, df, IndexName):
        actions = []
        count = 0
        for index, row in df.iterrows():
            actions.append(
                {
                    "_index" : IndexName,
                    "_id" : uuid.uuid4(), # random UUID for _id
                    "doc_type" : "doc", # document _type
                    "doc": json.loads(row.to_json(date_format='iso'))
                }
            )
            count += 1
            if count >= 1000:
                #print(len(actions))
                #print(actions[0])
                response = helpers.bulk(self.ESClient, actions)
                actions = []
                count = 0
        
        if actions != []:
            #print(len(actions))
            #print(actions[0])
            response = helpers.bulk(self.ESClient, actions)
        
        return response

## =================================================== ##
##   This class is for Drive Functions related tasks   ##
## =================================================== ##
class DriveFunctions:

    def __init__(self):
        self.LIBRARY_VERSION = LibraryVersion

    def GetDriveValidOperation(self, DriveSN):
        msg = "/GetPut?TYPE=GET&CLA=STRK"
        table = "QUALIFY"
        s = f":HEADER\r\nTABLE=VERSION\r\nVERSION\r\nSTRING\r\n01.00\r\nTABLE={table}\r\nSERIAL_NUM,PART_NUM,OPERATION,EQUIP_TYPE\r\nSTRING,STRING,STRING,STRING\r\n{DriveSN},000000-000,PREX,50"
        res = self.open_url_post_method("http://tkddo-ffsh01.kor.thai.seagate.com:8080" + msg, s)
        
        keys = res.split("\r\n")[6].split(",")
        values = res.split("\r\n")[8].split(",")
        
        result = {}
        for i in range(len(keys)):
            try:
                key = keys[i]
                value = values[i]
                result[key] = value
            except:
                pass

        return result

    def GetDriveAttributes(self, DriveSN):
        msg = "/GetPut?TYPE=GET&CLA=STRK"
        table = "CURRENT_ATTRIBUTES"
        s = f":HEADER\r\nTABLE=VERSION\r\nVERSION\r\nSTRING\r\n01.00\r\nTABLE={table}\r\nSERIAL_NUM,PART_NUM\r\nSTRING,STRING\r\n{DriveSN},000000-000"
        res = self.open_url_post_method("http://tkddo-ffsh01.kor.thai.seagate.com:8080" + msg, s)
        result = {}
        for data in res.split("\r\n"):
            try:
                key = data.split(",")[0]
                value = data.split(",")[1]
                result[key] = value
            except:
                pass
            
        return result

    def GetDriveSNFromCarrierID(self, CarrierSN):
        msg = "/GetPut?TYPE=GET&CLA=STRK"
        table = "CURRENT_ATTRIBUTES"
        s = f":HEADER\r\nTABLE=VERSION\r\nVERSION\r\nSTRING\r\n01.00\r\nTABLE={table}\r\nSERIAL_NUM,PART_NUM,OPERATION,EQUIP_TYPE\r\nSTRING,STRING,STRING,STRING\r\n{CarrierSN},000000-000,PRE2,50"
        res = self.open_url_post_method("http://tkddo-ffsh01.kor.thai.seagate.com:8080" + msg, s)
        
        result = {"HDA_SERIAL_00" : "UNKNOWN"}
        for data in res.split("\r\n"):
            try:
                key = data.split(",")[0]
                value = data.split(",")[1]
                result[key] = value
            except:
                pass
            
        return result["HDA_SERIAL_00"]

    def open_url_post_method(self, page_url, post_data=""):
        try:
            response = requests.post(page_url, data=post_data)
            return response.text

        except Exception as e:
            print(e)
            return False

## ==================================================== ##
## This class is about Files Transferring related tasks ##
## ==================================================== ##
class FilesTransfer:

    def __init__(self, IPAddress, PwdFile, Port = 22):
        if not sys.warnoptions:
            warnings.simplefilter("ignore")     # Disable warning message

        self.LIBRARY_VERSION = LibraryVersion
        self.host = IPAddress
        self.port = Port
        
        with open(PwdFile, "r") as f:
            ec = f.read()

        self.__rahas = ec
    
    def OpenConnection(self):
        result = False, ""
        try:
            x = rh.decrypt(self.__rahas)

            self.transport = paramiko.Transport((self.host, self.port))
            self.transport.connect(username = x[0], password = x[1])
            self.sftp = paramiko.SFTPClient.from_transport(self.transport)

            result = True, "Connected"

        except Exception as ex:
            result = False, "Error while connecting to (S)FTP server : " + self.host + "\n" + str(ex)
    
        return result

    def DownloadFile(self, remote_path, local_path):
        self.sftp.get(remote_path, local_path)

    def UploadFile(self, local_path, remote_path, remote_filename):
        self.sftp.chdir(remote_path)
        self.sftp.put(local_path, remote_filename)

    def CreteRemoteFolder(self, remote_path):
        self.sftp.mkdir(remote_path)

    def CloseConnection(self):
        self.sftp.close()

## =================================================== ##
## This class is for Utilities functions related tasks ##
## =================================================== ##
class Utilities:

    def __init__(self):
        self.LIBRARY_VERSION = LibraryVersion

    ## Function to open URL and return html source
    ## FIX ME --- This function is not work now
    def open_url_post_method(self, page_url, post_data=""):
        try:
            response = requests.post(page_url, data=post_data)
            print(response.text)
            #return response.data.decode('utf-8')
        except Exception as e:
            print(e)
            return False

    ## Function to open URL and return html source
    def open_url_as_source(self, page_url, connection_timeout=2.0, data_read_timeout=5.0):
        try:
            http = urllib3.PoolManager()
            response = http.request('GET', page_url, timeout=urllib3.Timeout(connect=connection_timeout, read=data_read_timeout))
            return response.data.decode('utf-8')
        except:
            return False
    
    def load_url(self, url, timeout):
        with urllib.request.urlopen(url, timeout=timeout) as conn:
            return conn.read().decode('utf-8')

    def DatetimeToString(self, DateTimeVariable, DateStringFormat):
        return datetime.strftime(DateTimeVariable, DateStringFormat)

    def StringToDatetime(self, DateStringVariable, DateStringFormat):
        return datetime.strptime(DateStringVariable, DateStringFormat)

    def StringTimeReFormat(self, DateStringVariable, CurrentDateStringFormat, NewDateStringFormat):
        temp_date = self.StringToDatetime(DateStringVariable, CurrentDateStringFormat)
        return self.DatetimeToString(temp_date, NewDateStringFormat)

## =================================================== ##
## This class is for Heartbeat functions related tasks ##
## =================================================== ##
class HeartBeat:
    ## Init constructor, user have to input Host, Database, Username, Password
    def __init__(self, ApplicationName, Version, Developer):
        if not sys.warnoptions:
            warnings.simplefilter("ignore")

        self.LIBRARY_VERSION = LibraryVersion
        self.AppName = ApplicationName
        self.AppVersion = Version
        self.Developer = Developer

    def AppStart(self):
        return True

    def AppFinish(self):
        return True

    def ReportError(self):
        return True

    def ReportMessage(self):
        return True

if __name__ == "__main__":
    MyDB = MySQLDB("10.8.142.206", "DETE_GEMINI_SYSTEM", "tapmynew.enc")
    
    #MyDB.OpenConnection()
    res, df = MyDB.Query_AsDataFrame("SELECT TESTER_ID, IP_ADDRESS FROM GeminiSystemID")
    #MyDB.CloseConnection()

    ES = ElasticSearch(['kor-keep1.seagate.com'], 9206)
    ES.ESBulkInsert_BatchSize_DataFrame(df, "tapparit-test-jan23")
    
    for idx, row in df.iterrows():
        tester_id = row["TESTER_ID"]
        try:
            FT = FilesTransfer(row["IP_ADDRESS"], "merlin.enc")
            FT.OpenConnection()
            FT.DownloadFile("/var/merlin/Logs/Testtime.log", f"C:\\Jupyter-Lab\\Test Time\\Raw Data\\{tester_id}_Testtime.log")
            FT.CloseConnection()
        except:
            print(f"ERROR {tester_id}")