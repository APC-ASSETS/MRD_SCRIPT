"""
File contains the functions or classes to be used in scripts
"""

import os
import staticVar
import pandas as pd
import time, datetime


class coLr:

    """ COLOR SCHEME FOR TERMINAL """

    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

# local function to convert string to datetime stamp
cnvrtToDTS = lambda x: pd.to_datetime(x)

def get_time()->"RETURNS CPU TIME":

    return time.process_time()

def get_current_time()->"RETURNS DATE TIME STAMP":

    return datetime.datetime.now().strftime("%d-%m-%y %H:%M:%S") # getting datetime stamp as string

def tag_values(root, tagPath,findTag) -> "RETURNS A LIST":
    """
        Function will return the list of tag present inside xml file
        as per the path and tag provided by the user
    """

    dateList = [] # empty list

    for item in root.findall(tagPath): # iterating over provided path

        dateList.append(item.get(findTag)) # updating list of

    return dateList

def get_interval(root, *args)-> "RETURNS INTERVAL PERIOD":
    """
    Function will locate and return INTERVALPEDIOD VALUE as per the path
    provided by the user. Return value will be an integer
    """
    path = args[0] # path you want to search tag
    code = args[1] # code number you want to search
    tag = args[2] # tag you want to seach
    finalPath = path+f"/[@CODE='{code}']/"+tag # final path to search

    for item in root.findall(finalPath):

        return item.attrib['INTERVALPERIOD'] # returning value


def get_tag_text(root, *args)->"RETURNS DICTIONARY":
    """
        Function will return the dictionary of tags passed as
        a list on second argument or arg[1]
    """
    path = args[0] # path to root tag
    tagList = args[1] # tag list
    dateLen = args[2] # number of unique dates
    times = 48*dateLen # number of times value will be repeated
    makeDict = {} # dictionary to be returned

    for tag in tagList: # iterating over tag list provided by user

        for item in root.findall(path+f"/{tag}"): # finding all the tags

            makeDict.update({tag:[item.text]*times}) # updating dictionary

    return makeDict


def paramcode_dict()->"RETURNS DICTIONARY":
    """
    Function will return dictionary of preset PARAMCODE.
    Edit the dictionary as required,
    You can add or delete any key you want.

    """

    paramDict = {
    'P1-1-7-4-0':[],
    'P1-2-1-1-0':[],
    'P1-2-1-4-0':[],
    'P1-2-2-1-0':[],
    'P1-2-2-4-0':[],
    'P1-2-3-1-0':[],
    'P1-2-3-4-0':[],
    'P1-2-7-4-0':[],
    'P11-1-0-0-0':[],
    'P1203-1-14-2-0':[],
    'P1205-1-1-4-0':[],
    'P1205-1-2-4-0':[],
    'P1205-1-3-4-0':[],
    'P1206-1-1-4-0':[],
    'P1206-1-2-4-0':[],
    'P1206-1-3-4-0':[],
    'P1208-1-0-0-0':[],
    'P1216-1-1-1-0':[],
    'P1216-1-2-1-0':[],
    'P1216-1-3-1-0':[],
    'P1221-2-0-0-0':[],
    'P1225-3-0-0-0':[],
    'P1253-1-5-4-0':[],
    'P1254-2-7-4-0':[],
    'P2-1-1-4-0':[],
    'P2-1-2-4-0':[],
    'P2-1-3-4-0':[],
    'P2-1-4-4-0':[],
    'P2-1-5-4-0':[],
    'P3-1-1-1-0':[],
    'P3-1-2-1-0':[],
    'P3-1-3-1-0':[],
    'P3-3-1-1-0':[],
    'P3-3-2-1-0':[],
    'P3-3-3-1-0':[],
    'P4-4-4-1-0':[],
    'P4-4-4-2-0':[],
    'P7-1-14-2-0':[],
    'P7-1-18-1-0':[],
    'P7-1-18-2-0':[],
    'P7-1-5-1-0':[],
    'P7-1-5-2-0':[],
    'P7-1-6-1-0':[],
    'P7-1-6-2-0':[],
    'P7-2-1-0-0':[],
    'P7-2-19-0-0':[],
    'P7-2-2-0-0':[],
    'P7-2-20-0-0':[],
    'P7-2-3-0-0':[],
    'P7-2-4-0-0':[],
    'P7-2-7-0-0':[],
    'P7-2-8-0-0':[],
    'P7-3-13-1-0':[],
    'P7-3-18-0-0':[],
    'P7-3-18-2-0':[],
    'P7-3-5-0-0':[],
    'P7-3-6-0-0':[],
    'P9-4-0-0-0':[],

    }

    return paramDict


def get_abs_path(path)->"WILL RETURN ABSOLUTE PATH OF THE DIRECTORY PROVIDED":

    return os.path.abspath(path)


def make_directory(filePath, checkDir) -> "Will check and create a directory on given path, Returns dir name with path":

    """ Function will check whether not or a directory exists on a give path, will create if it not. """

    import os

    filePath = get_abs_path(filePath) # getting the absoulte path
    dirName = filePath+"/"+checkDir # creating full path for directory

    if not os.path.exists(dirName):

        os.makedirs(dirName)

    return dirName


def create_text_file(path,*args:"Should be a list of file names")-> "RETURN NOTHING":

    """
        Function will create a empty text file inside the directory provided,
    """

    path = get_abs_path(path) # getting absolute path
    fileName = args[0]

    if type(fileName) is not list:

        fileName = [fileName]

    for name in fileName:


        if name+".txt" in os.listdir(path):# checking if the file is present inside the directory or not

            pass # do nothing

        else:

            open(path+"/"+name+'.txt', 'w+') # create empty file


def update_log(fileName, *args:"First argument should be datetime, second should be the message")->"WILL UPDATE THE LOG FILE, RETURNS NONE":
    """ Will update log file """

    logFile = "./Logs/"+f"{fileName.upper()}_Logs.txt" # creating full path

    create_text_file("./Logs/", f"{fileName.upper()}_Logs") # will check if file exists

    with open(logFile, 'a') as f:

        f.write(f"{args[0]}: {args[1]}\n")


def alter_table(ENGN, tableName, colName, dataType, val="")->"RETURNS NOTHING":
    """
        Function will create a new column within a table
    """
    dataType = staticVar.dataType_Dict[dataType.lower()] # fetching correct datatype

    try:
        # creating column inside table
        pd.read_sql_query(f'ALTER TABLE {tableName} ADD `{colName.strip()}` {dataType}({val}) NULL;', engn)

    except Exception as e:

        update_log("ALTER_ERROR", f"ERROR WHILE ALTERING TABLE, {e}") #updating error log
