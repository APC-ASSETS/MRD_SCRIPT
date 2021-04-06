# importing libraries
import os
import datetime
import pandas as pd
import staticVar
import MagicBox as mb
from MagicBox import coLr
import xml.etree.ElementTree as ET
from sqlalchemy import create_engine # engine for connection with postgres/mysql
from sqlalchemy.types import Float, String, Time

#************ CREATING CONNECTION **************#
USR_NME = 'root' # dm username
PSWRD = 'Analytics@123' # db password
DB_NME ='MRD_TEST' # name of database
TABL_NME = 'Meter_Dayprofile_Data_D4_New' # name of table
ENGN = create_engine(f"mysql://{USR_NME}:{PSWRD}@localhost/{DB_NME}") # creating engine
#**********************************************#

#*************** LOCATION OF PATH WHERE XML FILES ARE STORED *****************#
XML_PTH = "/home/santosh/mrd_source/Test/TEST_FILES/"
#*****************************************************************************#
def main(root, *args):

    #*********** ARGUMENTS EXTRACTION **************#
    fileName = args[0] # xml filename
    timeStmp = args[1] # CPU time stamp
    tableName = args[2] # database table name
    ENGN = args[3] # database engine
    #**********************************************#

    #preparing file, making some empty dictionaries and defininng variables
    counter = 0
    orderBy =[
        'FileName','G1', 'G2',
        'IntervalPeriod', 'Interval_d4', 'Date',
    ]
    maxR,minR = 0, 0
    intervalList = []
    paramDict = mb.paramcode_dict()
    dateDict={'Date':[]} # to store date
    # paramTags = [key for key,val in paramDict.items()] # unique tags of PARAMCODE

    dateList = mb.tag_values(root, "./UTILITYTYPE/D4/DAYPROFILE", "DATE")# list of dates present inside xml file
    getColNames = pd.read_sql_query(f"SELECT * FROM {tableName} LIMIT 1;", ENGN) # reading sql query to fetch column names
    getColNames.drop(orderBy+['created_timestamp'], axis=1, inplace=True) # deleting all irrelevant columns
    paramTags = getColNames.columns.tolist() # list of parametercode present in table

    try:

        for date in dateList: # iretarting over date list

            interValMin = [] # list to store intervall values, will get updated at the start of root iterator

            for intrvl in range(1, 49): # iterating over interaval, currently set to 1-48

                #querying over date tags, interval to fetch PARAMCODE
                for item in root.findall(f"./UTILITYTYPE/D4/DAYPROFILE/[@DATE='{date}']/IP/[@INTERVAL='{intrvl}']/PARAMETER"):

                    if item.attrib['PARAMCODE'] not in paramTags: # updating dictionary if PARAMCODE not present in ubique list

                        mb.add_parametercode(ENGN,tableName, item.attrib['PARAMCODE'])# creating new column in table
                        paramDict.update({item.attrib['PARAMCODE']:item.attrib['VALUE']}) # updating dictionary

                    else:

                        paramDict[item.attrib['PARAMCODE']].append(item.attrib['VALUE']) # appending values to dictionary

                    interValMin.append(intrvl) # appending the interval code consisting some value

            # swap value to generate number of times date occured when the value is found in PARAMCODE
            maxR = len(paramDict[item.attrib['PARAMCODE']]) # getting the maximmum value in list

            if len(interValMin) < 1:

                interValMin= [1]

            maxR,minR = maxR-minR, maxR # setting up max and min range

            [intervalList.append(_) for _ in range(min(interValMin), 49)] # appending value interval list

            [dateDict['Date'].append(date) for i in range(0, maxR)] # appending number of times date occur till max interval

            del interValMin

        childDict = {} # creating an enpty dictionary to contain the values formed above

         # updating dictionaries with only those keys which have value lenght more than 0
        [childDict.update({f'{key}':val}) for key, val in paramDict.items() if val]

        childDict.update({"Interval_d4": intervalList})# updating interval with the list formed above

        # process to create dataframe from an uneven value length
        childFrame = pd.DataFrame.from_dict(childDict, orient='index') # converting  dictionary to dataframe

        childFrame = childFrame.transpose() # swapping rows -> columns and columns -> rows

        dateFrame = pd.DataFrame(data=dateDict) # converting date dictionary formed above to dataframe

        xlmFrame = pd.concat([dateFrame, childFrame], axis=1) # joining date and child dataframe

        xlmFrame.dropna(inplace=True) # dropping all the na values from the dataframe

        # PROESS TO CREATE DATAFRAME WITH FILENAME, G1,G2 AND INTERVALPERIOD TAGS/VALUES

        # appending date date to dataframe --> date*interval(48), each date will be repeated 48 times
        mainFrame = pd.DataFrame(data={"Date":dateList*48})

        mainFrame.sort_values('Date', inplace=True, ignore_index=True) # sorting values by date ascending order

        # getting G1, G2 tag text
        gTagsDict = mb.get_tag_text(root, "./UTILITYTYPE/D1",['G1','G2'], len(dateList))
        finalPrep = pd.DataFrame(gTagsDict)# making dataframe of G tags
        finalPrep['Interval_d4'] = [_ for _ in range(1,49)]*len(dateList)# setting up interval, adding column to dataframe

        finalPrep = pd.concat([finalPrep,mainFrame], axis=1) # joining main frame with interval and g tags dataframe

        # converting interval tags to string
        finalPrep['Interval_d4'] =finalPrep['Interval_d4'].apply(str)
        xlmFrame['Interval_d4']=xlmFrame['Interval_d4'].apply(str)

        # finalizing file to create a one single dataframe consisting of all extracted values
        finalPrep = pd.merge(finalPrep, xlmFrame, on=['Date',"Interval_d4"], how='outer')

        # fetching interval period value and adding it to dataframe
        finalPrep['IntervalPeriod'] = mb.get_interval(root, "./UTILITYTYPE", 1, "D4" )
        finalPrep['FileName'] = fileName # adding filename column

        [orderBy.append(i) for i in finalPrep.columns if i not in orderBy] # ordering dataframe

        finalPrep = finalPrep[orderBy] # putting columns in order
        finalPrep['G2'] = finalPrep['G2'].astype('datetime64[s]')#apply(mb.cnvrtToDTS)# converting each value to datetime
        finalPrep['Date'] = finalPrep['Date'].astype('datetime64[s]')#.apply(mb.cnvrtToDTS)# converting each value to datetime
        finalPrep['created_timestamp'] = timeStmp# making current time stamp column


        if len(finalPrep) > 0: # checking the length of dataframe
            # fileName = fileName.replace(".XML",".csv")
            chnk = len(finalPrep)//3

            finalPrep.fillna(" ", inplace=True)

            finalPrep.to_sql(f"{tableName}", ENGN, index=False, if_exists='append', method='multi', chunksize=chnk)
            # to_csv(f"./GENERATED_CSV_FILES/{fileName}", index=False, chunksize=90000)
            mb.update_log("D4_RECORD_NEW", timeStmp, f"{len(xlmFrame)} rows got inserted from {fileName}")# updating record inserted log

            counter+=1

        else:

            # will update log if d4  tag doesnot exists in xml file
            mb.update_log("D4_NOTFOUND_NEW", timeStmp, f"{fileName} Doesn't contains D4 Tags")

        return counter

    except Exception as e:

        # will update log if XML file is not compiled
        mb.update_log("D4_FILE_REVIEW_NEW", timeStmp, f"{fileName} cannot be compiled due to: {e}")

        return counter

#***********************************************************************************************#
# if __name__ == "__main__":
#***********************************************************************************************#
filePath = XML_PTH #input(coLr.OKGREEN+coLr.BOLD+"ENTER PATH TO XML FILES: "+coLr.ENDC)

fileList = os.listdir(filePath) # getting list of files present in directory

startTime = mb.get_time() # storing cpu start time

current_Time = mb.get_current_time() # getting current datetime stamp, will be updated for each file

print(coLr.WARNING+coLr.BOLD+f"* DATA EXTRACTION IN PROGRESS, DO NOT CLOSE THIS WINDOW! *".upper()+coLr.ENDC)

fileCounter = 0

for file in fileList:

    root = ET.parse(filePath+'/'+file)

    counter = main(root, file, current_Time, TABL_NME, ENGN)

    fileCounter+=counter


endTime = mb.get_time()-startTime # calculating time taken to complete the whole process
print(coLr.OKBLUE+coLr.BOLD+f"Data Extraction of {fileCounter} file/files and UPLOAD(TO DATABASE) took {round(endTime,2)} Seconds".upper()+coLr.ENDC)
