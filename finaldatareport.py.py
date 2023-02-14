from sqlalchemy import create_engine
import pandas as pd
import mysql.connector
import numpy as np
from pandas import ExcelWriter
from datetime import datetime, timedelta

#///////////////////email///////////////////
import csv
from tabulate import tabulate
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
from pretty_html_table import build_table
# ///////////////////////email
# from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

import re

me = 'wijesinghelali@gmail.com'
password = 'nxfubrenxauhpchh'

# you = 'aravindafilm@gmail.com','aravindaedirisinghe99@gmail.com'
# you = 'frankkamalnonis@gmail.com'

print('Pipline Starting.......')

# //Date Time// at 5.00pm
# today = datetime.today()
# # today = datetime.today() - timedelta(hours=200, minutes=0)
# backto24h = datetime.today() - timedelta(hours=24, minutes=0)
# emailsendingtime = today.strftime("%d/%m/%Y")
# emailtoday = today.strftime("%m/%d/%Y (%H:%M:%S)")
# emailbackto24h = backto24h.strftime("%m/%d/%Y (%H:%M:%S)")
# ///////////////////////////////////////////////////////////
todayx = datetime.today()
emailsendingtime = todayx.strftime("%d/%m/%Y")
emailtime1 = todayx.strftime("%d/%m/%Y (05:00:pm)")
data2 = datetime.today() - timedelta(hours=24, minutes=0)
emailtime2 =data2.strftime("%d/%m/%Y (05:00:pm)")
# for dataretrewing
# sqltoday = todayx.strftime("%d/%m/%Y 16:59:59.999999")
# sqlbackto24h = data2.strftime("%d/%m/%Y 16:59:59.999999")

sqltoday = todayx.strftime("%Y-%m-%d 16:59:59.999999")
sqlbackto24h = data2.strftime("%Y-%m-%d 16:59:59.999999")


# ////////////
Live_db_connection_str = 'mysql+pymysql://kapraadmin:802710062V@139.162.50.185:3306/tf_iot'
db_connection = create_engine(Live_db_connection_str)
db_connection_str = 'mysql+pymysql://root:root@localhost:3306/touchfree'
db_connection = create_engine(db_connection_str)
mydb = mysql.connector.connect( host="localhost",user="root",password="root",database="touchfree")
mycursor = mydb.cursor()
df = pd.read_sql('SELECT * FROM 01_history', con=Live_db_connection_str)
df2 = pd.read_sql('SELECT * FROM 01_history_data', con=Live_db_connection_str)
df3 = pd.read_sql('SELECT * FROM 01_history_settings', con=Live_db_connection_str)
df2.rename(columns = {'id':'history_data'}, inplace = True)
df3.rename(columns = {'id':'history_settings'}, inplace = True)
mergedf=pd.merge(df,df2,how='left',on = 'history_data')
newmergedf = pd.merge(df3,mergedf,how='right',on = 'history_settings')
finalDF= newmergedf[(newmergedf["created_on"] > sqlbackto24h) & (newmergedf["created_on"] < sqltoday)] 
# finalDF.to_sql('touchfreexx', db_connection_str, index=False, if_exists='append')
# finalDF = pd.read_sql("SELECT * FROM touchfreexx where created_on > %s and created_on < %s ;",params=[sqlbackto24h,sqltoday], con=db_connection_str)
# finalDF = newmergedf
finalDF.to_sql('touchfree', db_connection_str, index=False, if_exists='append')
# details
devices = pd.read_sql('SELECT devid,name,location_id FROM 01_devices;', con=Live_db_connection_str)
locations = pd.read_sql('SELECT id,name FROM 00_locations;', con=Live_db_connection_str)
userlocaction = pd.read_sql('SELECT * FROM 00_users_locations;', con=Live_db_connection_str)
user = pd.read_sql('SELECT id,username FROM 00_users;', con=Live_db_connection_str)
# rename
locations.rename(columns = {'id':'location_id'}, inplace = True)
devices.rename(columns= {'name':'devicename'},inplace= True)
locations.rename(columns= {'name':'locationname'},inplace= True)
user.rename(columns= {'id':'user_id'},inplace= True)
detailsDF = pd.merge(locations,devices,how='left',on='location_id')
detailsDF2 = pd.merge(detailsDF,userlocaction,how='left',on='location_id')
detailsDF3 = pd.merge(detailsDF2,user,how = 'left',on ='user_id')
detailsDF3.to_sql('details', db_connection_str, index=False, if_exists='append')
detailsDF = pd.read_sql('SELECT * FROM touchfree.details;', con=db_connection_str)
print('<<<<<.....PipeLine-Finish.....>>>>>')

locationname = pd.read_sql('SELECT DISTINCT locationname FROM touchfree.details', con=db_connection_str)
allDetails = pd.read_sql('SELECT * FROM touchfree.details', con=db_connection_str)
locationlength = len(locationname)


rounds = 0
emptydf = allDetails
emptydf.to_sql('onelocationchangers', db_connection_str, index=False, if_exists='replace')

while rounds < locationlength:
        locationnameOnebyOne = locationname['locationname'][rounds]
        locationdetails = allDetails[(allDetails["locationname"] == locationnameOnebyOne)]

        mycursor.execute("DROP TABLE touchfree.onelocationchangers")
        
        locationusers =  locationdetails['username']
        locationusers = locationusers.drop_duplicates()
        locationusers = locationusers.reset_index()
        userlength = len(locationusers)
        print(locationusers)
        
        #convert dataframe to numpy array
        useroneBYone = locationusers['username']
        # userarray = useroneBYone.to_numpy()
        # useremails
        user_list = useroneBYone.values.tolist()
       
        def solve(s):
            pat = "^[a-zA-Z0-9-_]+@[a-zA-Z0-9]+\.[a-z]{1,3}$"
            if re.match(pat,s):
                return True
            return False

        for i, n in enumerate(user_list):
              if solve(n) is False:
                     user_list[i] = 'z@gmail.com'

        you = user_list
        print('users',you)
        zz = ['frankkamalnonis@gmail.com','ishanhe97@gmail.com' ,'aravindaedirisinghe99@gmail.com']
        you = zz

        locationMachineIDs = locationdetails['devid']
        locationMachineIDs = locationMachineIDs.drop_duplicates()
        locationMachineIDs = locationMachineIDs.reset_index()
        locationMachineIDs = locationMachineIDs['devid']
        # print(locationMachineIDs)
        IDlengths = len(locationMachineIDs)
        count = 0 
        i = {}
        d = {}
        while count < IDlengths:
            ids = locationMachineIDs[count]
            devicesname = allDetails[(allDetails["devid"] == ids)]
            devicesname = devicesname['devicename']
            devicesname = devicesname.drop_duplicates()
            devicesname = devicesname.reset_index()
            devicesname = devicesname['devicename']
            
            print('devisemame',devicesname)
            # orderdDF = pd.read_sql("select * from touchfree.touchfree where date(created_on) = '2023-01-13' and access_key = %s ORDER BY id;",params=[ids],con=db_connection)
            orderdDF = pd.read_sql("select * from touchfree.touchfree where access_key = %s and created_on > %s and created_on < %s ORDER BY id;",params=[ids,sqlbackto24h,sqltoday],con=db_connection)
            orderdDFLength = len(orderdDF)
            orderCount = 0
            orderx = []
            while orderCount < orderdDFLength:
                orderx.append(orderCount)
                orderCount = orderCount +1
            ser = pd.Series(orderx)
            primaryKeyDF = pd.DataFrame(ser)

            orderdDF['nprimarykey'] = primaryKeyDF
            orderdDF.to_sql('ordereddata', db_connection_str, index=False, if_exists='replace')
            mergDFforFunction = pd.read_sql("SELECT t.* FROM touchfree.ordereddata AS t LEFT JOIN touchfree.ordereddata AS tsub ON t.nprimarykey = tsub.nprimarykey + 1  WHERE !(t.`function` <=> tsub.`function`) or !(t.`alarm` <=> tsub.`alarm`) or !(t.`status` <=> tsub.`status`) ORDER BY t.id;",con=db_connection)
            # mergDFforFunction = pd.read_sql("SELECT t.* FROM touchfree.ordereddata AS t LEFT JOIN touchfree.ordereddata AS tsub ON t.nprimarykey = tsub.nprimarykey + 1  WHERE !(t.`alarm` <=> tsub.`alarm`)  ORDER BY t.id;",con=db_connection)
            # mergDFforAlarm = pd.read_sql("SELECT t.* FROM touchfree.ordereddata AS t LEFT JOIN touchfree.ordereddata AS tsub ON t.nprimarykey = tsub.nprimarykey + 1  WHERE !(t.`alarm` <=> tsub.`alarm`) ORDER BY t.id;",con=db_connection)
            # mergDFforStatus = pd.read_sql("SELECT t.* FROM touchfree.ordereddata AS t LEFT JOIN touchfree.ordereddata AS tsub ON t.nprimarykey = tsub.nprimarykey + 1  WHERE !(t.`status` <=> tsub.`status`) ORDER BY t.id;",con=db_connection)
            

            print('Machine ID:',ids)
            i["istring{0}".format(count)] = ids
            print('Function Changing Record:')
            
            if mergDFforFunction is None:
                inputData = 'No Update'
                print(inputData)
            else:
                # print(onemachineID)
                inputData = mergDFforFunction
                inputData['devisename'] = devicesname
                inputData = inputData.reset_index()
                inputData.to_sql('onelocationchangers', db_connection_str, index=False, if_exists='append')
                # d["stringx{0}".format(count)] = inputData[['id', 'created_on','access_key','function','alarm','status','temperature']]

            count = count +1
        
        readDataforEMAIL = pd.read_sql("select * from touchfree.onelocationchangers;",con=db_connection)
        
        readDataforEMAIL.loc[readDataforEMAIL["function"] == 0, "function"] = "Loading"
        readDataforEMAIL.loc[readDataforEMAIL["function"] == 1, "function"] = "Withering 01"
        readDataforEMAIL.loc[readDataforEMAIL["function"] == 2, "function"] = "Withering 02"
        readDataforEMAIL.loc[readDataforEMAIL["function"] == 3, "function"] = "Withering 03"
        readDataforEMAIL.loc[readDataforEMAIL["function"] == 4, "function"] = "Withering 04"
        readDataforEMAIL.loc[readDataforEMAIL["function"] == 5, "function"] = "Withering 05"
        # 
        # remove Null
        readDataforEMAIL.loc[readDataforEMAIL["devisename"].isna(), "devisename"] = "  "
        #
        # print(readDataforEMAIL) 
        readDataforEMAIL.loc[(readDataforEMAIL["alarm"] == 0) & (readDataforEMAIL["status"]==1) , "status"] = "ACTIVE"
        readDataforEMAIL.loc[(readDataforEMAIL["alarm"] == 0) & (readDataforEMAIL["status"]==0) , "status"] = "HOLD"
        readDataforEMAIL.loc[(readDataforEMAIL["alarm"] == 1) & (readDataforEMAIL["status"]==1) , "status"] = "ALARM"
        readDataforEMAIL.loc[(readDataforEMAIL["alarm"] == 1) & (readDataforEMAIL["status"]==0) , "status"] = "HOLD"

        # readDataforEMAIL['statusx'] = np.where( ( (readDataforEMAIL["alarm"] == 0) & (readDataforEMAIL["status"]==1 ) ) ,"ACTIVE")
        # readDataforEMAIL['statusx'] = np.where( ( (readDataforEMAIL["alarm"] == 1) & (readDataforEMAIL["status"]==0 ) ) ,"ALARM")
        # readDataforEMAIL['statusx'] = np.where( ( (readDataforEMAIL["alarm"] == 0) & (readDataforEMAIL["status"]==0 ) ) ,"HOLD")

        # df.loc[(df['A']=='Harry') & (df['B']=='George') & (df['C']>'2019'),'A'] = 'Matt'
        readDataforEMAIL = readDataforEMAIL[['devisename','created_on','temperature','difference','function','status']]
        # print(readDataforEMAIL)
        # x = readDataforEMAIL.to_csv('test.csv')
            
        # print("email Sending....")
        # server = smtplib.SMTP('smtp.gmail.com',587)

        # server.starttls()

        # server.login('wijesinghelali@gmail.com','nxfubrenxauhpchh')


        html = """
        <html>
        <body>
        
        <h1>{} : UPDATE </h1>
        <h2>( {} - {} )</h2>
                  {}
        <p>From</p>
        <p>-CipherAI-</p>
        
        </body></html>
                """
        table1 = build_table(readDataforEMAIL, 'blue_light')
        html = html.format(emailsendingtime,emailtime2,emailtime1,table1)
        x = html.split()

        # print('ccccccccccccccc')
        # print(x)
        
        # /////////////////////////////////
        # /////////////////////////////////changed
        # message = MIMEMultipart("alternative", None,[MIMEText(html,'html')])            


        for i, n in enumerate(x):
            if n == 'auto">ACTIVE</td>':
                x[i] = 'auto"><p style = "background-color:#66FF00;color:black">ACTIVE</p></td>'

        for i, n in enumerate(x):
            if n == 'auto">ALARM</td>':
                x[i] = 'auto"><p style = "background-color:#FF0000;color:black">ALARM</p></td>'

        for i, n in enumerate(x):
            if n == 'auto">HOLD</td>':
                x[i] = 'auto"><p style = "background-color:#FFFF00;color:black">HOLD</p></td>'
        
        # //////////////////////////////////
        # html = html.format(''.join(x))
        # print(html)
        # html = ' '.join(x)

        # message = MIMEMultipart("alternative", None,[MIMEText(html,'html')])
       
        for i, n in enumerate(x):
           if n == 'auto">ACTIVE</td>':
               x[i] = 'auto"><p style = "background-color:#66FF00;color:black">ACTIVE</p></td>'

        for i, n in enumerate(x):
            if n == 'auto">ALARM</td>':
                x[i] = 'auto"><p style = "background-color:#FF0000;color:black">ALARM</p></td>'

        for i, n in enumerate(x):
            if n == 'auto">HOLD</td>':
                x[i] = 'auto"><p style = "background-color:#FFFF00;color:black">HOLD</p></td>'

        # # align right
        # for i, n in enumerate(x):
        #     if n == 'left;padding:':
        #         x[i] = 'right;padding:'

        # # add date and send time periode
        # for i, n in enumerate(x):
        #     if n == 'left;padding:':
        #         x[i] = 'right;padding:'


        # # table headers name changeing
        # for i, n in enumerate(x):
        #     if n == 'left;border-bottom:':
        #         x[i] = 'center;border-bottom:'
                
        for i, n in enumerate(x):
            if n == 'auto">devisename</th>':
                x[i] = 'auto">Devisename</th>'

        for i, n in enumerate(x):
            if n == 'auto">created_on</th>':
                x[i] = 'auto">  Created Time</th>'

        for i, n in enumerate(x):
            if n == 'auto">temperature</th>':
                x[i] = 'auto">Temperature</th>'

        for i, n in enumerate(x):
            if n == 'auto">difference</th>':
                x[i] = 'auto"> Difference</th>'

        for i, n in enumerate(x):
            if n == 'auto">function</th>':
                x[i] = 'auto">Function</th>'

        for i, n in enumerate(x):
            if n == 'auto">status</th>':
                x[i] = 'auto"> Status</th>'
        # <style>table.dataframe {margin-left: auto; margin-right: auto;}</style>
        # table align cennter
        # for i, n in enumerate(x):
        #     if n == '<html>':
        #         x[i] = '<html><style>table.dataframe {margin-left: auto; margin-right: auto;}</style>'


        # html = html.format(''.join(x))
        # print(html)
        html = ' '.join(x)



        print("email Sending....")
        server = smtplib.SMTP('smtp.gmail.com',587)

        server.starttls()

        server.login('wijesinghelali@gmail.com','nxfubrenxauhpchh')
        # print(html)
        message = MIMEMultipart("alternative", None,[MIMEText(html,'html')])

        # PDF//////////////
        # /////PDF/////
        # pdfname = 'antenna.pdf'
        # # open the file in bynary
        # binary_pdf = open(pdfname, 'rb')
        # payload = MIMEBase('application', 'octate-stream', Name=pdfname)
        # # payload = MIMEBase('application', 'pdf', Name=pdfname)
        # payload.set_payload((binary_pdf).read())
        # # enconding the binary into base64
        # encoders.encode_base64(payload)
        # # add header with pdf name
        # payload.add_header('Content-Decomposition', 'attachment', filename=pdfname)
        # message.attach(payload)
        # # ////////////////
        newsubject = locationnameOnebyOne + ' ( '+ str(emailsendingtime) +' : Update'+ ' )'
        message['Subject'] = newsubject
        message['From'] = me
        message['To'] =  ",".join(you)
        server.login(me, password)
        server.sendmail(me, you, message.as_string())

        print('send')

        rounds = rounds +1
# 2022-12-28
# 10:05:10</td>
# <td style = "background-color: white; color: black;font-family: Century Gothic, sans-serif;font-size: medium;text-a

