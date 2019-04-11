import requests
import dateutil.parser
from datetime import datetime, timedelta
from dateutil.tz import tzlocal
import pandas as pd
import calendar
from io import StringIO
from azure.storage.blob import (
    BlockBlobService
)
accountName = accountname #your Azure blob storage account name
accountKey = accountKey #your Azure blob storage account key 
containerName = containerName #your Azure blob storage container name
blobService = BlockBlobService(account_name=accountName, account_key=accountKey)

date = (datetime.now(tzlocal()))
until = int(datetime.timestamp(date)) - 86400
since = until - 86400
df = pd.DataFrame ()

params = (
    ('metric', 'impressions,reach,follower_count,website_clicks'),
    ('period', 'day'),
    ('access_token', access_token), #insert your generated access token
    ('since', since),
    ('until', until)
)
response = requests.get('https://graph.facebook.com/v3.2/{instagram_business_account_id}/insights', params=params)
data = response.json()
impressions = data['data'][0]['values'][0]['value'] #impressions data
account = (data['data'][0]['id']).split("/")[0] #instagram business account id. We split the string to keep only the id
reach = data['data'][1]['values'][0]['value'] #reach data
followers = data['data'][2]['values'][0]['value'] #new followers acquired
website_click = data['data'][3]['values'][0]['value'] #website clicks made
end_time = data['data'][0]['values'][0]['end_time'] #getting the data end time

d = dateutil.parser.parse(end_time) #parsing the timestamp obtained from the API call
timing = d.strftime('%Y-%m-%d %H:%M:%S') 
time_end = datetime.strptime(timing, '%Y-%m-%d %H:%M:%S') #converting string to datetime
end_time_epoch = int(datetime.timestamp(time_end)) #converting datetime to epoch time
day = datetime.strptime(timing, '%Y-%m-%d %H:%M:%S') - timedelta(days=1) #removing 1 day to get the day of reporting
current_month = day.strftime('%Y-%m') #getting the current month and year for use with cost data
cal_year = int(day.strftime('%Y')) #getting the calendar year for finding the number of days in the month
cal_month = int(day.strftime('%m')) #getting the calendar month for finding the number of days in the month

params = (
    ('fields', 'timestamp'),
    ('limit', '100'), #assuming that no more than 100 tags are made in a day
    ('access_token', access_token), #insert your generated access token
)
response = requests.get('https://graph.facebook.com/v3.2/{instagram_business_account_id}/tags', params=params)
data = response.json()
c=0 #counter variable
for i in data['data']:
    timestamp = i['timestamp']
    d = dateutil.parser.parse(timestamp)
    timing = d.strftime('%Y-%m-%d %H:%M:%S')
    time_publish = datetime.strptime(timing, '%Y-%m-%d %H:%M:%S')
    time_publish = int(datetime.timestamp(time_publish)) #Converting published time to epoch time format

    if time_publish <= end_time_epoch and (time_publish >= end_time_epoch - 86400): #Checking if the post was made within the reporting period. The 86400 removes one day from the time
        c = c+1 #adding 1 to count the number of tags
mentions = c    
csv = requests.get(r"http://site-ftp.site.com/test/cat/account_cost.csv") #reading cost data from ftp source
cost_data = pd.read_csv(StringIO(csv.text)) 
for index, row in cost_data.iterrows():
    if row['Month'] == current_month and row['Account'] == account: #checking if month and account id match
        cost= round((row['Cost'])/(calendar.monthrange(cal_year,cal_month)[1]),2)  #dividing month cost data evenly to make it daily 
df1 = [day,account,impressions,reach,followers,website_click,cost,mentions] #creating the dataframe
headers = ['day','account','impressions','reach','followers','website_click','cost','mentions']
df2 = pd.DataFrame ([df1])
df = df.append(df2) #adding the dataframe
output = df.to_csv (encoding = "utf-8",index=False,header=headers) 
blobName = 'Instagram_organic.csv'
blobService.create_blob_from_text(containerName,blobName, output) #writing to Azure blob storage
print (df)
