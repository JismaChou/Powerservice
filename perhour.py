from powerservice import trading
import pandas as pd
import numpy as np
from datetime import datetime
import csv




trades_today = trading.get_trades(date='03/03/2022')
trades_yesterday = trading.get_trades(date='02/03/2022')

initial = pd.DataFrame(trades_today[1])

times = pd.to_datetime(initial.time)

intermediate = initial.groupby([times.dt.hour]).agg(date=('date', 'max'), volume=("volume", "sum"), id=('id', 'max'))

output = intermediate.reset_index()[['date', 'time', 'volume', 'id']]

#output2 = output[['time', 'volume']]
#output2['time'] = output2['time'].astype(pd.StringDtype())
#output2['time']= output2.time.str.extract("(^\d+).", expand=False)+":00"

#convert time column to datetime format
output['time'] = pd.to_datetime(output.time, format='%H')
#format time column to 24 hour (HH:MM)
output['time'] = output['time'].dt.strftime('%H:%M')
#shift last row to the first
output=output.apply(np.roll, shift=1)

#select only 2 columns from current dataframe
output2 = output[["time","volume"]]

#get current date/time and create file name 
dateTimeObj = datetime.now()
dt_string = dateTimeObj.strftime("%Y%m%d_%H%M")
file_name1="PowerPosition_" + dt_string + ".csv"
#print(file_name)
#output2['time'] = pd.to_datetime(output2.time, format='%H')
#output2['time'] = output2['time'].dt.strftime('%HH:%MM')
#print(output2.head(24))
output2.to_csv(file_name1,  index=False)
datetime_series = pd.to_datetime(initial['time'])
datetime_index = pd.DatetimeIndex(datetime_series.values)
df3=initial.set_index(datetime_index)

df4 = df3.resample('5T').mean()
df3=df3.set_index(df4.index)
df3=df3.set_index(df4.index).reset_index()
df3 = df3.fillna(0)
df_quality1= df3[df3['time'] == 0]
df_quality1['index'] = df_quality1['index'].dt.strftime('%H:%M')

df_quality1.rename(columns = {'index':'missed_intervals'}, inplace = True)
df_quality1 = df_quality1[["date","missed_intervals","id"]]

file_name="PowerPosition_" + dt_string + "_data_quality.csv"

fields=['<< TIME INTERVAL CHECK >>']
with open(file_name, 'a') as f:
     writer = csv.writer(f)
     writer.writerow(fields)
     
     #export csv file
df_quality1.to_csv(file_name, mode='a', index=False)

df_quality2=df3[(df3['volume'] == 0) & (df3['time'] != 0)]
df_quality2 = df_quality2[["date","time","volume","id"]]

fields=['<< MISSING VALUES CHECK >>']
with open(file_name, 'a') as f:
     writer = csv.writer(f)
     writer.writerow(fields)

#export csv file
df_quality2.to_csv(file_name, mode='a', index=False)
#df_quality1.to_csv(file_name, index=False)

#print(datetime_series)

#print(datetime_index)
#print(df4.describe())

if output2.time[0] == '23:00' and output2.time[23] == '22:00':
    # print('START AND END TIME: CORRECT')
     correct_format = 'START AND END TIME: CORRECT'
else:
     #print('START AND END TIME: INCORRECT')
     correct_format='START AND END TIME: INCORRECT'



with open(file_name, 'a') as f:
     writer = csv.writer(f)
     writer.writerow(correct_format)


try:
    pd.to_datetime(output2['time'], format='%H:%M', errors='raise')
    print('Valid')
    correct_time = 'TIME FORMAT: VALID'
except ValueError:
    print('Invalid')
    correct_time = 'TIME FORMAT: INVALID'

with open(file_name, 'a') as f:
     writer = csv.writer(f)
     writer.writerow(correct_time)


file_name2="PowerPosition_" + dt_string + "_data_profiling.csv"
fields=file_name1
with open(file_name2, 'a') as f:
     writer = csv.writer(f)
     writer.writerow(fields)