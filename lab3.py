# -*- coding: utf-8 -*-

import os
import time
import datetime
import getpass
import random

import numpy as np
import pandas as pd

path = os.path.dirname(os.path.realpath(__file__))
os.chdir(path)

from data_serializer import DataSerializer as ds

###################################################

def get_date_time_from_strings(date, time_):
    
    date_lst = date.split('/')
    time_lst = time_.split(':')
    
    year = int(date_lst[2])
    month = int(date_lst[1])
    day = int(date_lst[0])
    
    hour = int(time_lst[0])
    minute = int(time_lst[1])
    sec = int(time_lst[2])
    
    return datetime.datetime(year, month, day, hour, minute, sec)

##################################################################

def fill_dictionary_from_file(lines):
    
    bad_counter = 0
    bad_lines = []
    
    head = lines[0]
    headers = head.split(';')
    w = len(headers)
    
    d = dict() # result dictionary
    
    for i in range(w):
        headers[i] = headers[i].strip()
        d[headers[i]] = []
    
    for line in lines[1:]:
        
        bad = False
        
        items = line.split(';')
        
        # check the line items 
        for i in items[2:]:
            try:
                i = float(i.strip())
            except:
                bad = True
                break
        
        # skip bad lines
        if bad:
            bad_counter += 1
            bad_lines.append(line)
            continue       
       
        for i in range(w):
                
            if 'Date' in headers[i] or 'Time' in  headers[i]:
                d[headers[i]].append(items[i].strip())
            else:                
                d[headers[i]].append(float(items[i].strip()))
                
                
    return d, bad_counter, bad_lines

#############################################################


usr = getpass.getuser()
if usr == 'vlad':
    data_path = '/home/vlad/data/'
elif usr == 'root':
    data_path = '/root/data/'
else:
    data_path = 'c:/data/'
    
fname = 'household_power_consumption.txt'

# get data
#f = open(data_path + fname, 'r')
#lines = f.readlines()
#f.close()
         
#d, bad_counter, bad_lines = fill_dictionary_from_file(lines)         

d = ds.deserialize(data_path + 'lab3_d2')


# CREATE PANDAS DATAFRAME

df = pd.DataFrame(d, 
                  columns=list(d.keys()))
  
#################################################
                
# CREATE NUMPY ARRAY

# first, create list of lists
lst = []
rows = len(d['Time'])
cols = len(list(d.keys()))

for i in range(rows):
    lst.append([])
    for key in d.keys():
        lst[i].append(d[key][i])
        
# create numpy array from list of lists  
data_str = np.array(lst) #  (type == str)

# separate date, time and float data
date_time_str = data_str[:, 0:2]
float_data_str = data_str[:, 2:]

#  type == float
float_data = np.empty((rows, cols - 2), dtype=float)
for i in range(rows):
    # create float list
    lst_float = []
    for j in range(cols - 2):
        lst_float.append(float(float_data_str[i,j]))
    float_data[i,:] = lst_float
    

date_time = np.empty((rows, 1), dtype=datetime.datetime)

for i in range(rows):
    date = date_time_str[i, 0]
    time_ = date_time_str[i, 1] 
    date_time[i] = get_date_time_from_strings(date, time_)

# add datetime column after string cols 'Date', 'Time'
df['datetime'] = date_time
df_cols = list(df.columns.values)
cols = df_cols[:2]
cols += ['datetime']
cols += df_cols[2:-1]
print(cols)

  
print(df[:10])

print()
print('---------------------')
print(data_str[:10])
print()

print('date time -------------------')
print(date_time_str[:10])
print()

print('date time datetime.datetime')
print(date_time[:10])

print('float data -------------------')
print(float_data[:10])

  
#####################################################

title  =  """ TASKS: """
              
task1 = """
1) Обрати всі домогосподарства, у яких 
   загальна активна споживана потужність 
   перевищує 5 кВт."""
   
# with pandas
tt = time.time()
res1_df = df[df['Global_active_power'] > 5]
time_res1_df = time.time() - tt
              
# with numpy array
tt = time.time()
res1_np = float_data[float_data[:, 0] > 5]
time_res1_np = time.time() - tt
                        
print()
print('--- 111 --------------------')
print()
print(len(res1_df))
print(len(res1_np))
print(time_res1_df)
print(time_res1_np)
print(time_res1_df / time_res1_np) # df time / numpy time fraction

# -------------------------------------------
   
task2 = """
2) Обрати всі домогосподарства, у яких 
   вольтаж перевищую 235 В."""
   
# with pandas
tt = time.time()
res2_df = df[df['Voltage'] > 235]
time_res2_df = time.time() - tt
                        
# with numpy array
tt = time.time()
res2_np = float_data[float_data[:, 2] > 235]
time_res2_np = time.time() - tt

print()
print('--- 222 --------------------')
print()
print(len(res2_df))
print(len(res2_np))
print(time_res2_df)
print(time_res2_np)
print(time_res2_df / time_res2_np) # df time / numpy time fraction

# -------------------------------------------
   
task3 = """
3) Обрати всі домогосподарства, у яких 
   сила струму лежить в межах 19-20 А, 
   для них виявити ті, у яких 
   пральна машина та холодильних 
   споживають більше, ніж 
   бойлер та кондиціонер."""
   
   # & df['Global_intensity'] <= 20
# with pandas

tt = time.time()
res3_df = df[df['Global_intensity'] >= 19]
res3_df = res3_df[res3_df['Global_intensity'] <=20]
time_res3_df = time.time() - tt
                        
# with numpy array

tt = time.time()
res3_np = float_data[float_data[:, 3] >= 19]
res3_np = res3_np[res3_np[:, 3] <= 20]
time_res3_np = time.time() - tt
                   
print()
print('--- 333 --------------------')
print()
print(len(res3_df))
print(len(res3_np))
print(time_res3_df)
print(time_res3_np)
print(time_res3_df / time_res3_np)   # df time / numpy time fraction   

# -------------------------------------------
   
task4 = """
4) Обрати випадковим чином 500000 домогосподарств 
  (без повторів елементів вибірки), для них обчислити 
  середні величини усіх 3-х груп споживання електричної енергії """
   
# with pandas

N = 5000 # replace by 500000 later

res4_df = df.sample(N)

# with numpy array
res4_np = np.array(random.sample(list(float_data), N))

print()
print('--- 444 --------------------')
print()
print(len(res4_df))

tt = time.time()
print('df Sub_metering_1 mean', res4_df['Sub_metering_1'].mean())
print('df Sub_metering_2 mean', res4_df['Sub_metering_2'].mean())
print('df Sub_metering_3 mean', res4_df['Sub_metering_3'].mean())
time_res4_df = time.time() - tt

tt = time.time()
print(len(res4_np))
print('np Sub_metering_1 mean', np.mean(res4_np[:, -3]))
print('np Sub_metering_2 mean', np.mean(res4_np[:, -2]))
print('np Sub_metering_3 mean', np.mean(res4_np[:, -1]))
time_res4_np = time.time() - tt

print()
print(time_res4_df)
print(time_res4_np)
print(time_res4_df / time_res4_np) # df time / numpy time fraction

## -------------------------------------------
   
task2 = """
5) 5. Обрати ті домогосподарства, які після 18-00 
      споживають понад 6 кВт за хвилину в середньому, 
      серед відібраних визначити ті, у яких основне 
      споживання електроенергії у вказаний проміжок часу 
      припадає на пральну машину, сушарку, холодильник 
      та освітлення (група 2 є найбільшою), а потім 
      обрати кожен третій результат із першої половини 
      та кожен четвертий результат із другої половини."""
  
# get hours list
dt = [x[0] for x in date_time]
hours = np.array([x.hour for x in dt])
df['hours'] = hours

  
# with pandas

tt = time.time()
res5_df = df[df['hours'] > 18]
res5_df = res5_df[res5_df['Global_active_power'] > 6]
res5_df = res5_df[res5_df['Sub_metering_2'] > res5_df['Sub_metering_1']]
res5_df = res5_df[res5_df['Sub_metering_2'] > res5_df['Sub_metering_3']]
time_res5_df = time.time() - tt
                        

# with numpy array

res5_np = float_data[hours > 18]
res5_np = res5_np[res5_np[:, 0] > 6]
res5_np = res5_np[res5_np[:, -2] > res5_np[:, -1]]
res5_np = res5_np[res5_np[:, -2] > res5_np[:, -3]]
time_res5_np = time.time() - tt


print()
print('--- 555 --------------------')

print()
print(len(res5_df))
print(len(res5_np))
print(time_res4_df)
print(time_res4_np)
print(time_res4_df / time_res4_np)  # df time / numpy time fraction


















            
        
    
    
    
    
