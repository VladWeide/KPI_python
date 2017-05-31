
import os
import requests
import datetime
import getpass

import pandas as pd

path = os.path.dirname(os.path.realpath(__file__))
os.chdir(path)

from debug import Debug

###################################################

def construct_row_from_source_line(line):
    """ converts source line to csv format """
    
    try:
        year = line[:4]
        week = line[5:7]
        
        line = line[7:].strip()
        
        items_list = line.split(',')
        
        SMN = items_list[0].strip()
        SMT = items_list[1].strip()
        VCI = items_list[2].strip()
        TCI = items_list[3].strip()
        VHI = items_list[4].strip()
        
    except:
        return None
    
    return year, week, SMN, SMT, VCI, TCI, VHI

################################################### 
    

def get_data(year1='1981',
             year2='2017',
             province_id=1):
    """ fetch text data from web and save
        structured lines to file """
       
    try:
        
        # int to string
        year1 = str(year1)
        year2 = str(year2)
        province_id = str(province_id)
        
        # construct url using params
        url = 'https://www.star.nesdis.noaa.gov/smcd/emb/'
        url += 'vci/VH/get_provinceData.php?country=UKR&'
        url += 'provinceID='
        url += province_id
        url += '&year1='
        url += year1
        url += '&'
        url += 'year2='
        url += year2
        url += '&type=Mean'
        
    
        # get web page text
        r = requests.get(url, timeout=(60,60))
        txt = str(r.content)
        txt = txt.replace('\\n', '\n')
        
        # construct file name
        fname = 'vhi_id_' + province_id + '_'
        fname += year1 + '_' + year2 + '__'
        today = datetime.datetime.today()
        year = today.year
        month = today.month
        day = today.day
        hour = today.hour
        minute = today.minute
        sec = today.second
        
        month = str(month)
        if len(month) == 1:
            month = '0' + month
            
        day = str(day)
        if len(day) == 1:
            day = '0' + day
            
        hour = str(hour)
        if len(hour) == 1:
            hour = '0' + hour
            
        minute = str(minute)
        if len(minute) == 1:
            minute = '0' + minute
            
        sec = str(sec)
        if len(sec) == 1:
            sec = '0' + sec
            
        fname += 'loaded_date_' + str(year) + '-' 
        fname += month + '-' 
        fname += day + '_time_'
        fname += hour + '-'
        fname += minute + '-'
        fname += sec + '.csv'
        
        # save data to file
        f = open(fname, 'w')
        f.write('year,week,SMN,SMT,VCI,TCI,VHI\n')
        
        txt_lines = txt.split('\n') 
        
        for line in txt_lines:
            
            # get values from line            
            items = construct_row_from_source_line(line) 
            if not items: # if it is None
                continue
            
            year, week, SMN, SMT, VCI, TCI, VHI = items
               
            # skip wrong lines             
            try:
                if not 1981 <= int(year) <= 2017:
                    continue
            except:
                continue
            
            res_line = ''
            
            for item in items:
                res_line += item + ','
            res_line = res_line[:-1] # delete last comma
            
            f.write(res_line + '\n')
        
        f.close()
        
        return True
    
    except:
        Debug.print_exception_info()
        return False
    
###################################################

def read_data_to_dataframe(fname):
    """ loads the values from csv 
        to pandas dataframe
    """

    try:
        f = open(fname, 'r')
        lines = f.readlines()
        f.close()
       
        d = dict()
        
        d['year'] = []
        d['week'] = []
        d['SMN'] = []
        d['SMT'] = []
        d['VCI'] = []
        d['TCI'] = []
        d['VHI'] = []
        
        for line in lines[1:]:
            items = line.split(',')
            d['year'].append(int(items[0]))
            d['week'].append(int(items[1]))
            d['SMN'].append(float(items[2]))
            d['SMT'].append(float(items[3]))
            d['VCI'].append(float(items[4]))
            d['TCI'].append(float(items[5]))
            d['VHI'].append(float(items[6]))
            
        df = pd.DataFrame(d, 
                          columns=['year', 'week', 'SMN',
                                   'SMT', 'VCI', 'TCI', 'VHI'])
        
    except:
        Debug.print_exception_info()
        return None
                     
    return df
    
###################################################

def replace_province_id(x):
    
    if x == 1: # cherkasi
        return 22
    elif x == 2:
        return 24 # chernigiv
    elif x == 3:
        return 23 # chernivzi
    elif x == 4:
        return 25 # crim
    elif x == 5:
        return 3 # dnepr
    elif x == 6:
        return 4 # donezk
    elif x == 7:
        return 8 # ivano-frankivsk
    elif x == 8:
        return 19 # charkiv
    elif x == 9:
        return 20 # cherson
    elif x == 10:
        return 21 # chmelnizkiy
    elif x == 11:
        return 9 # kiyiv
    elif x == 12:
        return 9 # kiyiv (city)
    elif x == 13:
        return 10 # kirovograd
    elif x == 14:
        return 11 # lugansk
    elif x == 15:
        return 12 # lviv
    elif x == 16:
        return 13 # mikolayiv
    elif x == 17:
        return 14 # odesa
    elif x == 18:
        return 15 # poltava
    elif x == 19:
        return 16 # rivne
    elif x == 20:
        return 25 # crim (sevastopol)
    elif x == 21:
        return 17 # sumi
    elif x == 22:
        return 18 # ternopil
    elif x == 23:
        return 6 # zakarpat'e
    elif x == 24:
        return 1 # vinniza
    elif x == 25:
        return 2 # volin
    elif x == 26:
        return 7 # zaporizh'e
    elif x == 27:
        return 5 # zhitomir
    
###################################################

def get_year_vhi_min_max(df, year):
    
    try:
        year = int(year)
    except:
        Debug.print_exception_info()
        return None, None
    
    df = df[(df['year'] == year)]
    
    if df.empty == False: # if is not empty
        VHI = df['VHI']
        return min(VHI), max(VHI)
    else:
        return None, None
    
###################################################
    
def get_severe_drought_percentage(df):
    
    
    df2 = df[(df['VHI'] < 15)]
    
    if df2.empty == False: # if is not empty
    
        VHI1 = df['VHI'] # all
        VHI2 = df2['VHI'] # severe draught
        
        return 100 * len(VHI2) / len(VHI1) # %
    else:
        return None
    
###################################################

def get_moderate_drought_percentage(df):
    
    
    df2 = df[(15 < df['VHI'] < 35)]
    
    if df2.empty == False: # if is not empty
    
        VHI1 = df['VHI'] # all
        print(VHI1)
        VHI2 = df2['VHI'] # severe draught
        print(VHI2)
        return 100 * len(VHI2) / len(VHI1) # %
    else:
        return None

###################################################

def get_vhi_given_range(df, from_, to):
    
    
    df = df[(from_ < df['VHI'] < to)]
    
    return df['VHI']

###################################################



    
 #GET DATA
res = get_data(year1=2016, year2=2017)
if res:
    print('all is ok')
else:
    print('program was aborted (exception)')   
 
# READ DATA TO DATAFRAME
#if getpass.getuser() == 'root':
#    fname = 'vhi_id_1_2016_2017__loaded_date_2017-05-16_time_00-30-33.csv'
#else:
#    fname = 'vhi_id_1_2016_2017__loaded_date_2017-05-15_time_23-47-28.csv'

#df = read_data_to_dataframe(fname)
#print(list(df.columns.values))
#print(df[:10])
  
#print()
#print('------------------')

# GET MIN, MAX VHI FROM GIVEN YEAR
#print(get_year_vhi_min_max(df, '2016'))
#print(get_year_vhi_min_max(df, '2017'))

# GET VHI RANGE
#print()
#get_vhi_given_range(df, 0, 100)

# GET SEVERE DRAUGHT PERCENTAGE
#print('severe draught percentage: ', 
#      get_severe_drought_percentage(df))

# GET MODERATE DRAUGHT PERCENTAGE
#print('moderate draught percentage: ', 
#      get_severe_drought_percentage(df))






















