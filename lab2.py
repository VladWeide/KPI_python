
import os
import datetime
import requests
import pandas as pd

from spyre import server

from debug import Debug
from data_serializer import DataSerializer as ds

df = ds.deserialize('df1')

def construct_row_from_source_line(line):
    """ converts source line to csv format """
    
    try:
        year = line[:4]
        week = line[5:7]
        
        line = line[7:].strip()
        
        items_list = line.split(',')
        
        result_list = [year, week]
        
        for item in items_list:
            result_list.append(item.strip())
        
    except:
        return None
    
    return result_list


def format_date(date_part):
    """ if the length of date part is 1, 
        we add prefix '0' 
        returns -1 if """
        
    date_part = str(date_part)
    
    ln = len(date_part)
    
    if ln == 2:
        return date_part
    else:
        if ln == 1:
            return '0' + date_part
        else:
            print('input error in format_date function')
            return -1
        
def get_data(year1='1981',
             year2='2017',
             province_id=1,
             file_type=1):
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
        
        if file_type == 1:
            url += '&type=Mean'
            
        else:
            url += '&type=VHI_Parea'           
            
    
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
        
        # format date parts        
        month = str(month)
        month = format_date(month)        
        day = str(day)
        day = format_date(day)       
        hour = str(hour)
        hour = format_date(hour)       
        minute = str(minute)
        minute = format_date(minute)        
        sec = str(sec)
        sec = format_date(sec)
            
        fname += '_fileType' + str(file_type)
        fname += '_loaded_date_' + str(year) + '-' 
        fname += month + '-' 
        fname += day + '_time_'
        fname += hour + '-'
        fname += minute + '-'
        fname += sec + '.csv'
        
        # save data to file
    
        f = open(fname, 'w')
        
        txt_lines = txt.split('\n') 
        
        if file_type == 1:                
                
            head = 'year,week,SMN,SMT,VCI,TCI,VHI\n'
                      
        else:
            head = 'year,week,p0,p5,p10,p15,p20,p25,p30,p35,'
            head += 'p40,p45,p50,p55,p60,p65,p70,p75,p80,p85,'                
            head += 'p90,p95,p10\n'               
            
        f.write(head)
        
        for line in txt_lines[1:-1]:
            
            # get values from line

            items = construct_row_from_source_line(line)

            if not items: # if it is None
                continue      
               
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

def read_data_to_dataframe(fname, file_type=1):
    """ loads the values from csv 
        to pandas dataframe
    """
    
    if file_type == 1:
        columns_list = ['year', 'week', 'SMN', 'SMT', 
                        'VCI', 'TCI', 'VHI']
    
    else:
        columns_list = ['year','week','p0','p5','p10','p15','p20',
                        'p25','p30','p35', 'p40','p45','p50','p55',
                        'p60','p65','p70','p75','p80','p85','p90',
                        'p95','p100']

    try:
        f = open(path + '/' + fname, 'r')
        lines = f.readlines()
        f.close()
       
        d = dict()
        
        for c in columns_list:
            d[c] = []

        
        for line in lines[1:]:
            
            items = line.split(',')
            
            for i in range(len(columns_list)):
                if i < 2:
                    d[columns_list[i]].append(int(items[i].strip()))
                else:
                    d[columns_list[i]].append(float(items[i].strip()))
                    
                    
        df = pd.DataFrame(d, 
                          columns=columns_list)
        
    except:
        Debug.print_exception_info()
        return None
                     
    return df
    
###################################################



class SimpleApp(server.App):
    title = "Simple App"
    inputs = [{
        "type": "text",
        "key": "words",
        "label": "write week amonunt here",
        "value": "10",
        "action_id": "simple_html_output"
    }]

    outputs = [{
        "type": "html",
        "id": "simple_html_output"
    }]

    def getHTML(self, params):
        words = params["words"]
        msg =  "week amount from start point: <b>%s</b>" % words
        weeks  = int(words)

        
        list = "<br><br><br>"
        list += "<form hight=500 id=1 name=1 "
        list += "action='http://127.0.0.1:8080' method=GET>"



        list += '<select size=-1 hight=200 name=province size=55>'
        list +='<option selected value=1>Вінніца</option>'
        list +='<option value=2>Волин</option>'
        list +='<option value=3>Дніпр</option>'
        list +='<option value=4>Донецьк</option>'
        list +='<option value=5>Житомір</option>'
        list +="<option value=6>Закарпат'є</option>"
        list +="<option value=7>Запорож'є</option>"
        list +='<option value=8>Івано-Франківськ</option>'
        list +='<option value=9>Київ</option>'
        list +='<option value=10>Кіровоград</option>'
        list +='<option value=11>Луганск</option>'
        list +='<option value=12>Лвів</option>'
        list +='<option value=13>Миколаїв</option>'
        list +='<option value=14>Одеса</option>'
        list +='<option value=15>Полтава</option>'
        list +='<option value=16>Рівне</option>'
        list +='<option value=17>Суми</option>'
        list +='<option value=18>Тернопіл</option>'
        list +='<option value=19>Харків</option>'
        list +='<option value=20>Херсон</option>'
        list +='<option value=21>Хмельницькій</option>'
        list +='<option value=22>Черкасы</option>'
        list +='<option value=23>Чернівці</option>'
        list +='<option value=24>Чернiгів</option>'
        list +='<option value=25>Крім</option>'
        list += '</select>'
        
        list += '<input type="submit" value="send">'
        
        list += "</form>"
        
#        exists = 0
#        fname1 = ''
#    
#        
#        files = os.listdir()
#        for ff in files:
#            if ff.startswith('vhi_id'):
#                exists += 1
#                if 'fileType1' in ff:
#                    fname1 = ff
#            
#        
#        get_data(year1=2016, year2=2017, file_type=1)
#        df1 = read_data_to_dataframe(fname1)
        
#        list += '<pre>' + str(df1[:10])_+ "</pre>"

#        list += '<pre>'                              
#
#        list += str(df[10])
#        
#        list += '</pre>'
        
        
#        df = ds.deserialize('df1')
#        
#        list += '<pre>' + str(head(df)) + "</pre>"
#        
#        
#        table  = '<table width=100% cellpadding=0 cellspacing=1>'
#        for i in range(weeks):
#            table += '<tr><td>'+str(df['VCI'][i]) + '</td></tr>'
#        table = '</table>'
#        
#        list += '<br><br>'  + table
#        
        
        
        
        return msg + list
    
    
    

app = SimpleApp()
app.launch(port=8080)
