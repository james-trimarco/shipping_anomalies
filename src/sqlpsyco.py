import psycopg2
import requests
import re
from lxml import html
import csv

def content_clean(content):
    '''remove unicode characters'''

    joined = " ".join(content)
    content_ascii = re.sub(r'[^\x00-\x7f]',r'', joined).replace('\r', '').replace('\n', '').replace('\t', '')

    return content_ascii

def get_content(url):
    '''retrieve article content and add it to row, using novetta  csv schema'''
    
    #add "content" to columns if it isn't already there

  
    page = requests.get(url)
    
    try: #handle HTTP request errors
        page.raise_for_status()
        tree = html.fromstring(page.content)
        paras_anchors = tree.xpath('/html/head/title/text()|//p/a/text()|//p/text()')

        content = content_clean(paras_anchors)
        return content
    
    except requests.exceptions.HTTPError as e:
        return "NA"

#This function simply gets the correct new sqldate from date addition so sqldate 20140831+1=20140901 and not 20140832
#Can handle month changes, leap years, and situations with a large enough number of days that multiple months/years are iterated through.
def add_i(i):
    y = int(str(start)[:4])
    m = int(str(start)[4:6])
    d = int(str(start)[6:])
    def ml(year):
        month_lengths = {}
        if year%4==0:
            month_lengths[2]=29
        else:
            month_lengths[2]=28
        thirty=[4,6,9,11]
        thirty1=[1,3,5,7,8,10,12]
        for month in thirty:
            month_lengths[month]=30
        for month in thirty1:
            month_lengths[month]=31
        return month_lengths
    if d+i>ml(y)[m]:
        d_plus_i=d+i
        while d_plus_i-ml(y)[m]>0:
            d_plus_i-=ml(y)[m]
            m+=1
            if m==13:
                y+=1
                m=1
        if m<10:
            m_out='0'+str(m)
        else:
            m_out=str(m)
        if d_plus_i<10:
            d_out='0'+str(d_plus_i)
        else:
            d_out=str(d_plus_i)
        return int(str(y)+m_out+d_out)
    else:
        if m<10:
            m_out='0'+str(m)
        else:
            m_out=str(m)
        if d+i<10:
            d_out='0'+str(d+i)
        else:
            d_out=str(d+i)
        return int(str(y)+m_out+d_out)

import sys

start = int(sys.argv[1])
days = int(sys.argv[2])
days = list(range(1,days+1))

conn=psycopg2.connect(
  database="gdelt",
  user="Matthias",
  host="/tmp/"
)

for i in days:
    cur = conn.cursor() #(The cursor object allows interaction with the database.)
    cur.execute("select distinct * from florida_content where sqldate="+str(add_i(i))+";")
    if cur.rowcount>0:    
        t = cur.fetchall()
        t = [result for result in t]
        with open("daycsvs/florida_"+str(add_i(i))+".csv",'w',newline = '') as f:
            header = False
            writer = csv.writer(f)
            for row in t:
                writer.writerow(row)
        f.close()
    cur.close()
