#!/usr/bin/python3

"""Calculates the lhipa.

Reads stdin and calculates the lhipa for the given data
"""

__author__ = "Kay Erik Jenss & Jonas Hermann"
__license__ = "unlicence"
__version__ = "0.0.1"
__status__ = "Prototpye"

import pandas as pd
import sys
import json
import sqlite3
from datetime import datetime

for line in sys.stdin:
    data = json.loads(line)
    currentLine = data['line']
    db = sqlite3.connect('/data/lines_count.db')
    cursor = db.cursor()
    cursor.execute('CREATE TABLE IF NOT EXISTS lineCounter (line INTEGER PRIMARY KEY,count INTEGER)')
    cur = cursor.execute('SELECT * FROM lineCounter WHERE line=?;',(currentLine,))
    rows = cur.fetchall()
    if not rows:
        #print('Inserting rows') 
        cursor.execute('INSERT INTO lineCounter(line, count) VALUES(?,?)',(currentLine, 1))
    for row in rows:
        if(row[0]==currentLine):
            # build json
            json_string = "{\"line\":"+str(currentLine)+",\"count\":"+str(row[1]+1)+",\"timestamp\":\""+str(datetime.now())+"\"}"
            json_dict = {currentLine:row[1]+1}
            print(json_string)
            #print(row[1])
            cursor.execute('UPDATE lineCounter SET count=? WHERE line=?;',(row[1]+1, currentLine))
             
   



db.commit()
cursor.close()
db.close() 
