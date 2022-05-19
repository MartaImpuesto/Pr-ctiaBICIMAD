# -*- coding: utf-8 -*-
"""
Created on Thu May  5 10:30:52 2022

@author: mimpu
"""

import json
from pprint import pprint
from pyspark import SparkContext
import datetime
import sys
import matplotlib.pyplot as plt


def mapper(line):
    data = json.loads(line)
    date = data["unplug_hourTime"][0:7]
    user_type = data["user_type"]
    travel_time = data["travel_time"]
    ageRange = data["ageRange"]
    return date, user_type, travel_time, ageRange
    
def main():
    with SparkContext() as sc:
        files = ["20"+str(i)+"_movements.json" for i in [1908, 1909, 1910, 1911, 1912, 2001, 2002, 2003, 2004, 2005, 2006, 2007]]
        rdd = sc.textFile(",".join(files)).map(mapper)
        cuenta_usos = dict(rdd.countByKey().items())
        #cuenta_tipo_usuario = sorted(rdd.map(lambda x: (x[0], [1*(i==x[1]-1) for i in range(3)])).reduceByKey(lambda x, y: [x[i]+y[i] for i in range(3)]).collect())
        #cuenta_años_usuario = sorted(rdd.map(lambda x: (x[0], [1*(i==x[3]) for i in range(7)])).reduceByKey(lambda x, y: [x[i]+y[i] for i in range(7)]).collect())
        #media_tiempo = sorted(rdd.map(lambda x: (x[0], x[2])).reduceByKey(lambda x, y: x+y).map(lambda x: (x[0], x[1]/cuenta_usos[x[0]])).collect())
        
        
        print(cuenta_usos)
        #print(cuenta_tipo_usuario)
        #print(cuenta_años_usuario)
        #print(media_tiempo)
        
        fig, ax = plt.subplots()
        ax.bar(cuenta_usos.keys(), cuenta_usos.values())
        fig.show()
        
if __name__ == "__main__":
    main()
    
    
    
    