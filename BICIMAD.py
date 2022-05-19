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
    return date, user_type, travel_time
    
def main():
    with SparkContext() as sc:
        files = ["20"+str(i)+"_movements.json" for i in [1908, 1909, 1910, 1911, 1912, 2001, 2002, 2003, 2004, 2005, 2006, 2007]]
        rdd = sc.textFile(",".join(files)).map(mapper)
        cuenta_usos = dict(rdd.countByKey().items())
        cuenta_tipo_usuario_porcentaje = sorted(rdd.map(lambda x: (x[0], [1*(i==x[1]-1) for i in range(3)])).reduceByKey(lambda x, y: [x[i]+y[i] for i in range(3)]).map(lambda x: (x[0], [x[1][i]/cuenta_usos[x[0]] for i in range(3)])).collect())
        media_tiempo = sorted(rdd.map(lambda x: (x[0], x[2])).reduceByKey(lambda x, y: x+y).map(lambda x: (x[0], x[1]/cuenta_usos[x[0]])).collect())
        
        
        print(cuenta_usos)
        print()
        print(cuenta_tipo_usuario_porcentaje)
        print()
        print(media_tiempo)
        
        fig, ax = plt.subplots()
        plt.xticks(size = 6)
        ax.bar(cuenta_usos.keys(), cuenta_usos.values())
        fig.show()
        
        fig2, ax2 = plt.subplots()
        plt.xticks(size = 6)
        ax2.bar([cuenta_tipo_usuario_porcentaje[i][0] for i in range(12)], [cuenta_tipo_usuario_porcentaje[i][1][0] for i in range(12)])
        fig2.show()
        
        fig3, ax3 = plt.subplots()
        plt.xticks(size = 6)
        ax3.bar([cuenta_tipo_usuario_porcentaje[i][0] for i in range(12)], [cuenta_tipo_usuario_porcentaje[i][1][1] for i in range(12)])
        fig3.show()
        
        fig4, ax4 = plt.subplots()
        plt.xticks(size = 6)
        ax4.bar([cuenta_tipo_usuario_porcentaje[i][0] for i in range(12)], [cuenta_tipo_usuario_porcentaje[i][1][2] for i in range(12)])
        fig4.show()
        
        fig5, ax5 = plt.subplots()
        plt.xticks(size = 6)
        ax5.bar([media_tiempo[i][0] for i in range(12)], [media_tiempo[i][1] for i in range(12)])
        fig5.show()
        
        
if __name__ == "__main__":
    main()
    
    
    
    