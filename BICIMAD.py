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


# =============================================================================
# def cargar_datos():
#     datos = []
#     for i in [1907, 1908, 1909, 1910, 1911, 1912, 2001, 2002, 2003, 2004, 2005, 2006, 2007]:
#         for line in open("20" + str(i) +"_movements.json", "r"):
#             datos.append(json.loads(line))
#     return datos
# =============================================================================

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
        rddfile = sc.textFile(",".join(files)).map(mapper)
        print(rddfile.countByKey().items())
        print(sorted(rddfile.map(lambda x: (x[0]+"-"+str(x[1]), x[1], x[2], x[3])).countByKey().items()))
        print(sorted(rddfile.map(lambda x: (x[0]+"-"+str(x[3]), x[1], x[2], x[3])).countByKey().items()))

if __name__ == "__main__":
    main()
    
    
    
    
"""
def user_type(x):
    if x == 0:
        user = 'No se ha podido determinar el tipo de usuario'
    elif x == 1:
        user = 'Usuario anual (poseedor de un pase anual)'
    elif x == 2:
        user = 'Usuario ocasional'
    elif x == 3:
        user = 'Trabajador de la empresa'
    return user

def ageRange(y):
    if y == 0:
        rango_edad = 'No se ha podido determinar el rango de edad del usuario'
    elif y == 1:
        rango_edad = 'El usuario tiene entre 0 y 16 años'
    elif y == 2:
        rango_edad = 'El usuario tiene entre 17 y 18 años'
    elif y == 3:
        rango_edad = 'El usuario tiene entre 19 y 26 años'
    elif y == 4:
        rango_edad = 'El usuario tiene entre 27 y 40 años'
    elif y == 5:
        rango_edad = 'El usuario tiene 66 años o más'
    return rango_edad
"""