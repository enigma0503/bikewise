import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from pyspark.sql.functions import *

def create_report(yesterday, df):
    df = df.select('type'). \
        groupBy(col('type')).count()
    
    df = df.toPandas()
    
    graph = plt.figure(figsize=(10, 8))
    splot=sns.barplot(x="type",y="count",data = df)
    for p in splot.patches:
        splot.annotate(format(p.get_height(), '.0f'), 
                   (p.get_x() + p.get_width() / 2., p.get_height()), 
                   ha = 'center', va = 'center', 
                   xytext = (0, 9), 
                   textcoords = 'offset points')
    plt.xlabel("Incident Type", size=14)
    plt.ylabel("Count", size=14)
    plt.title("Count of Incidents", size = 20)
    plt.savefig(f'/home/itv000579/shubham/bike_data/reports/report_{yesterday}.pdf')
    plt.close()