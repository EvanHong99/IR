"""
import gensim : vsm
import networkx : pagerank
steps:
    1. 构建词典
    2.
"""
import gensim
import os
import networkx as nx
import traceback
import logging
import pandas as pd
import numpy as np
import configs
from collections import defaultdict

def cul_pageranke():
    """
    计算PageRank并存入数据库
    :return: 数据存储于PageRank数据库
    """
    G=nx.DiGraph()
    try:
        csv=pd.read_csv('tables/everytinku_links1.csv', encoding='unicode_escape', names=['index', 'hook', 'link', 'base'])
        for i in csv.itertuples():
            G.add_edge(getattr(i,'link'), getattr(i,'base'))
    except Exception:
        print(traceback.format_exc())

    pr=nx.pagerank(G,alpha=0.85)

    df=pd.DataFrame(pr.items(),columns=['node','value'])

    from sqlalchemy import engine
    engine1 = engine.create_engine(configs.config.DB_LOGIN)
    df.to_sql('pagerank', engine1, 'everytinku',if_exists='replace',  index=False)

class VSM(object):
    def __init__(self):



cul_pageranke()
