import pandas as pd
import numpy as np
import seaborn as sns
from sklearn.cluster import KMeans
from sklearn.preprocessing import LabelEncoder
from sklearn.preprocessing import MinMaxScaler
import matplotlib.pyplot as plt
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler
from google.cloud import bigquery
import time
from sklearn import metrics
from sklearn.metrics import pairwise_distances
import seaborn as sns
import simplejson as json
import os
from datetime import timedelta
import webbrowser
import papermill as pm
from enum import Enum
import re
import simplejson as json
from IPython.core.interactiveshell import InteractiveShell
from sklearn.ensemble import IsolationForest
from sklearn.cluster import OPTICS
from collections import Counter

# konfiguration

max_number_of_clusters = 30
min_number_of_clusters = 2

class OUTLIER_REMOVAL(Enum):
    NONE = 1 
    VARIANCE_THRESHOLD_WITHIN_CLUSTER = 2 
    ISOLATION_FOREST = 3 

outlier_removal = OUTLIER_REMOVAL.VARIANCE_THRESHOLD_WITHIN_CLUSTER.value

class CLUSTER_METHOD(Enum):
    KMEANS = 1 
    OPTICS = 2 

cluster_method = CLUSTER_METHOD.KMEANS.value

number_of_addresses = 100000

# format: yyyy-mm-dd
observation_period_start = "2020-01-01 00:00:00+00"
observation_period_end = "2019-06-01 00:15:00+00"

class ADDRESS_SELECTION(Enum):
    RANDOM = 1 # selects random features.index, that have been active within the observation period.
    RICHEST = 2 # selects the accounts that have the most ether # not yet implemented
    HIGHEST_TURNOVER = 3 # selects the accounts that have the most ether received + sent

address_selection = ADDRESS_SELECTION.RICHEST.value

# max USD amount to spent for executing sql queries
max_bigquery_costs_usd = 2

# Delete existing tables
reset = False

# run
for addsel in ADDRESS_SELECTION.__members__:
    for outrem in OUTLIER_REMOVAL.__members__: 
        for clumet in CLUSTER_METHOD.__members__: 
            experiment_id = "clusteranalysis_{}_{}_{}_{}_{}_{}".format(addsel, outrem, clumet, number_of_addresses, re.sub(r'[-.+: ]', '_', observation_period_start),re.sub(r'[-.+: ]', '_', observation_period_end))
    
            print("Executing experiment: {}".format(experiment_id))
            
            address_selection = ADDRESS_SELECTION[addsel].value
            outlier_removal = OUTLIER_REMOVAL[outrem].value
            cluster_method = CLUSTER_METHOD[clumet].value
            try:
                pm.execute_notebook(
                   './clusteranalysis.ipynb',
                   './results/{}.result.ipynb'.format(experiment_id),
                   parameters = dict(number_of_addresses=number_of_addresses, 
                                     observation_period_start=observation_period_start,
                                     observation_period_end=observation_period_end, 
                                     address_selection=address_selection,
                                     max_bigquery_costs_usd=max_bigquery_costs_usd, 
                                     reset = reset,
                                     outlier_removal = outlier_removal,
                                     cluster_method = cluster_method,
                                     max_number_of_clusters = max_number_of_clusters,
                                     min_number_of_clusters = min_number_of_clusters
                                    ),
                    cwd = "."
                )
            except Exception as e:
                print(e)

            # for file in os.listdir("/tmp"): 
            #     if re.compile("tmp.*.json").match(file): 
            #     os.remove("/tmp/{}".format(file)) 
            os.system("pkill -f HistoryManager")

            print("\nExecuted successfully.\n\n")