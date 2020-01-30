import json
from tqdm import tqdm
import sys
import traceback
import os
import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
from elasticsearch_dsl import Q


def get_attempts_es_index():
	es_client = Elasticsearch(os.getenv('ATTEMPTS_ES_URL', "http://172.30.0.189"))
	return Search(index='attempt1').using(es_client)


def get_attempts_of_user(user_id, get_attempts_after, size=100000):
	attempts_df = pd.DataFrame()
	try:
		extras = dict(size = size)
		query = get_attempts_es_index().filter('term', userid=user_id).filter('range', attempttime={'gte': get_attempts_after}).extra(**extras).source(["attempttime", "userid", "examid", "id", "iscorrect", "difficulty", "categoryid"])
		res = query.execute()
		if res:
			for attempt in res:
				attempts_df = attempts_df.append(attempt.__dict__['_d_'], ignore_index=True)
	except Exception as e:
		print (e)
		traceback.print_exc()
	return attempts_df
