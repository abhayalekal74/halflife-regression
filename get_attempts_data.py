import json
from tqdm import tqdm
import sys
import os
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
from elasticsearch_dsl import Q

es_client = None

def get_es_client():
	global es_client
	if es_client is None:
		es_client = Elasticsearch(os.getenv('ES_URL', "http://172.30.0.189"))
	return es_client


def get_attempt_from_hit(hit):
	return [hit["userid"], hit["examid"], hit["attempttime"], hit["id"], hit["iscorrect"], hit["difficulty"], hit["categoryid"]]


def get_attempts_of_user(s, uid, size=100000):
	try:
		extras = dict(size = size)
		query = s.filter('term', userid=uid).extra(**extras).source(["attempttime", "userid", "examid", "id", "iscorrect", "difficulty", "categoryid"])
		res = query.execute()
		if res:
			for hit in res:
				try:
					print ("{},{},{},{},{},{},{}".format(hit["userid"], hit["examid"], hit["attempttime"], hit["id"], hit["iscorrect"], hit["difficulty"], cat_chap_map[str(hit["categoryid"])]))
				except Exception:
					pass
		else:
			print ("No data for {}".format(uid))
	except Exception:
		pass

if __name__=='__main__':
	get_es_client()
	s = Search(index="attempt1").using(get_es_client())
	cat_chap_map = json.load(open(sys.argv[2]))
	with open(sys.argv[1]) as f:
		lines = f.readlines()
		for x in tqdm(range(len(lines))):
			l = lines[x]
			get_attempts_of_user(s, l.strip())
