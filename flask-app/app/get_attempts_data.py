import json
from tqdm import tqdm
import sys
import traceback
import os
import pandas as pd
from collections import namedtuple
from app import topic_hlr_train
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, SimpleStatement
from cassandra.auth import PlainTextAuthProvider
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
from elasticsearch_dsl import Q


CASSANDRA_HLR_KEYSPACE = 'hlr'
CASSANDRA_HLR_TABLE = 'entitywise_data'
CASSANDRA_USER_META_TABLE = 'user_meta'

CASSANDRA_USER = "cassandra"
CASSANDRA_PASS = "cassandra"

attempts_es_index = None
cassandra_cluster = None
cassandra_session = None


def get_attempts_es_index():
	global attempts_es_index
	if not attempts_es_index:
		attempts_es_client = Elasticsearch(os.getenv('ATTEMPTS_ES_URL', "http://172.30.0.189"))
		attempts_es_index = Search(index='attempt1').using(attempts_es_client)
	return attempts_es_index


def get_hlr_cassandra_session():
	global cassandra_cluster, cassandra_session
	if not cassandra_cluster:
		auth = PlainTextAuthProvider(username=os.getenv('CASSANDRA_USER', CASSANDRA_USER), password=os.getenv('CASSANDRA_PASS', CASSANDRA_PASS))
		cassandra_cluster = Cluster([os.getenv('CASSANDRA_URL', '172.30.0.91')], auth_provider=auth)
		cassandra_session = cassandra_cluster.connect()
		create_keyspace(cassandra_session)
		cassandra_session.set_keyspace(CASSANDRA_HLR_KEYSPACE)
		create_table(cassandra_session)
	return cassandra_cluster, cassandra_session


def kill_cassandra_cassandra_cluster(cassandra_cluster):
	cassandra_cluster.shutdown()


def create_keyspace(cassandra_session):
	cassandra_session.execute("""
		CREATE KEYSPACE IF NOT EXISTS hlr WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 3};
	""")


def create_table(cassandra_session):
	cassandra_session.execute("""
		CREATE TABLE IF NOT EXISTS {} (
			user_id text,
			entity_type text,
			entity_id int,
			last_practiced_at bigint,
			hl float,
			recall float,
			PRIMARY KEY (user_id, entity_type, entity_id)
		)
	""".format(CASSANDRA_HLR_TABLE))
	cassandra_session.execute("""
		CREATE TABLE IF NOT EXISTS {} (
			user_id text PRIMARY KEY,
			past_attempts_fetched int
		)
	""".format(CASSANDRA_USER_META_TABLE))


def past_attempts_fetched(user_id):
	cassandra_cluster, cassandra_session = get_hlr_cassandra_session()
	results = cassandra_session.execute("SELECT * FROM {} WHERE user_id='{}'".format(CASSANDRA_USER_META_TABLE, user_id))
	return len(results.current_rows) > 0


def get_last_practiced(user_id, entity_type):
	_, cassandra_session = get_hlr_cassandra_session()
	query = "SELECT entity_id, last_practiced_at from {} where user_id='{}' and entity_type='{}'".format(CASSANDRA_HLR_TABLE, user_id, entity_type)
	results = cassandra_session.execute(query)
	if len(results.current_rows) > 0:
		data = dict()
		for row in results:
			data[int(row.entity_id)] = int(row.last_practiced_at)
		return data
	return None


def write_to_hlr_index(user_id, results):
	cassandra_cluster, cassandra_session = get_hlr_cassandra_session()
	batch = BatchStatement()
	update_row = cassandra_session.prepare("UPDATE entitywise_data SET last_practiced_at=?, hl=?, recall=? WHERE user_id=? and entity_type=? and entity_id=?")
	for row in results:
		row = row._asdict()
		batch.add(update_row, (int(row['last_practiced_at']), row['hl'], row['recall'], row['userid'], 'chapter', int(row['chapterid'])))
	cassandra_session.execute(batch)
	cassandra_session.execute("UPDATE {} SET past_attempts_fetched=1 WHERE user_id='{}'".format(CASSANDRA_USER_META_TABLE, user_id))
	

def get_all_chapters_for_user(user_id):
	cassandra_cluster, cassandra_session = get_hlr_cassandra_session()
	res = cassandra_session.execute("SELECT * FROM {} WHERE user_id='{}'".format(CASSANDRA_HLR_TABLE, user_id))
	chapters_data = list()
	for chapter in res:
		chapters_data.append(topic_hlr_train.Result(chapter.user_id, chapter.entity_id, chapter.last_practiced_at, chapter.recall, chapter.hl))
	return chapters_data


def get_chapter_for_user(user_id, chapter_id):
	cassandra_cluster, cassandra_session = get_hlr_cassandra_session()
	res = cassandra_session.execute("SELECT * FROM {} WHERE user_id='{}' and entity_type='{}' and entity_id={}".format(CASSANDRA_HLR_TABLE, user_id, 'chapter', chapter_id))
	if res:
		return topic_hlr_train.Result(res[0].user_id, res[0].entity_id, res[0].last_practiced_at, res[0].recall, res[0].hl)
	return None


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
