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
CASSANDRA_TODAYS_ATTEMPTS = "today_attempts"

CASSANDRA_USER = "cassandra"
CASSANDRA_PASS = "cassandra"

attempts_es_index = None
cassandra_cluster = None
cassandra_session = None


def get_attempts_es_index():
	global attempts_es_index
	if not attempts_es_index:
		attempts_es_client = Elasticsearch(os.getenv('ATTEMPTS_ES_URL', "http://monouser:i8w4orw4u8ow4f4@172.30.0.172"))
		attempts_es_index = Search(index='attempt').using(attempts_es_client)
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
			last_practiced_before_today bigint,
			last_practiced_today bigint,
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
	cassandra_session.execute("""
		CREATE TABLE IF NOT EXISTS {} (
			user_id text,
			entity_type text,
			entity_id int,
			last_practiced_at bigint,
			PRIMARY KEY (user_id, entity_type, entity_id)
		)
	""".format(CASSANDRA_TODAYS_ATTEMPTS))


def past_attempts_fetched(user_id):
	cassandra_cluster, cassandra_session = get_hlr_cassandra_session()
	results = cassandra_session.execute("SELECT * FROM {} WHERE user_id='{}'".format(CASSANDRA_USER_META_TABLE, user_id))
	print ("Past attempts fetched for {}: {}".format(user_id, len(results.current_rows) > 0))
	return len(results.current_rows) > 0


def get_last_practiced(user_id, entity_type):
	_, cassandra_session = get_hlr_cassandra_session()
	query = "SELECT entity_id, last_practiced_before_today from {} where user_id='{}' and entity_type='{}'".format(CASSANDRA_HLR_TABLE, user_id, entity_type)
	results = cassandra_session.execute(query)
	if len(results.current_rows) > 0:
		data = dict()
		for row in results:
			if row.last_practiced_before_today:
				data[int(row.entity_id)] = int(row.last_practiced_before_today)
		return data
	return None


def write_to_hlr_index(user_id, results, todays_attempts, entity_type):
	cassandra_cluster, cassandra_session = get_hlr_cassandra_session()
	batch = BatchStatement()
	update_row = cassandra_session.prepare("UPDATE {} SET {}=?, hl=?, recall=? WHERE user_id=? and entity_type=? and entity_id=?".format(CASSANDRA_HLR_TABLE, 'last_practiced_today' if todays_attempts else 'last_practiced_before_today'))
	for row in results:
		row = row._asdict()
		batch.add(update_row, (int(row['last_practiced_at']), row['hl'], row['recall'], row['userid'], entity_type, int(row['entityid'])))
	cassandra_session.execute(batch)
	if todays_attempts:
		store_todays_attempts = cassandra_session.prepare("UPDATE {} set last_practiced_at=? WHERE user_id=? and entity_type=? and entity_id=?".format(CASSANDRA_TODAYS_ATTEMPTS))	
		todays_attempts_batch = BatchStatement()
		for row in results:
			row = row._asdict()
			todays_attempts_batch.add(store_todays_attempts, (int(row['last_practiced_at']), row['userid'], entity_type, int(row['entityid'])))
		cassandra_session.execute(todays_attempts_batch)
	else:
		# Set past_attempts_fetched to 1 
		cassandra_session.execute("UPDATE {} SET past_attempts_fetched=1 WHERE user_id='{}'".format(CASSANDRA_USER_META_TABLE, user_id))
	

def update_last_practiced_before_today():
	cassandra_cluster, cassandra_session = get_hlr_cassandra_session()
	results = cassandra_session.execute("SELECT * FROM {}".format(CASSANDRA_TODAYS_ATTEMPTS))
	print ("Updating last practiced time in {} rows".format(len(results.current_rows)))
	batch = BatchStatement()
	update_row = cassandra_session.prepare("UPDATE {} SET last_practiced_before_today=?, last_practiced_today=? WHERE user_id=? and entity_type=? and entity_id=?".format(CASSANDRA_HLR_TABLE))
	for result in results:
		batch.add(update_row, (result.last_practiced_at, 0, result.user_id, result.entity_type, result.entity_id))
	cassandra_session.execute(batch)
	print ("Truncating {}".format(CASSANDRA_TODAYS_ATTEMPTS))
	cassandra_session.execute('TRUNCATE {}'.format(CASSANDRA_TODAYS_ATTEMPTS))


def get_all_entities_for_user(user_id, entity_type):
	cassandra_cluster, cassandra_session = get_hlr_cassandra_session()
	return cassandra_session.execute("SELECT * FROM {} WHERE user_id='{}' and entity_type='{}'".format(CASSANDRA_HLR_TABLE, user_id, entity_type))


def get_entity_for_user(user_id, entity_type, entity_id):
	cassandra_cluster, cassandra_session = get_hlr_cassandra_session()
	rows = cassandra_session.execute("SELECT * FROM {} WHERE user_id='{}' and entity_type='{}' and entity_id={}".format(CASSANDRA_HLR_TABLE, user_id, entity_type, entity_id))
	return rows[0] if rows else None


def get_attempts_of_user(user_id, t_start, t_end, size=100000):
	attempts_df = pd.DataFrame()
	try:
		extras = dict(size = size)
		query = get_attempts_es_index().filter('term', userid=user_id).filter('range', attempttime={'gte': t_start, 'lte': t_end}).extra(**extras).source(["attempttime", "userid", "examid", "id", "iscorrect", "difficulty", "categoryid"])
		res = query.execute()
		if res:
			for attempt in res:
				attempts_df = attempts_df.append(attempt.__dict__['_d_'], ignore_index=True)
	except Exception as e:
		print (e)
		traceback.print_exc()
	return attempts_df
