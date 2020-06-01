from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, SimpleStatement
from cassandra.auth import PlainTextAuthProvider

CASSANDRA_HLR_KEYSPACE = 'hlr'
CASSANDRA_HLR_TABLE = 'entitywise_data'
CASSANDRA_USER_META_TABLE = 'user_meta'
CASSANDRA_TODAYS_ATTEMPTS = "today_attempts"

CASSANDRA_USER = "cassandra"
CASSANDRA_PASS = "cassandra"

BATCH_LIMIT = 50

attempts_es_index = None
cassandra_cluster = None
cassandra_session = None

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


def update_last_practiced_before_today():
	cassandra_cluster, cassandra_session = get_hlr_cassandra_session()
	results = cassandra_session.execute("SELECT * FROM {}".format(CASSANDRA_TODAYS_ATTEMPTS))
	batch = BatchStatement()
	update_row = cassandra_session.prepare("UPDATE {} SET last_practiced_before_today=?, last_practiced_today=? WHERE user_id=? and entity_type=? and entity_id=?".format(CASSANDRA_HLR_TABLE))

	count = 0	
	for result in results:
		batch.add(update_row, (result.last_practiced_at, 0, result.user_id, result.entity_type, result.entity_id))
		count += 1
		if count >= BATCH_LIMIT:
			cassandra_session.execute(batch)
			batch = BatchStatement()
			count = 0
	if count:
		cassandra_session.execute(batch)
	
	print ("Truncating {}".format(CASSANDRA_TODAYS_ATTEMPTS))
	cassandra_session.execute('TRUNCATE {}'.format(CASSANDRA_TODAYS_ATTEMPTS))

