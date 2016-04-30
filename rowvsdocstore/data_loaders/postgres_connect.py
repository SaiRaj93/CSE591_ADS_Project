import psycopg2

class Postgres_Connect(object):
	"""handles the conection to postgres"""
	def __init__(self):
		pass

	def getConnection(self, dbname, user_name, host_addr, pwd):
		'''Attempts to connect to postgres with the given credentials and returns the connection object
		'''
		return psycopg2.connect(database=dbname, user=user_name, host=host_addr, password = pwd)