from pymongo import MongoClient

class Mongodb_Connect(object):

	def __init__(self):
		self.client = MongoClient()

	def get_client(self):
		return self.client