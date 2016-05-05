from sentiment_analysis import Sentiment_Analysis

from data_loaders.sentiment_data_postgres import Sentiment_Data_Postgres
from data_loaders.sentiment_data_mongodb import Sentiment_Data_Mongodb

class Project_Runner(object):

	def __init__(self):
		self.sentiment_obj = Sentiment_Analysis()

	def postgres_sentiment_end_to_end(self, file, key):
		s_obj = Sentiment_Data_Postgres()
		s_obj.load_data(file)
		review = s.obj.get_review(key)
		cls = self.sentiment_obj.get_Sentiment(review)
		s_obj.delete_schema()


	def postgres_sentiment_query_response(self, s_obj, key):
		cls = self.sentiment_obj.get_review(s_obj.get_review(key))

	def mongodb_sentiment_end_to_end(self, file, key):
		s_obj = Sentiment_Data_Mongodb()
		s_obj.load_data(file)
		review = s_obj.get_review(key)
		cls = self.sentiment_obj.get_Sentiment(review)
		s_obj.delete_docs()

	def mongodb_sentiment_query_respose(self, s_obj, key):
		cls = self.sentiment_obj.get_review(s_obj.get_review(key))


if __name__ =='__main__' :
	pass
	# code for running and collecting the statics