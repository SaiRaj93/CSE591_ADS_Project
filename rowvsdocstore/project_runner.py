import time
from sentiment_analysis import Sentiment_Analysis

from data_loaders.sentiment_data_postgres import Sentiment_Data_Postgres
from data_loaders.sentiment_data_mongodb import Sentiment_Data_Mongodb

class Project_Runner(object):

	def __init__(self):
		self.sentiment_obj = Sentiment_Analysis()
		self.sentiment_obj.get_Sentiment('sample text to initially train the sentiment classifier')

	def postgres_sentiment_end_to_end(self, file, key):
		s_obj = Sentiment_Data_Postgres()
		s_obj.load_data(file)
		review = s_obj.get_review(key)
		cls = self.sentiment_obj.get_Sentiment(review)
		s_obj.delete_schema()


	def postgres_sentiment_query_response(self, s_obj, key):
		cls = self.sentiment_obj.get_Sentiment(s_obj.get_review(key))

	def mongodb_sentiment_end_to_end(self, file, key):
		s_obj = Sentiment_Data_Mongodb()
		s_obj.load_data(file)
		review = s_obj.get_review(key)
		cls = self.sentiment_obj.get_Sentiment(review)
		s_obj.delete_docs()

	def mongodb_sentiment_query_respose(self, s_obj, key):
		cls = self.sentiment_obj.get_Sentiment(s_obj.get_review(key))


if __name__ == '__main__' :
	# code for running and collecting the statics
	#start_time = time.time()
	#Project_Runner().postgres_sentiment_end_to_end('/home/master/Desktop/cse591_adb_project/rowvsdocstore/data/product_reviews_1.dat','800')
	#Project_Runner().postgres_sentiment_end_to_end('/home/master/Desktop/cse591_adb_project/rowvsdocstore/data/product_reviews_10.dat','800')
	#Project_Runner().postgres_sentiment_end_to_end('/home/master/Desktop/cse591_adb_project/rowvsdocstore/data/product_reviews_25.dat','800')
	#Project_Runner().postgres_sentiment_end_to_end('/home/master/Desktop/cse591_adb_project/rowvsdocstore/data/product_reviews_50.dat','800')
	#Project_Runner().postgres_sentiment_end_to_end('/home/master/Desktop/cse591_adb_project/rowvsdocstore/data/product_reviews_75.dat','800')
	#Project_Runner().postgres_sentiment_end_to_end('/home/master/Desktop/cse591_adb_project/rowvsdocstore/data/product_reviews.dat','800')
	#Project_Runner().mongodb_sentiment_end_to_end('/home/master/Desktop/cse591_adb_project/rowvsdocstore/data/product_reviews_1.dat','800')
	#Project_Runner().mongodb_sentiment_end_to_end('/home/master/Desktop/cse591_adb_project/rowvsdocstore/data/product_reviews_10.dat','800')
	#Project_Runner().mongodb_sentiment_end_to_end('/home/master/Desktop/cse591_adb_project/rowvsdocstore/data/product_reviews_25.dat','800')
	#Project_Runner().mongodb_sentiment_end_to_end('/home/master/Desktop/cse591_adb_project/rowvsdocstore/data/product_reviews_50.dat','800')
	#Project_Runner().mongodb_sentiment_end_to_end('/home/master/Desktop/cse591_adb_project/rowvsdocstore/data/product_reviews_75.dat','800')
	#Project_Runner().mongodb_sentiment_end_to_end('/home/master/Desktop/cse591_adb_project/rowvsdocstore/data/product_reviews.dat','800')
	#print(time.time() - start_time)
	# file = '/home/master/Desktop/cse591_adb_project/rowvsdocstore/data/product_reviews.dat'
	# keys = ['800','9126','15247', '23900', '33456', '46324', '53672', '66781', '72389', '83246']

	# obj = Project_Runner()

	# p_obj = Sentiment_Data_Postgres()
	# p_obj.load_data(file)
	# times = []
	# for key in keys:
	# 	start_time = time.time()
	# 	obj.postgres_sentiment_query_response(p_obj, key)
	# 	times.append(time.time() - start_time)

	# p_obj.delete_schema()
	# print('Average postgres ' + str(sum(times)/len(times))
	# print(times)

	# m_obj = Sentiment_Data_Mongodb()
	# m_obj.load_data(file)
	# times = []
	# for key in keys:
	# 	start_time = time.time()
	# 	obj.mongodb_sentiment_query_respose(m_obj, key)
	# 	times.append(time.time() - start_time)

	# m_obj.delete_docs()
	# print('Average mongodb ' + str(sum(times)/len(times)))
	# print(times)