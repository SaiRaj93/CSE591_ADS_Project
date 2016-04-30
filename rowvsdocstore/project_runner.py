from sentiment_analysis import Sentiment_Analysis

from data_loaders.sentiment_data_postgres import Sentiment_Data_Postgres

class Project_Runner(object):

	def __init__(self):
		pass

	def sentiment_end_to_end(self, file):
		s_obj = Sentiment_Data_Postgres()
		s_obj.load_data(file)
		review = s.obj.get_review('50')
		cls = Sentiment_Analysis().get_Sentiment(review)
		s_obj.delete_schema()


	def sentiment_query_response(self, s_obj, key):
		cls = Sentiment_Analysis().get_review(s_obj.get_review(key))



if __name__ = '__main__' :
	# code for running and collecting the statics