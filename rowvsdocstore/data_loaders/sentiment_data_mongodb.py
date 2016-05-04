from mongodb_connect import Mongodb_Connect

class Sentiment_Data_Mongodb(object):
	def __init__(self):
		self.client = Mongodb_Connect().get_client()
		self.db = self.client['product_reviews']

	def load_data(self, file_name):	
		file = open(file_name)
		for line in file:
			attributes = line.split('|')
			self.db.product_reviews.insert_one({
				'id':attributes[0],
				'col2':attributes[1],
				'col3':attributes[2],
				'col4':attributes[3],
				'col5':attributes[4],
				'col6':attributes[5],
				'col7':attributes[6],
				'review':attributes[7],
				})

		file.close()

	def get_review(self, key):
		result = self.db.product_reviews.find_one({'id':key})
		return result['review']

	def delete_docs(self):
		self.db.product_reviews.drop()