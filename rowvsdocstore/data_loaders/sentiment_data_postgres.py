from postgres_connect import Postgres_Connect

class Sentiment_Data_Postgres(object):
	def __init__(self):
		self.con = Postgres_Connect().getConnection('cse591prj' , 'postgres', 'localhost', 'ok')
		self.cur = self.con.cursor()

	def load_data(self, file):
		self.cur.execute('''
			CREATE TABLE reviews(
				col1 varchar(10), 
				col2 varchar(10), 
				col3 varchar(10), 
				col4 varchar(10), 
				col5 varchar(10),
				col6 varchar(10),
				col7 varchar(10),
				col8 varchar(6000)); 
			''')
		self.con.commit()

		q = "COPY reviews FROM '" + file + "' DELIMITER '|';"
		self.cur.execute(q)
		self.con.commit()

	def get_review(self, key):
		self.cur.execute("SELECT col8 FROM reviews WHERE col1 = '"+ key + "';")
		return self.cur.fetchone()[0]

	def delete_schema(self):
		self.cur.execute('DROP TABLE reviews;')
		self.con.commit()

 #/home/master/Desktop/cse591_adb_project/rowvsdocstore/data/product_reviews_1.dat
 #COPY reviews FROM  '/home/master/Desktop/cse591_adb_project/rowvsdocstore/data/product_reviews_1.dat' DELIMITER '|';