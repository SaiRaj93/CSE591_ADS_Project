from textblob import TextBlob
from textblob.sentiments import NaiveBayesAnalyzer

class Sentiment_Analysis(object):
	"""Determine the sentiments of the given text"""

	def __init__(self, arg):
		pass
	
	def get_Sentiment(text):
		"""Returns a string that determines the classification of the given text, namely 'pos' or 'neg."""

		sentiment = TextBlob(text, analyzer=NaiveBayesAnalyzer()).sentiment
		return sentiment.classification
