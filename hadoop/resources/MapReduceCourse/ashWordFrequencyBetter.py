from mrjob.job import MRJob
import re

WORD_REGEX = re.compile(r"[\w']+")

class WordFrequencyBetter(MRJob):


	def mapper(self,_,line):
		words = WORD_REGEX.findall(line)
		for word in words:
			word =unicode(word,"utf-8",errors="ignore")
			yield word.lower(),1

	def reducer(slef,key,values):
		yield key,sum(values)
		

if __name__== '__main__':
	WordFrequencyBetter.run()
