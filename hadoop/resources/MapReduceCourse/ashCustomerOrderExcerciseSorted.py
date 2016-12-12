from mrjob.job import MRJob
from mrjob.step import MRStep

class CustomerSpending(MRJob):

	def steps(self):
	   return [
	     MRStep(mapper=self.mapper,reducer=self.reducer),
	     MRStep(mapper=self.mappersecond,reducer=self.reducersecond)
	]
	
	def convertToFloat(self,stringValue):
		return float(stringValue)
	
	def mapper(self,_,line):
		(custid,itemid,price)=line.split(",")
		yield custid,self.convertToFloat(price)

	def reducer(self,key,values):
	     yield key,sum(values)

	def mappersecond(self,key,values):
		yield '%04.02f'%float(values),key
	
	def reducersecond(self,key,values):
		for value in values:
			yield key ,value

if __name__ == "__main__":
	CustomerSpending.run()
	
