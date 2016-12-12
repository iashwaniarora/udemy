from mrjob.job import MRJob

class CustomerSpending(MRJob):
	
	def convertToFloat(self,stringValue):
		return float(stringValue)
	
	def mapper(self,_,line):
		(custid,itemid,price)=line.split(",")
		yield custid,self.convertToFloat(price)

	def reducer(self,key,values):
		for value in values:
	     		yield key,value

if __name__ == "__main__":
	CustomerSpending.run()
	
