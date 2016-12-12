from mrjob.job import MRJob
	
class ashFriendByAge(MRJob):
	def mapper(self,key,line):
		(userID,name,age,friends)= line.split(',')
		yield age,float(friends)
	
	def reducer(self,age,friends):
		totalsum=0
		noOfelements=0
		for x in friends:
			totalsum=totalsum+x
			noOfelements=noOfelements+1
		yield age,totalsum/noOfelements


if __name__ == '__main__':
	ashFriendByAge.run()

