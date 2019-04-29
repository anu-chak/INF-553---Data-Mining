import tweepy
import random
#override tweepy.StreamListener to add logic to on_status
sequence_no= -1
sample_size= 100
tags= {}
tweets= []

class MyStreamListener(tweepy.StreamListener):
	def on_status(self, tweet):
		global sequence_no
		global tweets
		global tags

		hashtags= tweet.entities['hashtags']
		if(len(hashtags)>0):
			# print(hashtags)
			# for i in hashtags:
			# 	print(i['text'])

			sequence_no= sequence_no+1
			if(sequence_no<sample_size):
				# Keep
				tweets.append(tweet)
				for i in hashtags:
					hashtag= i['text']
					if(hashtag in tags.keys()):
						tags[hashtag]= tags[hashtag]+1
					else:
						tags[hashtag]= 1
			else:
				r= random.randint(1,sequence_no)
				if(r<=100):
					pos= random.randint(0,sample_size-1)
					twt_to_remove= tweets[pos]

					tags_to_remove= twt_to_remove.entities['hashtags']
					for i in tags_to_remove:

						h= i['text']
						tags[h]= tags[h]-1
						if(tags[h]==0):
							del tags[h]

					tweets[pos]= tweet
					new_hashtags= tweet.entities['hashtags']
					for i in new_hashtags:
						ht= i['text']
						if(ht in tags.keys()):
							tags[ht]= tags[ht]+1
						else:
							tags[ht]= 1
					#Keep
				else:
					pass
					#Don't keep
			
			tags_list= []
			for key, value in tags.items():
				temp = [key,value]
				tags_list.append(temp)
			tags_list.sort(key=lambda x:x[0])
			tags_list.sort(key=lambda x:x[1], reverse=True)

			ctr=0
			prev= tags_list[0][1]

			print("The number of tweets with tags from the beginning: "+str(sequence_no+1))
			for i in tags_list:
				if(i[1]==prev):
					print(i)
				else:
					if(ctr<2):
						prev= i[1]
						ctr= ctr+1
						print(i)
					else :
						break

			print("\n")


	def on_error(self, status_code):
		if status_code == 420:
			#returning False in on_error disconnects the stream
			return False

api_key= "k3WqMoP1Vy6QdKQOFO14MEBlm"
api_key_secret= "8fFCnosOrW9bIy8rbxAHrB9mGomtfWiCcIIFuPMCT355yvGtbO"
access_token= "1345126940-saQ6YwQCfRRCPACPHofXVMwjabcMczSRDjUIhvk"
access_token_secret= "0JWPCu6TJ0nFZFlBcu4pBg4wTffTnlt1HwBQcUeqKIU3M"

auth = tweepy.OAuthHandler(api_key, api_key_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth)

myStreamListener = MyStreamListener()
myStream = tweepy.Stream(auth = api.auth, listener=myStreamListener)

myStream.filter(track=['BBMAsTopSocial'])