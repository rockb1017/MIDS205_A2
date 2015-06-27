import sys
import tweepy
import datetime
import urllib
import urllib3
import urllib3.contrib.pyopenssl
import signal
import json
import signal
import requests
from boto.s3.connection import S3Connection
from boto.s3.key import Key
urllib3.disable_warnings()

conn = S3Connection('AKIAJZGVFFBC3QQNHLYQ', 'QmNI2mZ2gx+/ANxeLMM/GaW4/2mAN7kT2NO3Z1mp')
bucket= conn.get_bucket('rockbaek-205-assig2')

consumer_key = "oLxbLGmNxm1Ut8HBYOf5Cq7wy";
consumer_secret = "oG0oAjU6J3Pl5NXTQGExQF1qt0H9K6hJW11uFcAfI8hPMZuwYS";

access_token = "321815250-XE8ijM357JHkRXOJTW5yU6jg2EOncLFIGZqKZYSF";
access_token_secret = "6whVTm1HMkwxDik0oCiIk8FOA1XLxvqj7NMerIVsLpOCd";

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth_handler=auth,wait_on_rate_limit=True,wait_on_rate_limit_notify=True)


query=[]
with open('input2.txt','r') as q:#
	q= q.readlines()
	for line in q:
		query.append(line.split(','))


class TweetSerializer:
	def __init__(self, items,search,date):
		self.items=items
		self.out = None
		self.first = True
		self.n = 0
		self.count = 0
		self.max_n= 500
		self.search=search
		self.date=date

	
	def start(self):
		self.count += 1
		fname = "extract2/%s.%s.tweets-%d.json"%(self.search, self.date, self.count)
		print fname
		self.out = open(fname,"w")
		self.out.write("[\n")
		self.first = True

	def end(self):
	  	if self.out is not None:
			self.out.write("\n]\n")
		 	self.out.close()
		 	myKey = Key(bucket)
			myKey.key = "extract2/%s.%s.tweets-%d.json"%(self.search, self.date, self.count)
			myKey.set_contents_from_filename("extract2/%s.%s.tweets-%d.json"%(self.search, self.date, self.count))
	 	self.out = None
	 	self.n = 0

	def write(self,tweet):
		if not self.first:
			self.out.write(",\n")
		self.n += 1
		self.first = False
		self.out.write(json.dumps(tweet._json).encode('utf8'))

def interrupt(signum, frame):
	print "Interrupted, closing ..."
	if TS.n >0:
		TS.end()
	exit(1)

signal.signal(signal.SIGINT, interrupt)

for item in query:
	TS = TweetSerializer(tweepy.Cursor(api.search,q=item[2]).items(),item[0],item[1])
	TS.start()
	for tweet in TS.items:
		if TS.n==TS.max_n:
			TS.end()
			TS.start()
		TS.write(tweet)
	TS.end()
	

