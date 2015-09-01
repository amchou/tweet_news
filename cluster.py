#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re, time
from nltk.tag import pos_tag  
from nltk.tokenize import word_tokenize 
import nltk, string
from sklearn.feature_extraction.text import TfidfVectorizer

def preprocess(tweets):
	for n,t in enumerate(tweets):
		tweets[n] = t.strip()
		tweets[n] = re.sub(r'http:\S+','',tweets[n])

		#remove some punc; will get rid of end of abbreviations (e.g. U.S.) but will still match across tweets
		tweets[n] = re.sub(r'[":;!?\.,\n\t]',r' ',tweets[n])

		# 's   : make sure it's used as clitic and remove
		tweets[n] = re.sub(r'(\w)(\'s)( )',r'\1 ',tweets[n])
	return tweets

def tag(tweets):
	# NNP (proper nouns) will be an important feature for clustering/grouping news stories, so tagging is necessary
	# each string, when tagged, becomes list of tuples ('word','TAG')
	tagged_tweets = [pos_tag(word_tokenize(t)) for t in tweets]
	return tagged_tweets

def stem_tokens(tokens):
	return [stemmer.stem(item) for item in tokens]

def normalize(text):
	return stem_tokens(nltk.word_tokenize(text.lower()))

def cosine_sim(text1, text2):
	tfidf = vectorizer.fit_transform([text1, text2])
	return ((tfidf * tfidf.T).A)[0,1]

stemmer = nltk.stem.porter.PorterStemmer()
vectorizer = TfidfVectorizer(tokenizer=normalize, stop_words='english')

with open ('tweet_dump.txt','r') as r:

	t_data = r.readlines()
	print t_data[:2]

t={}
for line in t_data:
	try:
		id,user_name,text,retweets,faves,date = line.split('\t')
	except ValueError:
		print 'formatting issue, skip this for now'
		continue
	t[id] = [user_name,text,retweets,faves,date]
	#print t

for key in t:
	#print key
	t[key].append(preprocess([t[key][1]]))
	#print t[key]

# play with cosine similarity on preprocessed tweets to get an idea of how close stories are
# adj is a adjacency dictionary storing related tweets for every tweet
adj = {}
with open ('clustered.txt','a+') as a:
	for i in t:
		for j in t:
			if i != j:
				try:
					cos_sim = cosine_sim(t[i][5][0],t[j][5][0])
				except ValueError:
					cos_sim = 0
					print 'Error: empty vocab (possible all stopwords)'
				if cos_sim > 0.4:

					a.write('cos_sim for: '+str(i)+'\t'+str(j)+'\t'+str(cos_sim)+'\n')
					a.write(t[i][5][0]+'\n'+t[j][5][0]+'\n\n')

# use agglomerative clustering; see Ward method					
# create dict of clusters, including single tweets
clusters = {}
