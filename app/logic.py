#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import twitter_api_func as twapi
import pickle
import numpy as np
import tokenization as tknz

def vectorize_tweeter(screen_name):
    """Look up tweeter by screen name. Produce feature matrix from
    their user information and timeline"""
    
    try:
        api = twapi.get_api()
        
        user_tweets = api.user_timeline(screen_name=screen_name, count=4)
        
        user_corpus = [tweet.text for tweet in user_tweets]
        
        lda = pickle.load(file("lda.pickle", "r"))['lda']
        user_lda_dict = dict(np.array(lda[lda.id2word.doc2bow(tknz.full_tokenize(u'\n'.join(user_corpus)))]))
        
        scaler = pickle.load(file("scaler.pickle","r"))
        
        topic_scores = np.array([user_lda_dict.get(i, 0.0) for i in range(0, lda.num_topics)])
        topic_scores = scaler.transform(topic_scores)
    finally:
        return [], user_corpus
    
def load_model(model_path=None):
    if model_path is None:
        model_path="svm.pickle"
    model = pickle.load(file(model_path, "r"))
    return model
