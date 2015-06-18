#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import twitter_api_func as twapi
import pickle
import numpy as np
import tokenization as tknz
import datetime


def get_tweets_by_user(screen_name):
    api = twapi.get_api()
    user_tweets = api.user_timeline(screen_name=screen_name, count=4)
    return user_tweets
    

def vectorize_tweeter(screen_name_or_id, tweets, query_date=datetime.datetime.now()):
    """Look up tweeter by screen name. Produce feature matrix from
    their user information and timeline. Returns tuple of untransformed
    features (in a dict) and transformed features (in a numpy array"""
    
    features = { 'age' : None, 'friends_count': None, 'followers_count': None }
    
    try:
        api = twapi.get_api()
        user_corpus = [tweet.text for tweet in tweets]
        scaler = pickle.load(file("scaler.pickle","r"))
        user = api.get_user(screen_name_or_id)
        age = datetime.user.created_at
        if age.days > 365:
            user_age = "%0.1f years" % (float(age.days) / 365.)
        elif age.days < 365:
            user_age = "%d months" % (age.days / 30)
        features = { 'age' :  user_age,
                     'friends_count' : user.friends_count,
                     'followers_count' : user.followers_count }
        scaled_features = scaler.transform(np.array(features['followers_count'], 
                                    features['friends_count'], 
                                    features['age']))
    finally:
        return (features, scaled_features)
    
def load_model(model_path=None):
    if model_path is None:
        model_path="svm.pickle"
    model = pickle.load(file(model_path, "r"))
    return model
