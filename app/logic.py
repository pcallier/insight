#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import twitter_api_func as twapi
import pickle
import numpy as np
import tokenization as tknz
import datetime
import psycopg2 as mdb
import logging
import pandas as pd
logging.basicConfig(level=logging.DEBUG)


dbuser="ubuntu"

def get_random_users(prediction=True, n=5):
    con = mdb.connect(dbname="tweets",user=dbuser)
    with con.cursor() as cur:
        cur.execute(u"SELECT name, screen_name, profile_pic_url, "
        "prediction FROM users WHERE prediction IS NOT NULL AND name "
        "IS NOT NULL AND screen_name IS NOT NULL "
        "AND profile_pic_url IS NOT NULL")
        data = np.array(cur.fetchall())
        print data.shape
        print data[[0,1,2],3]
        data = data[data[:,3]==str(prediction), :]
        print data.shape
        indices = np.random.random_integers(0, data.shape[0], n)
        print indices
        print data[indices,:].shape
        return [ { 'name': r[0].decode('utf-8') ,'screen_name': r[1].decode('utf-8'), 'profile_image_url': r[2] } for r in data[indices] if r is not None ]

def get_tweets_by_user(screen_name):
    api = twapi.get_api()
    logging.debug(screen_name)
    user_tweets = api.user_timeline(screen_name=screen_name, count=4)
    logging.debug("Number of tweets retrieved: {}".format(len(user_tweets)))
    return user_tweets
    

def vectorize_tweeter(screen_name_or_id, tweets, query_date=datetime.datetime.now()):
    """Look up tweeter by screen name. Produce feature matrix from
    their user information and timeline. Returns tuple of untransformed
    features (in a dict) and transformed features (in a numpy array"""
    
    features = { 'age' : None, 'friends_count': None, 'followers_count': None,
    'name': None, 'profile_image_url': None }
    
    try:
        api = twapi.get_api()
        user_corpus = [tweet.text for tweet in tweets]
        scaler = pickle.load(file("app/static/models/scaler.pickle","r"))
        user = api.get_user(screen_name_or_id)
        now = pd.to_datetime(datetime.datetime.now())
        age = now - user.created_at
        if age.days > 365:
            user_age = "%0.1f years" % (float(age.days) / 365.)
        elif age.days < 365:
            user_age = "%d months" % (age.days / 30)
        
        features = { 'age' :  user_age,
                     'friends_count' : user.friends_count,
                     'followers_count' : user.followers_count,
                     'name': user.name,
                     'profile_image_url': user.profile_image_url.replace(u"_normal", u"") }
        age = pd.Series(age).astype(np.int64).values[0]
        
        to_transform=np.array([features['followers_count'], 
                                    features['friends_count'], 
                                    age]).T
        logging.debug(to_transform)
        scaled_features = np.array(scaler.transform(to_transform))
    except:
        logging.warning("Exception: ", exc_info=True)
    
    finally:
        return (features, scaled_features)
    
def load_model(model_path=None):
    if model_path is None:
        model_path="app/static/models/svm1434693300.pkl"
    model = pickle.load(file(model_path, "r"))
    return model
