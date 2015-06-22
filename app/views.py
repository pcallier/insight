#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import sys
import os
from flask import render_template , request, url_for, redirect
from app import app
import logging
logging.basicConfig(level=logging.DEBUG)

import psycopg2 as mdb
from logic import vectorize_tweeter, load_model, get_tweets_by_user, \
    get_random_users

dbuser="ubuntu"
db= mdb.connect(dbname="tweets", user=dbuser)
svm = load_model()  
    
@app.route('/')
@app.route('/index')
def index():
    return redirect(url_for('app_dashboard'))

@app.route("/app/")
def app_dashboard():
    random_users = get_random_users()
    return render_template('dashboard.html', users=random_users)

@app.route("/user", methods=["GET"])
def redirect_to_user():
    screen_name = request.args.get('screen_name')
    logging.debug(screen_name)
    return redirect(url_for('twitter_user_view', screen_name=screen_name))

@app.route("/user/<screen_name>")
def twitter_user_view(screen_name=None):
    
    if screen_name == "":
        screen_name=None
    try:
        tweets = get_tweets_by_user(screen_name)
        logging.debug(screen_name)
        features, scaled_features = vectorize_tweeter(screen_name, tweets)
        will_churn=svm.predict(scaled_features)
        error_txt = None

    except Exception as e:
        error_txt="Error"
        logging.debug("Error: ", exc_info=True)
        raise
        
    return render_template('twitter-user.html',
                           twitter_name=features['name'],
                           screen_name=screen_name,
                           tweet_list=tweets,
                           user=features,
                           will_churn=will_churn,
                           error_txt=error_txt)
                           
