#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from flask import render_template , request, url_for, redirect
from app import app
import psycopg2 as mdb
from logic import vectorize_tweeter, load_model

db= mdb.connect("dbname=tweets user=patrick")
svm = load_model()  
    
@app.route('/')
@app.route('/index')
def index():
    return redirect(url_for('twitter_user_view'))

@app.route("/app/")
@app.route("/app/<screen_name>")
def twitter_user_view(screen_name=None):
    if screen_name == "":
        screen_name=None
    try:
        features, tweets = vectorize_tweeter(screen_name)
        #on_road=svm.predict(features)
        error_txt = None
    except TypeError:
        tweets = None
        on_road = None
        error_txt = "Type error"
    except Exception as e:
        tweets = None
        on_road = None
        error_txt =  str(e)
        
    return render_template('twitter-user.html', 
                           screen_name=screen_name,
                           tweets=tweets,
                           on_road=False,
                           error_txt=error_txt)