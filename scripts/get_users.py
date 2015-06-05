#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import sys
import os
import tweepy
from twitter_api_func import get_api
import logging
logging.basicConfig(level=logging.WARNING)
import psycopg2 as mdb
import re


def normalize_text(text):
    blank_these = re.compile(r"(')+")
    space_these = re.compile(r"(\s+)")
    return space_these.sub(" ", blank_these.sub("", text))


def connect_db():
    con = mdb.connect("dbname=tweets user=patrick")
    assert con is not None
    return con

def get_missing_users(con):
    with con.cursor() as cur:
        cur.execute("SELECT DISTINCT user_id FROM tweets WHERE "
                    "user_id NOT IN (SELECT user_id FROM users)")
        logging.info(cur.rowcount)
        missing_user_ids = zip(*cur.fetchall())[0]
        return missing_user_ids

def add_users(ids, api, con):
    with con.cursor() as cur:
        while len(ids) > 0:
            ids_to_look_up = ids[0:min(len(ids),100)]
            try:
                ids = ids[100:]
            except:
                ids = []
            user_result = api.lookup_users(ids_to_look_up)
            for user in user_result:
                try:
                    cur.execute((u"INSERT INTO users (user_id, screen_name, name,"
                            u"followers_count, friends_count, description,"
                            u"timezone) VALUES"
                            u" ('{}', '{}', '{}', '{}','{}','{}','{}')").format(
                            user.id_str,
                            user.screen_name,
                            normalize_text(user.name),
                            user.followers_count,
                            user.friends_count,
                            normalize_text(getattr(user,'description','')),
                            normalize_text(getattr(user,'timezone',''))))
                except mdb.IntegrityError:
                    logging.warning("User already exists")
                    con.rollback()
        con.commit()
                        
                        
