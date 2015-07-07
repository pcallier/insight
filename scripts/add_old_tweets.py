#!/usr/bin/env python

import sys
import psycopg2 as ppg
import pandas as pd

def insert_row(cur, row):
    cur.execute(("INSERT INTO tweets (text, tokens, tweet_id, user_id, long, lat, language, created_at) VALUES ("
    "$blah${}$blah$, $blah${}$blah$, '{}', '{}', {}, {}, '{}', '{}')").format(row.text, row.tokens, row.tweet_id, row.user_id, row.long, row.lat, row.lang, row.created_at))

def update_row(cur, row):
    cur.execute(("UPDATE tweets SET (text, tokens, user_id, long, lat, language, created_at) = ("
    "$blah${}$blah$, $blah${}$blah$, '{}', {}, {}, '{}', '{}') "
    "WHERE tweet_id = '{}'").format(row.text, row.tokens, row.user_id, row.long, row.lat, row.lang, row.created_at, row.tweet_id))


def insert_user(cur, row):
    cur.execute(("INSERT INTO users (user_id, followers_count, friends_count, name, place, created_at) VALUES ("
    "'{}', {}, {}, '{}', $blah${}$blah$, '{}')").format(row.user_id, row.followers_count, row.friends_count, row.name, row.location, row.created_at))

def update_user(cur, row):
    cur.execute(("UPDATE users SET (followers_count, friends_count, name, place, created_at) = ("
    "{}, {}, '{}', $blah${}$blah$, '{}') "
    "WHERE user_id = '{}'").format(row.followers_count, row.friends_count, row.name, row.location, row.created_at, row.user_id))


def add_tweets():
    con = ppg.connect(dbname="oldtweets", user="patrick")
    with con.cursor() as cur:
        cur.execute("SELECT text, tokens, tweet_id, user_id, long, lat, lang, created_at FROM tweets")
        old_df = pd.DataFrame(cur.fetchall(), columns = [c[0] for c in cur.description])
    con.close()

    con = ppg.connect(dbname="tweets", user="patrick")
    with con.cursor() as cur:
        for i,row in old_df.iterrows():
            try:
                insert_row(cur, row)
                con.commit()
            except ppg.IntegrityError:
                con.rollback()
                update_row(cur, row)
                con.commit()
            except IndexError:
                print row
                raise
                
def add_users():
    con = ppg.connect(dbname="oldtweets", user="patrick")
    with con.cursor() as cur:
        cur.execute("SELECT user_id, followers_count, friends_count, name, location, created_at FROM users")
        old_df = pd.DataFrame(cur.fetchall(), columns = [c[0] for c in cur.description])
    con.close()

    con = ppg.connect(dbname="tweets", user="patrick")
    with con.cursor() as cur:
        for i,row in old_df.iterrows():
            try:
                insert_user(cur, row)
                con.commit()
            except ppg.IntegrityError:
                con.rollback()
                update_user(cur, row)
                con.commit()
            except IndexError:
                print row
                raise
                
if __name__ == '__main__': 
    if sys.argv[1] == 'addusers':
        add_users()
    elif sys.argv[1] == 'addtweets':
        add_tweets()