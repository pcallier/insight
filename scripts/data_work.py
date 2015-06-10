#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import sys
import os
import logging
logging.basicConfig(level=logging.DEBUG)
import random
from collections import Counter
import psycopg2 as mdb
import numpy as np
import pandas as pd
import lda
#import twitter_api_func as twapi
import shared_utilities as shdb
import ark_twokenize_py as ark
import sklearn.feature_extraction.text as xtxt

def tokenize_tweets(con, tbl="tweets", id_col="tweet_id", 
                    src_col="text", dest_col="tokens"):
    """Tokenize all the entries in column src_col of table tbl in con.
    record results in column dest_col"""
    
    with con.cursor() as cur:
        cur.execute(u"SELECT {0}, {1} FROM {2}".format(
            id_col,
            src_col,
            tbl))
        tweets = cur.fetchall()
        for tw_id, tw_txt in tweets:
            try:
                tokenize_and_add(cur, tw_id, tw_txt, tbl, id_col, dest_col)
                con.commit()
            except KeyboardInterrupt:
                con.rollback()
                return
            except:
                logging.warning("Unable to tokenize", exc_info=True)
                con.rollback()
            
            
def tokenize_and_add(cur, tweet_id, tweet_text, tbl, id_col, col):
    """Given a cursor in the db, update the value of a given column given
    a certain value of a certain id column with the tokenized 
    version of the input text, separated by spaces"""
    tokens = ark.tokenize(tweet_text.decode('utf-8'))
    #logging.debug(tokens)
    
    cur.execute(u"UPDATE {0} SET {1}='{2}' WHERE {3}='{4}'".format(
        tbl,
        col,
        u' '.join(tokens),
        id_col,
        tweet_id))

def normalize_tokens(tokens):
    """Placeholder for token normalization fn, not yet implemented"""
    pass
    
def get_vocab(docs, n=5000):
    """Get top n most frequent tokens in the document set docs"""
    #join docs
    docs_txt = u'\n'.join(docs)
    doc_tokens = docs_txt.split()
    return np.array(zip(*Counter(doc_tokens).most_common(n)))[0]

 
    
def lda_authors(con):
    """Do LDA on db, with authors as documents. For now, output 
    some version of results to stdout"""
    
    def concat_tokens(tkns):
        try:
            return u'\n'.join(tkns.tokens.where(tkns.tokens.notnull(),u"").tolist())
        except TypeError:
            return ""
    
    with con.cursor() as cur:
        cur.execute("SELECT user_id, tokens FROM tweets")
        documents = pd.DataFrame(cur.fetchall(), columns=['user_id',
            'tokens'])
        documents.tokens = documents.tokens.dropna().map(lambda x: x.decode('utf-8'))
        doc_series = documents.groupby('user_id').apply(concat_tokens)
        
        #ranuser = random.sample(doc_df.index, 1)[0]
        #print ranuser
        #print documents[documents.user_id==ranuser].values
        #print doc_df[ranuser]
        
        vocab = get_vocab(doc_series.values, 5000)
        vectorizer = xtxt.CountVectorizer(tokenizer=lambda x: x.split(), 
                             vocabulary=vocab, binary=True)
        terms = vectorizer.fit_transform(doc_series.values)
        logging.debug(doc_series[0])
        logging.debug([vocab[i] for i in terms[0] if terms[0][i] != 0])
        logging.debug(terms.shape)
        
if __name__ == "__main__":
    tokenize_tweets(shdb.connect_db())