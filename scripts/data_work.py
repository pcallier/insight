#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import sys
import os
import logging
logging.basicConfig(level=logging.DEBUG)
import random
import re
from collections import Counter
import psycopg2 as mdb
import numpy as np
import pandas as pd
import lda
from nltk.corpus import stopwords
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
    tokens = map(unicode.lower, tokens)
    # get rid of URLs
    tokens = [re.sub(ark.url, "~url~", x, re.UNICODE) for x in tokens]
    return tokens
    
    
def get_vocab(docs, n=5000):
    """Get top n most frequent tokens in the document set docs"""
    #join docs
    docs_txt = u'\n'.join(docs)
    doc_tokens = docs_txt.split()
    return np.array(zip(*Counter(doc_tokens).most_common(n)))[0]

 

def get_corpus(con):
    def concat_tokens(tkns):
        try:
            return u'\n'.join(tkns.tokens.where(tkns.tokens.notnull(),u"").tolist())
        except TypeError:
            return ""
            
    with con.cursor() as cur:
        cur.execute("SELECT user_id, tokens FROM tweets")
        documents = pd.DataFrame(cur.fetchall(), columns=['user_id',
            'tokens']).dropna()         # watch out, indiscriminately dropping rows
            
        # normalize (to lowercase)
        documents.tokens = normalize_tokens(map(lambda x: x.decode('utf-8'), documents.tokens))
        
        #sample users
        #random.seed(500)
        #user_sample = random.sample(documents.user_id, 2000)
        #documents = documents[documents.user_id.isin(user_sample)]
        #documents.tokens = documents.tokens.dropna().map(lambda x: x.decode('utf-8'))
        doc_series = documents.groupby('user_id').apply(concat_tokens)
        
        return doc_series

    
def lda_authors(doc_array):
    """Do LDA on db, with authors as documents. For now, output 
    some version of results to stdout"""
    
    vocab = get_vocab(doc_array, 5000)
    # use NLTK stopwords
    vocab = np.array([v for v in vocab if v not in stopwords.words('english')])
    vectorizer = xtxt.CountVectorizer(tokenizer=lambda x: x.split(), 
                         vocabulary=vocab, binary=True)
    terms = vectorizer.fit_transform(doc_array)
    logging.debug(terms.shape)
    
    # do LDA (see https://pypi.python.org/pypi/lda)
    model = lda.LDA(n_topics=40, n_iter=2000, random_state=1)
    model.fit(terms)
    return model, vocab

def display_lda(model, vocab, n_top_words = 8, n_top_topics=3):
    topic_word = model.topic_word_
    # loop over fitted probabilities for vocab items in each topic
    for i, topic_dist in enumerate(topic_word):
        # get top words for topic, having sorted by ascending probability
        topic_words = vocab[np.argsort(topic_dist)][:-n_top_words-1:-1]
        print u'Topic {}: {}'.format(i, u' '.join(topic_words)) 
    for i, doc_dist in enumerate(model.doc_topic_):
        # get top topics per document
        print "Document ", i, ":"
        topic_docs = np.argsort(doc_dist)[:n_top_topics-1:-1]
        for k in xrange(0, n_top_topics):
            print np.argsort(doc_dist)[-k]
            print vocab[np.argsort(model.topic_word_[np.argsort(doc_dist)[-k]])][:-n_top_words:-1]
        
        
def doctopic_to_features(con, corpus, model):
    """Create matrix of features for each document (author) with 
    n columns wher en is the number of topics, valued as the probability 
    of that topic for that author (first column will be doc/author id).
    
    Write that to the given connection"""
    
    feature_matrix = np.concatenate([np.array(corpus.index, ndmin=2).T, 
                                     model.doc_topic_], axis=1)
    with con.cursor() as cur:
        for doc_row_i in range(0,feature_matrix.shape[0]):
            set_statements =  ', '.join( ["topic_{} = {}".format(x,y) 
                for x,y in zip(range(0, feature_matrix.shape[1]), 
                feature_matrix[doc_row_i,1:]) ])
            #logging.debug(set_statements)
            query = "UPDATE users SET " + \
                set_statements + \
                " WHERE user_id='{}'".format(feature_matrix[doc_row_i, 0])
            cur.execute(query)
            con.commit()
    
    
        
if __name__ == "__main__":
    tokenize_tweets(shdb.connect_db())