#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import re
from nltk.corpus import stopwords
import ark_twokenize_py as ark

stop_punct = [ ":", '"', "'", ",", ".", "!", "-", "(", ")",
               u"—", u"…", "@", u"❤", "im", "&", "$", "%", "[", "]",
               ";", "+", "~", "...", "..", "?", "(@" ]

def remove_stop_words(tokens, stop_list=stopwords.words('english') + stop_punct):
    return [tkn for tkn in tokens if tkn not in stop_list]
    
def normalize_tokens(tokens):
    """"""
    tokens = map(unicode.lower, tokens)
    # get rid of URLs
    tokens = [re.sub(ark.url, "~url~", x, re.UNICODE) for x in tokens]
    return tokens

def full_tokenize(txt):
    return remove_stop_words(normalize_tokens(ark.tokenize(txt)))
