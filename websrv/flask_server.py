#! /usr/bin/env python

import pandas as pd
import os
from flask import Flask, jsonify
import pickle
from GeoBases import GeoBase

geo_o = GeoBase(data='ori_por', verbose=False)
sorted_airports = None
app = Flask(__name__)

CURR_DIR = os.path.dirname(__file__)
@app.route('/top_airports/<n>')
def topn(n):
    try:
        n = int(n)
    except:
        return jsonify({"status": "failure", "message": "Invalid numerical value"})
    data = []
    rank = 1
    for index, row in sorted_airports[:n].iterrows():
        data.append({ "rank" : rank, "code": index, "pax": int(row[0]), "name": geo_o.get(index, 'name')})
        rank +=1
    return jsonify({"status": "success", "airports": data})


@app.errorhandler(404)
def page_not_found(e):
    return ''''Page not found.<br>
URL to top N airports:<br><br>
/top_airports/N'''

def load_from_pickle(fn):
    try:
        return pickle.load(open(fn))
    except:
        print "Pickle does not exist or corrupted, reloading..."
        CHUNKSIZE=100000
        pieces = [pd.DataFrame(chunk)
                  for chunk in pd.read_csv(os.path.join(CURR_DIR, "..", "bookings.csv"),
                                           chunksize = CHUNKSIZE,
                                           delimiter = r" *\^",
                                           usecols=["arr_port", "pax"])]
        df = pd.concat(pieces)
        df = df.dropna()
        res = df.groupby("arr_port").sum().sort('pax', ascending=False)
        pickle.dump(res, open(fn, "wb"))
        return res

if __name__ == '__main__':
    pickle_file = os.path.join(CURR_DIR, "airports.pickle")
    sorted_airports = load_from_pickle(pickle_file)
    app.run(host = "0.0.0.0")
