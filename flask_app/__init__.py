import os
from flask import Flask, render_template, jsonify
import pickle
import pandas as pd


app = Flask(__name__)


@app.route('/')
def index():
    return render_template('index.html')

@app.route('/list')
def name_list():
    with open('market_name.pickle', 'rb') as p:
        market_name = pickle.load(p)    
    return render_template('name_lst.html', name_list = market_name)

@app.route('/<YYYY>/<Q>/<market>/<name>')
def predict_sales(YYYY, Q, market, name):
    num1 = '업종수'
    num2 = '점포수'
    people = '총_생활인구'
    working = '총_직장인구'
    house = '아파트_단지수'
    house_price = '아파트_평균_시가'

    model_list = [num1, num2, people, working, house, house_price]
    
    data = [[YYYY, Q, market, name]]
    df_more = pd.DataFrame(data, columns = ['YYYY', 'Q', '상권_구분명', '상권명'])
    
    with open('pipe_more.pickle', 'rb') as pm:
        pipe_more = pickle.load(pm)
    
    x_test_more = pipe_more.transform(df_more)

    for i in model_list:
        with open(f'model_more_{i}.pickle', 'rb') as k:
            model_more = pickle.load(k)
        pred_more = model_more.predict(x_test_more)

        data[0].append(pred_more)
        

    with open('pipe.pickle', 'rb') as p:
        pipe = pickle.load(p)
    with open('model.pickle', 'rb') as m:
        model = pickle.load(m)
    with open('columns.pickle', 'rb') as c:
        col = pickle.load(c)
    
    df = pd.DataFrame(data, columns = col)
    x_test = pipe.transform(df)
    pred = model.predict(x_test)
    price = round(float(pred))
    price = format(price, ",")
                
    data = data[0]
    return render_template('result.html', YYYY=data[0], Q = data[1],
    market = data[2], name = data[3], num1 = format(int(data[4]), ","), num2 = format(int(data[5]), ","),
    people = format(int(data[6]), ","), working = format(int(data[7]), ","), house = format(int(data[8]), ","),
    house_price = format(int(data[9]), ","),
    price = price)
    


if __name__ == "__main__":
    app.run(debug=True)
