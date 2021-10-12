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

@app.route('/<YYYY>/<Q>/<market>/<name>/<num1>/<num2>/<num3>/<num4>/<aptnum>/<aptprice>')
def predict_sales(YYYY, Q, market, name, num1, num2, num3, num4, aptnum, aptprice):
    with open('pipe.pickle', 'rb') as p:
        pipe = pickle.load(p)
    with open('model.pickle', 'rb') as m:
        model = pickle.load(m)
    with open('columns.pickle', 'rb') as c:
        col = pickle.load(c)
    
    data = [[YYYY, Q, market, name, num1, num2, num3, num4, aptnum, aptprice]]
    df = pd.DataFrame(data, columns = col)
    x_test = pipe.transform(df)
    pred = model.predict(x_test)
    price = round(float(pred))
    price = format(price, ",")

    first = f'{name}의 {YYYY}년도 {Q}분기 예상 총 매출액은 {price}원 입니다.'
    scd = f'[입력하신 예상 정보]\n- num1:{num1}\n- num2:{num2}\n- num3: {num3}\n- aptnum: {aptnum}\n- aptprice: {aptprice}'
    return first
    


if __name__ == "__main__":
    app.run(debug=True)
