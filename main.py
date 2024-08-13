from flask import Flask, request
from flask_sqlalchemy import SQLAlchemy
import pandas as pd

app = Flask(__name__)

app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///database.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)


class AnualData(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    well_number = db.Column(db.String(80), unique=True, nullable=False)
    oil = db.Column(db.Integer)
    gas = db.Column(db.Integer)
    brine = db.Column(db.Integer)

    def __repr__(self):
        return f'<AnualData {self.well_number}>'

@app.route('/insert/data' , methods = ['POST'])
def add_data():
    df = pd.read_excel('20210309_2020_1 - 4 (1) (1).xls')
    df = df[['API WELL  NUMBER','OIL', 'GAS' , 'BRINE']]
    df = df.groupby('API WELL  NUMBER').sum().reset_index()
    renaming_coumns = {
    'API WELL  NUMBER' : 'well_number' ,
    'OIL':'oil',
    'GAS':'gas',
    'BRINE':'brine'
    }
    df.rename(columns=renaming_coumns,inplace=True)
    
    df.to_sql(name='anual_data',con=db.engine, if_exists='append', index=False)

    return {"msg":"Data Inserted..."}

@app.route('/data')
def get_users():
    well = request.args.get('well')
    datas = AnualData.oil.query.filter(AnualData.well_number == well).first()
    data_dict = {
            'oil': datas.oil,
            'gas': datas.gas,
            'brine': datas.brine
        }
        
    
    return data_dict
    


if __name__ == '__main__':
    with app.app_context():
        db.create_all()

    app.run(host='0.0.0.0', port=8080,debug = True)
