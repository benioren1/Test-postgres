from flask import Flask
from db import db
from blu_prints.missions import bp_missions
from blu_prints.queries_air_forc import bp_query
from blu_prints.index import bp_index
app = Flask(__name__)

app.config['SQLALCHEMY_DATABASE_URI'] ='postgresql://postgres:1234@localhost/normal_wwii_mission'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db.init_app(app)

@app.route('/')
def hello_world():  # put application's code here
    return 'Hello World!'

app.register_blueprint(bp_missions, url_prefix='/api/mission')
app.register_blueprint(bp_query, url_prefix='/api/query')
app.register_blueprint(bp_index, url_prefix='/api/index')



if __name__ == '__main__':
    app.run(debug=True)
