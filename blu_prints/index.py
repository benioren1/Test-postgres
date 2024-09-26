from flask import flask, Blueprint, request

from db import get_db_connection

bp_index = Blueprint('index', __name__)

@bp_index.route('/index',methods = ['POST'])
def create_index():
    val = {"mission_id": "mission_id"}
    index = request.args
    to_index = index['to_index']
    try:
        if to_index in val:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute(f'''CREATE INDEX my_index
                                on mission( {val[to_index]})
                        ''')
            conn.commit()
            cursor.close()
            return "was index in succsafuly",200
        else:
            return "nooooo good what are you doing",400
    except Exception as e:
        return str(e),400
