from flask import Flask, request, jsonify,Blueprint
from sqlalchemy.dialects.postgresql import psycopg2

from models.mission import mission
from sqlalchemy.orm import sessionmaker
from db import db, get_db_connection

bp_missions = Blueprint('missions', __name__)





@bp_missions.route('/<int:mission_id>', methods=['GET'])
def get_mission_by_id( mission_id):
    try:
        conn_new = get_db_connection()
        cur = conn_new.cursor()
        query = """
            SELECT *
            FROM mission
            WHERE mission_id = %s
        """

        cur.execute(query, (mission_id,))
        mission = cur.fetchone()
        if mission:
            mission_data = {
                'mission_id': mission[0],
                'mission_date': mission[1],
                'theater_of_operations': mission[2],
                'country': mission[3],
                'air_force': mission[4],
                'unit_id': mission[5],
                'aircraft_series': mission[6],
                'callsign': mission[7],
                'mission_type': mission[8],
                'takeoff_base': mission[9],
                'takeoff_location': mission[10],
                'takeoff_latitude': mission[11],
                'takeoff_longitude': mission[12],
                'target_key': mission[13],
                'target_id': mission[14],
                'altitude_hundreds_of_feet': mission[15],
                'airborne_aircraft': mission[16],
                'attacking_aircraft': mission[17],
                'bombing_aircraft': mission[18],
                'aircraft_returned': mission[19],
                'aircraft_failed': mission[20],
                'aircraft_damaged': mission[21],
                'aircraft_lost': mission[22],
                'high_explosives': mission[23],
                'high_explosives_type': mission[24],
                'high_explosives_weight_pounds': mission[25],
                'high_explosives_weight_tons': mission[26],
                'incendiary_devices': mission[27],
                'incendiary_devices_type': mission[28],
                'incendiary_devices_weight_pounds': mission[29],
                'incendiary_devices_weight_tons': mission[30],
                'fragmentation_devices': mission[31],
                'fragmentation_devices_type': mission[32],
                'fragmentation_devices_weight_pounds': mission[33],
                'fragmentation_devices_weight_tons': mission[34],
                'total_weight_pounds': mission[35],
                'total_weight_tons': mission[36],
                'time_over_target': mission[37],
                'bomb_damage_assessment': mission[38],
                'source_id': mission[39],
            }
            return mission_data
        else:
            return None

    except Exception as e:
        print(f"An error occurred: {e}")
        return None

    finally:
        if cur:
            cur.close()



@bp_missions.route('/missions', methods=['GET'])
def get_all_missions():
    conn_new = get_db_connection()
    cur = conn_new.cursor()
    conn_new.commit()
    my_list =[]
    cur.execute("SELECT * FROM mission")
    rows = cur.fetchall()
    for row in rows:
        my_list.append(row)
    if mission:
        return my_list
    else:
        return jsonify({'error': 'Mission not found'}), 404
