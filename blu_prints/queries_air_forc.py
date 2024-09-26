from db import get_db_connection
from flask import Flask, request, jsonify,Blueprint

bp_query = Blueprint('query', __name__)


@bp_query.route('/by_year/<int:year>', methods=['GET'])
def get_active_air_force_by_year(year):
    connection = get_db_connection()
    try:
        cursor = connection.cursor()

        query = """
            SELECT air_force, takeoff_location, COUNT(*) AS mission_count
            FROM mission
            WHERE EXTRACT(YEAR FROM mission_date) = %s
            GROUP BY air_force, takeoff_location
            ORDER BY mission_count DESC;
        """

        cursor.execute(query, (year,))
        results = cursor.fetchall()

        active_air_forces = []
        for row in results:
            active_air_forces.append({
                'air_force': row[0],
                'takeoff_location': row[1],
                'mission_count': row[2]
            })

        return active_air_forces

    except Exception as e:
        print(f"An error occurred: {e}")
        return None

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()





