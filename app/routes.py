from flask import request, jsonify
from .data_processed import get_well_data
import sqlite3


def init_routes(app):
    @app.route('/data', methods=['GET'])
    def get_annual_data():
        api_well_number = request.args.get('well')
        if not api_well_number:
            return jsonify({'error': 'API well number is required'}), 400

        data = get_well_data(api_well_number)
        if not data:
            return jsonify({'error': 'Data not found for the given API well number'}), 404

        return jsonify(data), 200

    @app.route('/debug_data', methods=['GET'])
    def debug_data():
        conn = sqlite3.connect('production_data.db')
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM annual_production')
        all_data = cursor.fetchall()
        cursor.close()
        conn.close()

        formatted_data = [
            {
                'api_well_number': row[0],
                'oil': row[1],
                'gas': row[2],
                'brine': row[3]
            }
            for row in all_data
        ]

        return jsonify(formatted_data)