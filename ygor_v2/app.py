from flask import Flask, abort, jsonify
import sqlite3
from flask_cors import CORS

app = Flask(__name__)
CORS(app)


def dictfetchall(cursor):
    # from https://docs.djangoproject.com/en/2.1/topics/db/sql/
    columns = [col[0] for col in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


@app.errorhandler(404)
def resource_not_found(e):
    return jsonify(error=str(e)), 404


@app.route("/characters")
def characterList():
    conn = sqlite3.connect('marvel.db')
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM character ORDER BY name')

    data = dictfetchall(cursor)
    return {
        'next': '',
        'previous': '',
        'data': data,
        'count': len(data),
    }


@app.route("/characters/<id>")
def characterDetail(id):
    conn = sqlite3.connect('marvel.db')
    cursor = conn.cursor()
    cursor.execute(
        'SELECT * FROM character WHERE id = :id LIMIT 1',
        {'id': id},
    )

    data = dictfetchall(cursor)
    if not data:
        abort(404, description="Resource not found")
    return data[0]
