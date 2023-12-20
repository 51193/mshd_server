import json
import math

from flask import Flask, render_template, request, make_response

from Mysql import Mysql
from flask_cors import *

app = Flask(__name__)

CORS(app, supports_credentials=True, resources=r"/*")


@app.route("/select", methods=['GET', 'POST'])
def select():
    # 调用
    db = Mysql()
    results = db.get_data()
    return results[0][5]


@app.route("/province", methods=['GET', 'POST'])
def province():
    db = Mysql()
    return list(db.get_province())


@app.route("/city", methods=['GET', 'POST'])
def city():
    if request.method == "POST":
        data = request.get_data()
        json_data = json.loads(data.decode("utf-8"))
        db = Mysql()
        if len(json_data.get('province')) == 1:
            return list(db.get_city(json_data.get('province')[0]))
        else:
            return ['Invalid Input']
    else:
        return ['Invalid Request Method']


@app.route("/county", methods=['GET', 'POST'])
def county():
    if request.method == "POST":
        data = request.get_data()

        json_data = json.loads(data.decode("utf-8"))
        db = Mysql()
        if len(json_data.get('city')) == 1 and len(json_data.get('province')) == 1:
            return list(db.get_county(json_data.get('province')[0], json_data.get('city')[0]))
        else:
            return ['Invalid Input']
    else:
        return ['Invalid Request Method']


@app.route("/town", methods=['GET', 'POST'])
def town():
    if request.method == "POST":
        data = request.get_data()

        json_data = json.loads(data.decode("utf-8"))
        db = Mysql()
        if len(json_data.get('city')) == 1 and len(json_data.get('province')) == 1 and len(
                json_data.get('county')) == 1:
            return list(db.get_town(json_data.get('province')[0], json_data.get('city')[0], json_data.get('county')[0]))
        else:
            return ['Invalid Input']
    else:
        return ['Invalid Request Method']


@app.route("/village", methods=['GET', 'POST'])
def village():
    if request.method == "POST":
        data = request.get_data()

        json_data = json.loads(data.decode("utf-8"))
        db = Mysql()
        if len(json_data.get('city')) == 1 and len(json_data.get('province')) == 1 and len(
                json_data.get('county')) == 1 and len(
            json_data.get('town')) == 1:
            return list(
                db.get_village(json_data.get('province')[0], json_data.get('city')[0], json_data.get('county')[0],
                               json_data.get('town')[0]))
        else:
            return ['Invalid Input']
    else:
        return ['Invalid Request Method']


@app.route("/user/login", methods=['GET', 'POST'])
def user_login():
    if request.method == "POST":
        data = request.get_data()
        json_data = json.loads(data.decode("utf-8"))
        db = Mysql()

        if json_data.get('uid') and json_data.get('password'):
            return {'result': db.verify_password(json_data.get('uid'), json_data.get('password'))}
        else:
            return {'result': 'Invalid Input'}
    else:
        return {'result': 'Invalid Request Method'}


@app.route("/admin/login", methods=['GET', 'POST'])
def admin_login():
    if request.method == "POST":
        data = request.get_data()
        json_data = json.loads(data.decode("utf-8"))
        db = Mysql()

        if json_data.get('uid') and json_data.get('password'):
            return {'result': db.verify_admin(json_data.get('uid'), json_data.get('password'))}
        else:
            return {'result': 'Invalid Input'}
    else:
        return {'result': 'Invalid Request Method'}


@app.route("/user/update_password", methods=['GET', 'POST'])
def update_password():
    if request.method == "POST":
        data = request.get_data()
        json_data = json.loads(data.decode("utf-8"))
        db = Mysql()

        if json_data.get('uid') and json_data.get('password') and json_data.get('newPassword'):
            return {'result': db.update_password(json_data.get('uid'), json_data.get('password'),
                                                 json_data.get('newPassword'))}
        else:
            return {'result': 'Invalid Input'}
    else:
        return {'result': 'Invalid Request Method'}


@app.route("/user/delete_user", methods=['GET', 'POST'])
def delete_user():
    if request.method == "POST":
        data = request.get_data()
        json_data = json.loads(data.decode("utf-8"))
        db = Mysql()

        if json_data.get('uid') and json_data.get('password') and json_data.get('deleteUser'):
            return {'result': db.delete_user(json_data.get('uid'), json_data.get('password'),
                                             json_data.get('deleteUser'))}
        else:
            return {'result': 'Invalid Input'}
    else:
        return {'result': 'Invalid Request Method'}


@app.route("/user/add_user", methods=['GET', 'POST'])
def add_user():
    if request.method == "POST":
        data = request.get_data()
        json_data = json.loads(data.decode("utf-8"))
        db = Mysql()

        if json_data.get('adminId') and json_data.get('adminPassword') and json_data.get('password') and json_data.get(
                'uid'):
            return {'result': db.add_user(json_data.get('adminId'), json_data.get('adminPassword'),
                                          json_data.get('uid'), json_data.get('password'))}
        else:
            return {'result': 'Invalid Input'}
    else:
        return {'result': 'Invalid Request Method'}


@app.route("/disaster_code/submit", methods=['GET', 'POST'])
def disaster_code_submit():
    if request.method == "POST":
        data = request.get_data()
        json_data = json.loads(data.decode("utf-8"))
        db = Mysql()

        print(json_data)

        if json_data.get('province') and json_data.get('city') and json_data.get('county') and json_data.get(
                'town') and json_data.get('village') and json_data.get('source_type') and json_data.get(
            'source_subtype') and json_data.get('carrier') and json_data.get('disaster_type') and json_data.get(
            'disaster_subtype') and json_data.get('disaster_index') and json_data.get('date') and json_data.get('time') \
                and json_data.get('have_file'):
            if json_data.get('description'):
                return {'result': db.insert_disaster_code(json_data.get('province'), json_data.get('city'),
                                                          json_data.get('county'),
                                                          json_data.get(
                                                              'town'), json_data.get('village'), json_data.get('date'),
                                                          json_data.get('time'),
                                                          json_data.get('source_type'),
                                                          json_data.get(
                                                              'source_subtype'), json_data.get('carrier'),
                                                          json_data.get('disaster_type'),
                                                          json_data.get(
                                                              'disaster_subtype'), json_data.get('disaster_index'),
                                                          json_data.get('have_file'), json_data.get('description'))}
            else:
                return {'result': db.insert_disaster_code(json_data.get('province'), json_data.get('city'),
                                                          json_data.get('county'),
                                                          json_data.get(
                                                              'town'), json_data.get('village'), json_data.get('date'),
                                                          json_data.get('time'),
                                                          json_data.get('source_type'),
                                                          json_data.get(
                                                              'source_subtype'), json_data.get('carrier'),
                                                          json_data.get('disaster_type'),
                                                          json_data.get(
                                                              'disaster_subtype'), json_data.get('disaster_index'),
                                                          json_data.get('have_file'))}
        else:
            return {'result': 'Invalid Input'}

    else:
        return {'result': 'Invalid Request Method'}


@app.route("/disaster_code/delete", methods=['GET', 'POST'])
def disaster_code_delete():
    if request.method == "POST":
        data = request.get_data()
        json_data = json.loads(data.decode("utf-8"))
        db = Mysql()

        if json_data.get('disaster_code') :
            return {'result': db.delete_disaster_code(json_data.get('disaster_code'))}
        else:
            return {'result': 'Invalid Input'}
    else:
        return {'result': 'Invalid Request Method'}


@app.route("/disaster_code/yearly", methods=['GET', 'POST'])
def disaster_code_yearly():
    if request.method == "POST":
        data = request.get_data()
        json_data = json.loads(data.decode("utf-8"))

        db = Mysql()

        return db.get_yearly(json_data.get('startYear'), json_data.get('endYear'))
    else:
        return {'result': 'Invalid Request Method'}


@app.route("/disaster_code/province", methods=['GET', 'POST'])
def disaster_code_province():
    db = Mysql()
    return db.get_each_province()


color = [
    '#8C1EB2',
    '#8C1EB2',
    '#DA05AA',
    '#F0051A',
    '#FF2A3C',
    '#FF4818',
    '#FF4818',
    '#FF8B18',
    '#F77B00',
    '#ED9909',
    '#ECC357',
    '#EDE59C'
]


@app.route("/disaster_code/map", methods=['GET', 'POST'])
def disaster_code_map():
    db = Mysql()
    items = db.get_each_province()
    with open('province.geojson', 'r', encoding='utf-8') as f:
        data = json.load(f)
    for i in data['features']:
        for j in items:
            if i['properties']['name'] == j['province']:
                i['properties']['disaster_count'] = j['value']
                i['properties']['color'] = color[int(j['value'] / 100)]

    return data


@app.route("/disaster_code/display", methods=['GET', 'POST'])
def disaster_code_display():
    if request.method == "POST":
        data = request.get_data()
        json_data = json.loads(data.decode("utf-8"))

        db = Mysql()
        count = db.get_disaster_code_count()
        items = db.display(json_data.get('pageSize'), json_data.get('pageIndex'))

        return {'count': count, 'items': items}

    else:
        return {'result': 'Invalid Request Method'}


@app.route("/disaster_code/export", methods=['GET', 'POST'])
def disaster_code_export():
    db = Mysql()
    response = make_response(db.export_disaster_code())
    response.headers['Content-type'] = 'application/vnd.ms-excel'
    response.headers['Content-Disposition'] = 'attachment; filename=disaster_code.xls'
    return response


@app.route("/ping", methods=['GET', 'POST'])
def ping():
    if request.method == "POST":
        data = request.get_data()
        json_data = json.loads(data.decode("utf-8"))
        print(json_data)
        return {'result': 'Success'}
    else:
        return {'result': 'Invalid Request Method'}


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)
