import random
from io import BytesIO

import pandas
import pymysql
import xlwt


class Mysql(object):
    def __init__(self):
        # try:
        self.db = pymysql.connect(port=3306, host="crepusculumx.icu", user="test_admin", password="123456",
                                  database="mshd",
                                  autocommit=True)
        # 游标对象
        self.cursor = self.db.cursor()

    def randomly_insert_disaster_code(self, count):

        time = 0

        for i in range(count):
            sql = "SELECT id FROM mshd.region_code ORDER BY RAND() LIMIT 1;"
            self.cursor.execute(sql)
            result = self.cursor.fetchall()
            region_id = result[0][0]
            disaster_code = str(region_id)

            year = random.randint(2000, 2022)
            month = str(random.randint(1, 12))
            month = month.zfill(2)
            day = str(random.randint(1, 28))
            day = day.zfill(2)
            hour = str(random.randint(0, 23))
            hour = hour.zfill(2)
            minute = str(random.randint(0, 59))
            minute = minute.zfill(2)
            second = str(random.randint(0, 59))
            second = second.zfill(2)
            time_code = str(year) + str(month) + str(day) + str(hour) + str(minute) + str(second)
            disaster_code += str(time_code)

            source_id = random.randint(1, 17)
            sql = "SELECT type_code, subtype_code FROM mshd.source_code where id='" + str(source_id) + "';"
            self.cursor.execute(sql)
            result = self.cursor.fetchall()
            disaster_code += str(result[0][0]) + str(result[0][1])

            carrier_id = random.randint(1, 5)
            sql = "SELECT carrier_code FROM mshd.carrier_code where id='" + str(carrier_id) + "';"
            self.cursor.execute(sql)
            result = self.cursor.fetchall()
            disaster_code += str(result[0][0])

            disaster_type_id = random.randint(1, 5)
            sql = "SELECT type_code FROM mshd.disaster_type_code where id='" + str(disaster_type_id) + "';"
            self.cursor.execute(sql)
            result = self.cursor.fetchall()
            disaster_code += str(result[0][0])

            sql = "SELECT id, subtype_code FROM mshd.disaster_subtype_code where type_id='" + str(
                disaster_type_id) + "' ORDER BY RAND() LIMIT 1;"
            self.cursor.execute(sql)
            result = self.cursor.fetchall()
            disaster_subtype_id = result[0][0]
            disaster_code += str(result[0][1])

            sql = "SELECT id, index_code FROM mshd.disaster_index_code where type_id='" + str(
                disaster_type_id) + "' ORDER BY RAND() LIMIT 1;"
            self.cursor.execute(sql)
            result = self.cursor.fetchall()
            disaster_index_id = result[0][0]
            disaster_code += str(result[0][1])

            sql = ("INSERT INTO mshd.disaster_code (id, region_code, time_code, source_code, carrier_code, "
                   "disaster_type_code, disaster_subtype_code, disaster_index_code, description, have_file) VALUES ('" + str(
                disaster_code) + "', '" + str(region_id) + "', '" + str(time_code) + "', '" + str(
                source_id) + "', '" + str(
                carrier_id) + "', '" + str(disaster_type_id) + "', '" + str(
                disaster_subtype_id) + "', '" + str(disaster_index_id) + "', '灾害描述', '0');")
            self.cursor.execute(sql)

            time += 1
            print(time)

    # 查询数据函数
    def get_data(self):
        sql = "SELECT * FROM mshd.region_code"
        # 执行sql语句
        self.cursor.execute(sql)
        # 获取所有的记录
        results = self.cursor.fetchall()
        return results

    def get_disaster_code_count(self):
        sql = "SELECT count(*) FROM mshd.disaster_code"
        self.cursor.execute(sql)
        result = self.cursor.fetchall()
        return result[0][0]

    def get_disaster_code_detail(self, disaster_code):
        sql = "SELECT * from mshd.disaster_code where id='" + str(disaster_code) + "';"
        self.cursor.execute(sql)
        results = self.cursor.fetchall()[0]

        sql = "SELECT province, city, county, town, village from mshd.region_code where id='" + str(results[1]) + "';"
        self.cursor.execute(sql)
        temp = self.cursor.fetchall()[0]
        province = temp[0]
        city = temp[1]
        county = temp[2]
        town = temp[3]
        village = temp[4]

        time = str(results[2])

        sql = "SELECT type, subtype from mshd.source_code where id='" + str(results[3]) + "';"
        self.cursor.execute(sql)
        temp = self.cursor.fetchall()[0]
        source_type = temp[0]
        source_subtype = temp[1]

        sql = "SELECT carrier from mshd.carrier_code where id='" + str(results[4]) + "';"
        self.cursor.execute(sql)
        temp = self.cursor.fetchall()[0]
        carrier = temp[0]

        sql = "SELECT type from mshd.disaster_type_code where id='" + str(results[5]) + "';"
        self.cursor.execute(sql)
        temp = self.cursor.fetchall()[0]
        disaster_type = temp[0]

        sql = "SELECT subtype from mshd.disaster_subtype_code where id='" + str(results[6]) + "';"
        self.cursor.execute(sql)
        temp = self.cursor.fetchall()[0]
        disaster_subtype = temp[0]

        sql = "SELECT index_name from mshd.disaster_index_code where id='" + str(results[7]) + "';"
        self.cursor.execute(sql)
        temp = self.cursor.fetchall()[0]
        disaster_index = temp[0]

        description = str(results[8])

        have_file = str(results[9])

        return {
            'disaster_code': disaster_code,
            'province': province,
            'city': city,
            'county': county,
            'town': town,
            'village': village,
            'time': time,
            'source_type': source_type,
            'source_subtype': source_subtype,
            'carrier': carrier,
            'disaster_type': disaster_type,
            'disaster_subtype': disaster_subtype,
            'disaster_index': disaster_index,
            'description': description,
            'have_file': have_file
        }

    def get_yearly(self, start_year, end_year):
        results = []
        for i in range(int(start_year), int(end_year)):
            sql = "SELECT count(*) from mshd.disaster_code where time_code>'" + str(
                i * 10000000000) + "' and time_code<'" + str((i + 1) * 10000000000) + "';"
            self.cursor.execute(sql)
            result = self.cursor.fetchall()[0][0]
            results.append({'year': i, 'value': result})
        return results

    def get_each_province(self):
        provinces = self.get_province()
        items = []
        for i in provinces:
            sql = "SELECT left(id, 2) from mshd.region_code where province='" + str(i[0]) + "'limit 1;"
            self.cursor.execute(sql)
            province_id = self.cursor.fetchall()[0][0]

            sql = "SELECT count(*) from mshd.disaster_code where left(region_code,2)='" + str(province_id) + "';"
            self.cursor.execute(sql)
            count = self.cursor.fetchall()[0][0]
            items.append({'province': i[0], 'value': count})
        return items

    def get_province(self):
        sql = "SELECT DISTINCT province from mshd.region_code"
        self.cursor.execute(sql)
        results = self.cursor.fetchall()
        return results

    def get_city(self, province):
        sql = "SELECT DISTINCT city from mshd.region_code where province='" + str(province) + "';"
        self.cursor.execute(sql)
        results = self.cursor.fetchall()
        return results

    def get_county(self, province, city):
        sql = "SELECT DISTINCT county from mshd.region_code where province='" + str(province) + "' and city='" + str(
            city) + "';"
        self.cursor.execute(sql)
        results = self.cursor.fetchall()
        return results

    def get_town(self, province, city, county):
        sql = "SELECT DISTINCT town from mshd.region_code where province='" + str(province) + "' and city='" + str(
            city) + "' and county='" + str(county) + "';"
        self.cursor.execute(sql)
        results = self.cursor.fetchall()
        return results

    def get_village(self, province, city, county, town):
        sql = "SELECT DISTINCT village from mshd.region_code where province='" + str(province) + "' and city='" + str(
            city) + "' and county='" + str(county) + "'and town='" + str(town) + "';"
        self.cursor.execute(sql)
        results = self.cursor.fetchall()
        return results

    def display(self, page_size, page_index):
        start = page_size * (page_index - 1)
        sql = "SELECT id from mshd.disaster_code limit " + str(start) + ", " + str(page_size) + ";"
        self.cursor.execute(sql)
        results = self.cursor.fetchall()
        items = []
        for i in results:
            items.append(self.get_disaster_code_detail(i[0]))

        return items

    def insert_region_code(self, id, province, city, county, town, village):
        sql = "INSERT INTO mshd.region_code(id, province, city, county, town, village) VALUES('" + str(
            id) + "', '" + str(province) + "', '" + str(city) + "', '" + str(county) + "', '" + str(
            town) + "', '" + str(village) + "');"
        self.cursor.execute(sql)
        # print(sql)

    def insert_disaster_code(self, province, city, county, town, village, date, time, source_type, source_subtype,
                             carrier, disaster_type, disaster_subtype, disaster_index, have_file, description=None):
        sql = "SELECT id from mshd.region_code where province='" + str(province) + "' and city='" + str(
            city) + "' and county='" + str(county) + "' and town='" + str(town) + "' and village='" + str(
            village) + "';"
        if self.cursor.execute(sql):
            results = self.cursor.fetchall()
            region_id = str(results[0][0])
            disaster_code = region_id
        else:
            return False

        time_code = str(date) + str(time)
        disaster_code += time_code

        sql = "SELECT id, type_code, subtype_code from mshd.source_code where type='" + str(
            source_type) + "' and subtype='" + str(source_subtype) + "';"
        if self.cursor.execute(sql):
            results = self.cursor.fetchall()
            source_id = str(results[0][0])
            disaster_code += str(results[0][1]) + str(results[0][2])
        else:
            return False

        sql = "SELECT id, carrier_code from mshd.carrier_code where carrier='" + str(carrier) + "';"
        if self.cursor.execute(sql):
            results = self.cursor.fetchall()
            carrier_id = str(results[0][0])
            disaster_code += str(results[0][1])
        else:
            return False

        sql = "SELECT id, type_code from mshd.disaster_type_code where type='" + str(disaster_type) + "';"
        if self.cursor.execute(sql):
            results = self.cursor.fetchall()
            disaster_type_id = str(results[0][0])
            disaster_code += str(results[0][1])
        else:
            return False

        sql = "SELECT id, subtype_code from mshd.disaster_subtype_code where subtype='" + str(
            disaster_subtype) + "' and type_id='" + str(disaster_type_id) + "';"
        if self.cursor.execute(sql):
            results = self.cursor.fetchall()
            disaster_subtype_id = str(results[0][0])
            disaster_code += str(results[0][1])
        else:
            return False

        sql = "SELECT id, index_code from mshd.disaster_index_code where index_name='" + str(
            disaster_index) + "' and type_id='" + str(disaster_type_id) + "';"
        if self.cursor.execute(sql):
            results = self.cursor.fetchall()
            disaster_index_id = str(results[0][0])
            disaster_code += str(results[0][1])
        else:
            return False

        sql = ("INSERT INTO mshd.disaster_code (id, region_code, time_code, source_code, carrier_code, "
               "disaster_type_code, disaster_subtype_code, disaster_index_code, description, have_file) VALUES ('" + str(
            disaster_code) + "', '" + str(region_id) + "', '" + str(time_code) + "', '" + str(source_id) + "', '" + str(
            carrier_id) + "', '" + str(disaster_type_id) + "', '" + str(
            disaster_subtype_id) + "', '" + str(disaster_index_id) + "', '" + str(description) + "', '" + str(
            have_file) + "');")
        if self.cursor.execute(sql):
            return disaster_code
        else:
            return False

    def delete_disaster_code(self, disaster_code):
        sql = "DELETE from mshd.disaster_code WHERE id = '" + str(disaster_code) + "';"
        if self.cursor.execute(sql):
            return True
        else:
            return False

    def verify_admin(self, uid, verified_password):
        if self.verify_password(uid, verified_password):
            sql = "SELECT is_admin from mshd.user where uid='" + str(uid) + "';"
            self.cursor.execute(sql)
            is_admin = self.cursor.fetchall()
            if is_admin[0][0] == 1:
                return True
        return False

    def verify_password(self, uid, verified_password):
        sql = "SELECT password from mshd.user where uid='" + str(uid) + "';"
        if self.cursor.execute(sql):
            password = self.cursor.fetchall()
            if password[0][0] == verified_password:
                return True
        return False

    def update_password(self, uid, password, new_password):
        if self.verify_password(uid, password):
            sql = "UPDATE mshd.user set password = '" + str(new_password) + "' WHERE `uid` = '" + str(uid) + "';"
            if self.cursor.execute(sql):
                return True
            else:
                return False
        else:
            return False

    def add_user(self, uid, password, new_user, new_password):
        if self.verify_password(uid, password):
            sql = "INSERT INTO mshd.user (uid, password, is_admin) VALUES ('" + str(new_user) + "', '" + str(
                new_password) + "', '0');"
            if self.cursor.execute(sql):
                return True
            else:
                return False
        else:
            return False

    def delete_user(self, uid, password, delete_user):
        if self.verify_password(uid, password):
            sql = "DELETE from mshd.user WHERE uid = '" + str(delete_user) + "';"
            if self.cursor.execute(sql):
                return True
            else:
                return False
        else:
            return False

    def export_disaster_code(self):
        count = self.cursor.execute("SELECT * from disaster_code")
        self.cursor.scroll(0, mode='absolute')
        results = self.cursor.fetchall()
        fields = self.cursor.description
        new = xlwt.Workbook(encoding='utf-8')
        sheet = new.add_sheet('灾情码', cell_overwrite_ok=True)
        for field in range(0, len(fields)):
            sheet.write(0, field, fields[field][0])

        row = 1
        col = 0
        for row in range(1, len(results) + 1):
            for col in range(0, len(fields)):
                sheet.write(row, col, u'%s' % results[row - 1][col])

        sio = BytesIO()
        new.save(sio)
        sio.seek(0)
        return sio.getvalue()

    # 关闭
    def __del__(self):
        self.db.close()
