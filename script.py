import pandas

from Mysql import Mysql

excel = pandas.read_excel('D:\\QQ\\2878740067\\FileRecv\\region_code.xls',
                          sheet_name=['region_code(2)', 'region_code(3)', 'region_code(4)', 'region_code(5)',
                                      'region_code(6)', 'region_code(7)', 'region_code(8)', 'region_code(9)'],
                          names=['id', 'province', 'city', 'county', 'town', 'village'], header=None)

db = Mysql()
print("start insert")

for i in range(2, 10):
    for row in excel['region_code(' + str(i) + ')'].itertuples():
        db.insert_region_code(getattr(row, 'id'), getattr(row, 'province'), getattr(row, 'city'),
                              getattr(row, 'county'), getattr(row, 'town'), getattr(row, 'village'))

print("!")
