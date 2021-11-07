# coding:utf-8
import sys
from urllib.parse import urlparse

import mysql.connector

# 都道府県置き換え
url = urlparse('mysql://root@localhost:3306/stationapi')

conn = mysql.connector.connect(
    host=url.hostname or 'localhost',
    port=url.port or 3306,
    user=url.username or 'root',
    password=url.password or 'password',
    database=url.path[1:],
    auth_plugin='mysql_native_password'
)

if conn.is_connected() == False:
    print("Database connection failed! Please check the credential!")
    sys.exit()

all_pref_list = [
    "北海道",
    "青森県",
    "岩手県",
    "宮城県",
    "秋田県",
    "山形県",
    "福島県",
    "茨城県",
    "栃木県",
    "群馬県",
    "埼玉県",
    "千葉県",
    "東京都",
    "神奈川県",
    "新潟県",
    "富山県",
    "石川県",
    "福井県",
    "山梨県",
    "長野県",
    "岐阜県",
    "静岡県",
    "愛知県",
    "三重県",
    "滋賀県",
    "京都府",
    "大阪府",
    "兵庫県",
    "奈良県",
    "和歌山県",
    "鳥取県",
    "島根県",
    "岡山県",
    "広島県",
    "山口県",
    "徳島県",
    "香川県",
    "愛媛県",
    "高知県",
    "福岡県",
    "佐賀県",
    "長崎県",
    "熊本県",
    "大分県",
    "宮崎県",
    "鹿児島県",
    "沖縄県",
]
stations_select_query = "SELECT `station_cd`, `pref_cd`, `address` FROM stations WHERE `address`"
lines_select_query = "SELECT `line_cd`, `line_color_c` FROM `lines`"
foreign_name_select_query = "SELECT `station_name`, `station_name_zh`, `station_name_ko` FROM stations"

for i, pref in enumerate(all_pref_list):
    # 北海道
    if i == 0:
        stations_select_query += " NOT LIKE '{}%'".format(pref)
        continue
    stations_select_query += " AND `address` NOT LIKE '{}%'".format(pref)

cursor = conn.cursor()
try:
    # すべての駅に都道府県をつける
    cursor.execute(stations_select_query)
    all_station = cursor.fetchall()
    for station in all_station:
        pref = all_pref_list[int(station[1] - 1)]
        new_addr = "{}{}".format(pref, station[2])
        update_query = "UPDATE `stations` SET `address`='{}' WHERE  `station_cd`={}".format(
            new_addr, station[0])
        cursor.execute(update_query)
        conn.commit()
    # すべての路線の桁を合わせる
    cursor.execute(lines_select_query)
    all_lines = cursor.fetchall()
    for stored_line in all_lines:
        line_id = stored_line[0]
        line_color = stored_line[1]
        if line_color != None and len(line_color) != 6:
            padded = line_color.rjust(6, '0')
            update_query = "UPDATE `lines` SET `line_color_c`='{}' WHERE  `line_cd`={}".format(
                padded, line_id)
            cursor.execute(update_query)
            conn.commit()
    # ローカライズされてない駅を検出
    cursor.execute(foreign_name_select_query)
    all_foreign_name = cursor.fetchall()
    print("THESE STATATIONS ARE NOT LOCALIZED!")
    for station in all_foreign_name:
        name = station[0]
        name_zh = station[1]
        name_ko = station[2]
        zh_not_localized = name_zh == ""
        ko_not_localized = name_ko == ""
        if zh_not_localized == True or ko_not_localized == True:
            not_localized_languages = []
            if zh_not_localized == True:
                not_localized_languages.append("ZH")
            if ko_not_localized == True:
                not_localized_languages.append("KO")
            print("[%s]: %s" % ('|'.join(not_localized_languages), name))
except Exception as e:
    conn.rollback()
    raise e
finally:
    cursor.close()

# 後片付け
conn.close()
