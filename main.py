from urllib.parse import urlparse
import sys
import mysql.connector

url = urlparse('mysql://root@localhost:3306/stationapi')

conn = mysql.connector.connect(
    host=url.hostname or 'localhost',
    port=url.port or 3306,
    user=url.username or 'root',
    password=url.password or '',
    database=url.path[1:],
)

if conn.is_connected() is False:
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
select_query = "SELECT `station_cd`, `pref_cd`, `add` FROM stations WHERE `add`"

for i, pref in enumerate(all_pref_list):
    # 北海道
    if i is 0:
        select_query += " NOT LIKE '{}%'".format(pref)
        continue
    select_query += " AND `add` NOT LIKE '{}%'".format(pref)

cursor = conn.cursor()
try:
    cursor.execute(select_query)
    all_station = cursor.fetchall()
    for station in all_station:
        print(station)
        pref = all_pref_list[int(station[1] - 1)]
        new_addr = "{}{}".format(pref, station[2])
        update_query = "UPDATE `stations` SET `add`='{}' WHERE  `station_cd`={}".format(new_addr, station[0])
        print(update_query)
        cursor.execute(update_query)
        conn.commit()
except Exception as e:
    conn.rollback()
    raise e
finally:
    cursor.close()
    conn.close()
