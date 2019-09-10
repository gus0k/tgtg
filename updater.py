#!/usr/bin/python
import socks
import requests
import json
from notify_run import Notify
import datetime as dt
from datetime import datetime
from dateutil import tz
from tinydb import TinyDB, Query
from tinydb.storages import JSONStorage
from constants import TGTG_URL, USER_AGENT, DATABASE, PLACES

from tinydb_serialization import Serializer

class DateTimeSerializer(Serializer):
    OBJ_CLASS = datetime  # The class this serializer handles

    def encode(self, obj):
        return obj.strftime('%Y-%m-%dT%H:%M:%S')

    def decode(self, s):
        return datetime.strptime(s, '%Y-%m-%dT%H:%M:%S')

from tinydb import TinyDB
from tinydb_serialization import SerializationMiddleware
from tinydb import Query

serialization = SerializationMiddleware()
serialization.register_serializer(DateTimeSerializer(), 'TinyDate')

db = TinyDB(DATABASE, storage=serialization)

from_zone = tz.tzutc()
to_zone   = tz.tzlocal()

db = TinyDB(DATABASE, storage=serialization)

def add_new_entry(db):
    data = new_request()
    summary = process_data(data)
    changes = []
    for (i, x) in summary:
        new = False
        table = db.table(i)
        L = len(table)
        if L > 0:
            last = table.get(doc_id=L)
            if last['quantity'] != x['quantity']:
                new = True
        else:
            new = True
        if new:
            table.insert(x)
            changes.append((i, x))
    return changes

    

def new_request():

    session = requests.session()
    session.proxies = {}
    headers = {}
    headers['User-agent'] = USER_AGENT
    r = session.get(TGTG_URL, headers=headers)
    try:
        assert r.status_code == 200
        data = json.loads(r.text)
        assert data['status_code'] == 1
        data = data['info']
    except Exception as e:
        print(r.status_code)
        print(r.text)
        data = []
    
    return data


def process_data(data):
    summary = []
    for row in data:
        id_ = int(row['id'])
        if id_ in PLACES:
            name = row['business_name']
            quantity = int(row['todays_stock'])
            pickup = row['current_window_pickup_start_utc']

            utc = datetime.strptime(pickup, '%Y-%m-%d %H:%M:%S')
            utc = utc.replace(tzinfo=from_zone)
            pickup = utc.astimezone(to_zone)
            
            if pickup.date() == datetime.today().date():
                when = 'Today'
            elif pickup.date() == datetime.today().date() + dt.timedelta(days=1):
                when = 'Tomorrow'
            else:
                when =  datetime.combine(pickup.date(), datetime.min.time())
            s = {
                'id': id_,
                'when': when,
                'pickup': pickup,
                'quantity': quantity,
                'timestamp': datetime.now()
            }
            summary.append((name,s))
    
    
    return summary


if __name__ == '__main__':
    from nonoLINE import nonoLINE
    from constants import TOKEN_PATH


    with open(TOKEN_PATH, 'r') as fh: TOKEN = fh.read().strip()
    nono_line = nonoLINE(TOKEN, max_workers=4, default_tag='')

    changes = add_new_entry(db)

    for i, x in changes:
        s = f"{i}, {x['quantity']}, {x['when']}"
        nono_line.send(s)


