from faker import Faker
import random
import time_uuid
import json
from datetime import datetime
fake = Faker()

def payload():
    data = {}
    data['name']        = fake.name()
    data['city']        = fake.city()
    data['zipcode']     = fake.zipcode()
    data['created_at']  = str(fake.date_time_between(start_date="-1y", end_date="now", tzinfo=None))
    data['email']       = fake.email()
    data['job']         = fake.job()
    data['gender']      = random.choice(['M', 'F'])
    data['age']         = random.randint(20,60)
    return json.dumps(data)

def event():
    event = {}
    event['event_id']   = str(time_uuid.TimeUUID.with_timestamp(time_uuid.utctime()))
    event['event_name'] = 'EventName'+str(random.randint(1000,9999))
    event['bucket_id']  = str(fake.date(pattern="%Y-%m-%d"))
    event['payload']    = payload()
    return json.dumps(event)