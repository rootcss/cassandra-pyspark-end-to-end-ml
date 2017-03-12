from faker import Faker
import random
import time_uuid
import json
from datetime import datetime
fake = Faker()

def edges():
    data = {}
    data['comment_mobile_orig']     = random.randint(7000000000, 9999999999)
    data['updated_at']              = str(fake.date_time_between(start_date="-1y", end_date="now", tzinfo=None))
    data['comment_duration']        = random.randint(20,60)
    data['comment_type']            = random.choice(['Spam', 'Type2', 'Type3', 'Type4'])
    data['contact_name']            = fake.name()
    data['src']                     = str(random.randint(1,500)) + '_' + str(random.randint(1,500))
    data['dst']                     = str(random.randint(1,500)) + '_' + str(random.randint(1,500))
    data['tagged_contact_name']     = fake.name()
    return json.dumps(data)

def vertices():
    data = {}
    data['id']              = str(random.randint(1,500)) + '_' + str(random.randint(1,500))
    data['mobilenumber']    = random.randint(7000000000, 9999999999)
    return json.dumps(data)