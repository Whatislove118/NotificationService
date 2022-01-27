import os
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
import json
import redis as broker
from sqlalchemy import text
from apscheduler.schedulers.background import BackgroundScheduler
import atexit
import boto3

basedir = os.path.abspath(os.path.dirname(__file__))

app = Flask(__name__)
app.config['DATABASE_HOST'] = os.getenv('DATABASE_HOST')
app.config['DATABASE_NAME'] = os.getenv('DATABASE_NAME')
app.config['DATABASE_USER'] = os.getenv('DATABASE_USER')
app.config['DATABASE_PASSWORD'] = os.getenv('DATABASE_PASSWORD')

app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql+pymysql://%s:%s@%s/%s' % (
    app.config['DATABASE_USER'],
    app.config['DATABASE_PASSWORD'],
    app.config['DATABASE_HOST'],
    app.config['DATABASE_NAME']   
)

app.config['SQLACLHEMY_COMMIT_ON_TEARDOWN'] = True
app.config['COUNT_LIKES'] = 3

db = SQLAlchemy(app)
# SES

app.config['AWS_ACCESS_KEY_ID'] = os.getenv('AWS_ACCESS_KEY_ID')
app.config['AWS_SECRET_ACCESS_KEY'] = os.getenv('AWS_SECRET_ACCESS_KEY')
app.config['SES_REGION_NAME'] = os.getenv('SES_REGION_NAME')
app.config['SES_EMAIL_SOURCE'] = os.getenv('SES_EMAIL_SOURCE')
app.config['REDIS_HOST'] = os.getenv('REDIS_HOST')

redis = broker.StrictRedis(host=app.config['REDIS_HOST'], port=6379, charset='utf-8', decode_responses=True)
pubsub = redis.pubsub()

def like_picture(id):
    print('like ', id)
    result = db.engine.execute(text('UPDATE picture SET likes = likes + 1 WHERE id = :id'), {'id': id})

def message_callback(message):
    print(message)
    id = json.loads(message['data'])['id']
    like_picture(id)
    print('REDIS: ', message['data'])
    
pubsub.subscribe(**{'likes_channel': message_callback})

def get_user_email(id):
    result = db.engine.execute(text('SELECT email FROM user where id = :id'),
                               {"id": id})
    for row in result:
        return row[0]


def send_mail(id, name):
    email = get_user_email(id)
    ses = boto3.client(
        'ses',
        region_name=app.config['SES_REGION_NAME'],
        aws_access_key_id=app.config['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=app.config['AWS_SECRET_ACCESS_KEY']
    )
    sender = app.config['SES_EMAIL_SOURCE']

    ses.send_email(
        Source=sender,
        Destination={'ToAddresses': [email]},
        Message={
            'Subject': {'Data': 'picture %s achievements' % name},
            'Body': {
                'Text': {'Data': 'Your picture %s got %d likes. Congratulations!' % (name, app.config['COUNT_LIKES'])},
            }
        }
    )
    
    
def update_notify(id):
    db.engine.execute(text('UPDATE picture SET is_notify = true WHERE id = :id'), {'id': id})

    
def check_picture_likes():
    result = db.engine.execute(text('SELECT user_id, name, id FROM picture where likes >= :likes and is_notify = false'),
                               {"likes": app.config['COUNT_LIKES']})
    for row in result:
        send_mail(row['user_id'], row['name'])
        update_notify(row['id'])
        
        

@app.route('/')
def send():
    send_mail(1, 'kotleta')
    return 'asdada'
    


sched = BackgroundScheduler(daemon=True)
sched.add_job(check_picture_likes,'interval',minutes=1)
sched.start()



# mysql://username:password@hostname/database


import signal

def handle_exit(*args):
    print('Stopping subscribing')
    thread.stop()
    pubsub.unsubscribe('likes_channel')
    exit()

atexit.register(handle_exit)
signal.signal(signal.SIGTERM, handle_exit)
signal.signal(signal.SIGINT, handle_exit)

def get_picture(id):
    result = db.engine.execute(text('SELECT * FROM picture WHERE id = :id'), {'id': id})
    for row in result:
        print(row.likes)
       

def exception_handler(e, pub, self):
    print(e)
    

thread = pubsub.run_in_thread(sleep_time=0.001, exception_handler=exception_handler)

if __name__ == '__main__':
   
    print(thread)
    app.run(debug=True)
    # thread.stop()
    # manager.run()