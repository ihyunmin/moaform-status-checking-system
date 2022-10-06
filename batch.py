# Send API to moaform and update the data every ten minutes.
import schedule
import time
import requests
from dotenv import load_dotenv
import os
import app

from datetime import datetime
from pytz import timezone

from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import Column, TEXT, INTEGER, func

load_dotenv()
MOAFORM_URL = os.environ.get("MOAFORM_URL")
MOAFORM_COOKIE = os.environ.get("MOAFORM_COOKIE")


def send_api(page):
    url = MOAFORM_URL + str(page)
    headers = {
        'Content-Type': 'application/json',
        'charset': 'UTF-8',
        'Accept': '*/*',
        'Cookie': MOAFORM_COOKIE
    }
    try:
        response = requests.get(url, headers=headers)
        if response.status_code ==200:
            # print("response text %r" % response.text)
            return response.json()
        else:
            # print('Check the moaform API')
            return [{}]
    except Exception as ex:
        print(ex)

    

def batch():
    data_list = []
    for i in range(0,15):
        data = send_api(i)
        if(len(data) != 0):
            data_list.append(data)
        else:
            break
    
    # id / title / response_count
    for data in data_list:
        for row in data:
            form_id, name, res_cnt = row['id'], row['title'], row['response_count']
            answers = app.Answer.query.filter(app.Answer.form_id == form_id, app.Answer.name == name).all()

            try:
                if(len(answers) == 1):
                    app.db.session.query(app.Answer). \
                        filter(app.Answer.form_id == form_id, app.Answer.name == name). \
                        update({'answers':res_cnt})
                    app.db.session.commit()
                elif(len(answers) == 0):
                    new_data = app.Answer(form_id=form_id, name=name, answers=res_cnt)
                    app.db.session.add(new_data)
                    app.db.session.commit()
                else:
                    print('Insert Error', res)
            except:
                print('***ERROR IN SQLALCHEMY')

    now = datetime.now(timezone('Asia/Seoul'))
    print("{}/{} {}:{} Query Finish Log".format(now.month,now.day, now.hour, now.minute))

def main():
    schedule.every(10).minutes.do(batch)

    while True:
        schedule.run_pending()
        time.sleep(600)

if __name__ == "__main__":
    main()
