import json
import redis
from pymongo import MongoClient
from settings import *
from bson.objectid import ObjectId
from datetime import datetime


def update_batch_path():
    try:
        print("update_industry_batch_path: start processing at:{}".format(str(datetime.now())))
        update_flag = redis_client.get('update_industry_flag')
        if update_flag == 'true':
            all_industries = industry_coll.find({"valid": True})
            for industry in all_industries:
                industry_id = str(industry['_id'])
                is_parent_industry = False
                if "parent_id" not in industry or not industry['parent_id']:
                    industry['parent_id'] = ""
                    is_parent_industry = True
                entity = dict()
                entity['title'] = industry['title']
                entity['slug'] = industry['slug']

                entity['parent_id'] = industry['parent_id']
                entity['valid'] = True
                # set path in redis
                if is_parent_industry:
                    value = {'path': '', 'slug_path': ''}
                    redis_client.hset(INDUSTRY_PATH_HASH, industry_id, json.dumps(value))
                else:
                    get_industry_path(industry_id, entity['parent_id'], entity['title'], entity['slug'])
            redis_client.set('update_industry_flag', 'false')
            print("update_industry_batch_path: completed at:{}".format(str(datetime.now())))
        else:
            print("Industry:update_industry_batch_path::{}".format("All Industries data are up-to-date"))
    except Exception as e:
        print("Industry:update_industry_batch_path:error:{}".format(str(e)))


def get_industry_path(industry_id, parent_id, path, slug_path):
    try:
        parent_data = redis_client.hget(INDUSTRY_PATH_HASH, parent_id)
        if parent_data:
            parent_data = json.loads(parent_data)
            if parent_data['path']:
                value = {'path': parent_data['path'] + '/' + path,
                         'slug_path': parent_data['slug_path'] + '/' + slug_path}
                redis_client.hset(INDUSTRY_PATH_HASH, str(industry_id), json.dumps(value))
            else:
                parent_industry = industry_coll.find_one({'_id': ObjectId(parent_id)})
                if parent_industry:
                    value = {'path': parent_industry['title'] + '/' + path,
                             'slug_path': parent_industry['slug'] + '/' + slug_path}
                    redis_client.hset(INDUSTRY_PATH_HASH, str(industry_id), json.dumps(value))
                else:
                    print("Industry:get_industry_path:error:parent_industry:{} is not found".format(parent_id))
        else:
            value = get_industry_path_recursively(parent_id, {'path': path, 'slug_path': slug_path})
            if value:
                redis_client.hset(INDUSTRY_PATH_HASH, str(industry_id), json.dumps(value))
    except Exception as e:
        print("Industry:get_industry_path:error:{}".format(str(e)))


def get_industry_path_recursively(parent_id, industry_path):
    try:
        industry = industry_coll.find_one({'_id': ObjectId(parent_id)})
        if industry:
            industry_path['path'] = industry['title'] + '/' + industry_path['path']
            industry_path['slug_path'] = industry['slug'] + '/' + industry_path['slug_path']
            return get_industry_path(str(industry['_id']), industry['parent_id'], industry_path['path'],
                                     industry_path['slug_path'])
        else:
            industry_path['path'] = industry_path['title'] + '/' + industry_path['path']
            industry_path['slug_path'] = industry_path['slug'] + '/' + industry_path['slug_path']
        return industry_path
    except Exception as e:
        print("Industry:get_industry_path_recursively:error:{}".format(str(e)))


if __name__ == "__main__":
    mongo_client = MongoClient(host=mongo_server, port=int(mongo_port), username=mongo_user,
                               password=mongo_password, authSource=mongo_authsource)
    mongo_db = mongo_client[mongo_database]
    industry_coll = mongo_db.industry_coll

    redis_client = redis.Redis(host='localhost', port=6379, db=2, decode_responses=True)

    update_batch_path()

