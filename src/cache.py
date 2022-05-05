import hashlib
import os

import pickle
import joblib

try:
    from src.mongo import create_mongo_client, create_mongo_indexes_general
except:
    print("no pymongo!")


def load_local_cache(hash_str):
    filename = hash_str
    os.makedirs("cache", exist_ok=True)
    cachefile = f"cache/{filename}.pkl"
    if os.path.exists(cachefile):
        out = joblib.load(cachefile)
        return out
    else:
        return None


def write_local_cache(hash_str, data):
    filename = hash_str
    os.makedirs("cache", exist_ok=True)
    cachefile = f"cache/{filename}.pkl"
    joblib.dump(data, cachefile)


def write_mongo_cache(hash_str, data):
    out = pickle.dumps(data)
    db_name = "bq_cache"
    coll_name = "cache"
    create_mongo_indexes_general(
        uri=None,
        db=db_name,
        coll=coll_name,
        singleindex_fields=["HASH"],
        multiindex_fields=[],
    )
    with create_mongo_client() as client:
        coll = client[db_name][coll_name]
        packet = {"HASH": hash_str, "data": out}
        x = coll.insert_one(packet)
        return x


def load_mongo_cache(hash_str):
    try:
        with create_mongo_client() as client:
            coll = client["bq_cache"]["cache"]
            x = coll.find_one({"HASH": hash_str})
            if x is None:
                return None
            else:
                out = pickle.loads(x["data"])
                return out
    except Exception as err:
        print("Error@load_mongo_cache:", err)
        return None


def load(hash_str):
    try:
        return load_mongo_cache(hash_str)
    except Exception:
        return load_local_cache(hash_str)


def write(hash_str, data):
    try:
        write_mongo_cache(hash_str, data)
    except Exception:
        write_local_cache(hash_str, data)


class memorize(dict):
    def __init__(self, func):
        self.cache = {}
        self.func = func

    def __call__(self, *arg1, **args):
        a1, a2 = str(arg1), str(args)
        b1 = self.func.__name__
        a3 = a1 + a2
        hash_str = b1 + hashlib.sha224(str(a3).encode()).hexdigest()

        out = load(hash_str=hash_str)
        if out is None:
            out = self.func(*arg1, **args)
            write(hash_str=hash_str, data=out)
        return out
