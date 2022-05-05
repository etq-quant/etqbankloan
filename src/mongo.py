import os
from pymongo.errors import BulkWriteError
import pymongo

# MONGOS = ["192.168.0.115:27018"]
# URI = ",".join(MONGOS)
URI = "localhost:27017"
# URI = "localhost:27019"


def create_mongo_client(
    uri=None,
    maxIdleTimeMS=10 * 1000,
    socketTimeoutMS=50,
    connectTimeoutMS=50,
    serverSelectionTimeoutMS=50,
    readPreference="primaryPreferred",  # primaryPreferred
):
    """Create MongoDB client
    Returns:
        [client]: MongoDB client
    """
    if uri is None:
        uri = URI
    client = pymongo.MongoClient(
        uri,
        maxIdleTimeMS=maxIdleTimeMS,
        socketTimeoutMS=socketTimeoutMS,
        connectTimeoutMS=connectTimeoutMS,
        readPreference=readPreference,
        serverSelectionTimeoutMS=serverSelectionTimeoutMS,
    )
    return client


def create_mongo_indexes(coll):
    if len([j for j in coll.list_indexes()]) < 2:
        coll.create_index([("Date", pymongo.ASCENDING)])  # , background=True

        coll.create_index([("Code", pymongo.ASCENDING)])  # , background=True

        coll.create_index(
            [("Date", pymongo.ASCENDING), ("Code", pymongo.ASCENDING)]
            # unique=True,  # , background=True
        )


def create_mongo_indexes_general(uri, db, coll, singleindex_fields, multiindex_fields):
    """Create indexes in mongodb collection (if no index exist)

    Args:
        db ([str]): DB
        coll ([str]): COLLECTION
        singleindex_fields ([list]):
            Example:
            ["Date", "Code"] will create a "Date" index and a "Code" index
        multiindex_fields ([list of tuples]):
            Example:
            [("Date", "Code"), ("Date", "Sector")] will create a
            ("Date", "Code") compound index and a ("Date", "Sector")
            compound index

    """
    if uri is None:
        uri = URI
    with create_mongo_client(uri) as client:
        coll = client[db][coll]
        if len([j for j in coll.list_indexes()]) < 2:
            for field in singleindex_fields:
                # , background=True
                coll.create_index([(field, pymongo.ASCENDING)])

            for field_tuple in multiindex_fields:
                compound_index = [(j, pymongo.ASCENDING) for j in field_tuple]
                coll.create_index(
                    compound_index, unique=True,  # , background=True
                )


def mongo_insert(db, coll, docs):
    """Insert documents into collection

    Args:
        db ([str]): DB
        coll ([str]): COLLECTION
        docs ([list of dict]): list of dictionary/dictionary
    """
    if not isinstance(docs, list):
        docs = [docs]
    if len(docs) > 0:
        with create_mongo_client() as client:
            client[db][coll].insert_many(docs)
    else:
        print("Nothing to insert.")


def insert_bloomberg_data(to_insert, coll_name, db_name="HK", uri=None, quiet=True):
    if len(to_insert) == 0:
        return

    if uri is None:
        uri = URI
    create_mongo_indexes_general(
        uri=uri,
        db=db_name,
        coll=coll_name,
        singleindex_fields=["DATE", "ID"],
        multiindex_fields=[("DATE", "ID")],
    )
    with create_mongo_client(uri) as client:
        coll = client[db_name][coll_name]
        try:
            status = coll.insert_many(to_insert)
        except BulkWriteError:
            return

        if not quiet:
            print("@insert_bloomberg_data status:", status)


def insert_bloomberg_data_dateonly(to_insert, coll_name, db_name="HK", uri=None):
    if uri is None:
        uri = URI
    create_mongo_indexes_general(
        uri=uri,
        db=db_name,
        coll=coll_name,
        singleindex_fields=["DATE"],
        multiindex_fields=[],
    )
    with create_mongo_client(uri) as client:
        coll = client[db_name][coll_name]
        status = coll.insert_many(to_insert)
        print("@insert_bloomberg_data status:", status)


def insert_bloomberg_data_idonly(to_insert, coll_name, db_name="HK", uri=None):
    if uri is None:
        uri = URI
    create_mongo_indexes_general(
        uri=uri,
        db=db_name,
        coll=coll_name,
        singleindex_fields=["ID"],
        multiindex_fields=[],
    )
    with create_mongo_client(uri) as client:
        coll = client[db_name][coll_name]
        status = coll.insert_many(to_insert)
        print("@insert_bloomberg_data status:", status)
