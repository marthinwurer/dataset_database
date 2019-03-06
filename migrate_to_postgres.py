import json

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from SQLTypes import Base, Item, query_tq

SQLALCHEMY_DATABASE_URI = "postgresql://yourusername:yourpassword@localhost/yournewdb"




def main():
    data_dir = "/mnt/nas/datasets/my_db/data/"
    with open("creds.json") as cred_file:
        creds = json.load(cred_file)

    user = creds['user']
    password = creds['pass']
    hostname = creds['host']
    db_name = creds['db']

    # engine = create_engine('sqlite:///%s' % (database_path,))
    engine = create_engine('postgresql://%s:%s@%s/%s' % (user, password, hostname, db_name))
    # engine = create_engine('postgresql://%s@localhost/%s' % (creds['user'], creds['db']))
    # Base.metadata.create_all(engine)
    DBSession = sessionmaker(bind=engine)
    session = DBSession()

    item_count = session.query(Item).count()
    print("Item Count: %s" % (item_count,))

    database_path = "./database.db"
    sqlite_engine = create_engine('sqlite:///%s' % (database_path,))
    sqlite_builder = sessionmaker(bind=sqlite_engine)
    sqlite_session = sqlite_builder()

    item_count = sqlite_session.query(Item).count()
    
    print("Item Count: %s" % (item_count,))

    first_sql = sqlite_session.query(Item).first()
    sqlite_session.expunge(first_sql)
    print(first_sql)
    session.merge(first_sql)
    session.commit()
    first_sql = session.query(Item).first()
    print(first_sql)

    query = sqlite_session.query(Item)
    for item in query_tq(query):
        sqlite_session.expunge(item)
        session.merge(item)
        session.commit()



    item_count = session.query(Item).count()
    print("final Item Count: %s" % (item_count,))







if __name__ == "__main__":
    main()

