import json
from time import sleep

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from SQLTypes import Base, Item, query_tq, q_all_tq

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
    pg_session = DBSession()

    item_count = pg_session.query(Item).count()
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
    pg_session.merge(first_sql)
    pg_session.commit()
    first_sql = pg_session.query(Item).first()
    print(first_sql)

    query = sqlite_session.query(Item)
    try:
        for item in q_all_tq(query):
            sqlite_session.expunge(item)
            # if pg_session.query(Item).filter_by(id=item.id).first():
            pg_session.merge(item)
            # else:
            #     pg_session.add(item)
            pg_session.commit()
    finally:
        pg_session.commit()



    item_count = pg_session.query(Item).count()
    print("final Item Count: %s" % (item_count,))







if __name__ == "__main__":
    main()

