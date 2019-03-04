import os
import unittest

from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

from SQLTypes import Base, Item, download_item


class DatasetTest(unittest.TestCase):
    def test_item_saving(self):
        database_path = "sqlalchemy_example.db"
        print("Wiping database at %s" % (database_path,))
        os.remove(database_path)
        # Create an engine that stores data in the local directory's
        # sqlalchemy_example.db file.
        engine = create_engine('sqlite:///%s' % (database_path,))

        # Create all tables in the engine. This is equivalent to "Create Table"
        # statements in raw SQL.
        Base.metadata.create_all(engine)

        DBSession = sessionmaker(bind=engine)
        session = DBSession()

        url = "https://homepages.cae.wisc.edu/~ece533/images/cat.png"
        path = "./data"
        item = Item(url=url)

        # if the item does not exist, add it
        try:
            session.add(item)
            session.commit()
        except IntegrityError as e:
            print(e)

        item = session.query(Item).filter_by(url=url).first()
        print(item)
        download_item(item, path, session)
        print(item)


        print(session.query(Item).all())



"""
engine = create_engine('sqlite:///mnt/nas/datasets/my_db/data/database.db')

"""




if __name__ == '__main__':
    unittest.main()

