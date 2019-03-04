import glob
import os

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from tqdm import tqdm

from SQLTypes import Base, hash_file, Item


def main():
    new_base_dir = "/mnt/nas/datasets/my_db/data/"
    old_base_dir = "/mnt/data/datasets/dataset_database/data/"

    database_path = "/mnt/nas/datasets/my_db/data/database.db"
    engine = create_engine('sqlite:///%s' % (database_path,))
    Base.metadata.create_all(engine)
    DBSession = sessionmaker(bind=engine)
    session = DBSession()
    types = [".jpg", ".png", ".gif"]

    for type in types:
        files = glob.glob(new_base_dir + "*" + type)

        tq = tqdm(files)
        for file in tq:
            # hash it, see if it's in the database. If so, delete it
            hash = hash_file(file).hexdigest()
            val = session.query(Item).filter_by(hash=hash).first()
            if val:
                tq.set_postfix(file=file, refresh=False)
                os.remove(file)




if __name__ == "__main__":
    main()

