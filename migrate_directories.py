import os
import shutil
import sys

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from tqdm import tqdm

from SQLTypes import Base, Item, hash_path, hash_file, query_iterator


def main():

    new_base_dir = "/mnt/nas/datasets/my_db/data/"
    old_base_dir = "/mnt/data/datasets/dataset_database/data/"

    database_path = "/mnt/nas/datasets/my_db/data/database.db"
    engine = create_engine('sqlite:///%s' % (database_path,))
    Base.metadata.create_all(engine)
    DBSession = sessionmaker(bind=engine)
    session = DBSession()
    # print(session.query(Item).filter(Item.path.like(old_base_dir + "%")).count())

    # ok, I just moved the base dir. So I want to update that in everything.

    """
    Works:
    session.query(Item).filter(Item.path.contains("/mnt/")).first()
    
    it was under the data directory, not nas
    
    I need to move towards relative paths
    
    kk"""
    query = session.query(Item).filter(Item.path.contains(str(old_base_dir)))
    total = query.count()
    for item in tqdm(query_iterator(query), total=total):
        # update paths to new relative path

        # print(item)
        # get the new-ish path - The files were all copied here
        new_ish_path = new_base_dir + item.path[len(old_base_dir):]
        filename = os.path.basename(new_ish_path)
        extension = os.path.splitext(filename)[1]
        # see if it exists
        # print(os.path.exists(new_ish_path))
        # find new relative path then copy it there
        path, new_name = hash_path(item.hash, 2)

        new_filename = new_name + extension

        new_path = os.path.join(".", path, new_filename)
        new_dir = os.path.join(new_base_dir, path)
        new_location = os.path.join(new_base_dir, new_path)
        # print(new_ish_path)
        # print(new_path, new_location)
        # os.makedirs(new_dir, exist_ok=True)
        # os.rename(file_path, new_path)
        # 4 is the deepest that i will need to go for even 100M images,
        # even with 256 per dir with the 2 char dirs

        os.makedirs(new_dir, exist_ok=True)
        # copy it to the new location
        shutil.copy2(new_ish_path, new_location)
        # update the path with the new relative one
        item.path = new_path
        # session.add(item)
        # session.commit()
        # print(item)
        # print(os.path.exists(os.path.join(new_base_dir, item.path)))

        # make sure both files have the same hash
        new_hash = hash_file(new_location).hexdigest()
        old_hash = hash_file(new_ish_path).hexdigest()
        same_hash = new_hash == old_hash

        if not same_hash:
            print("Bad hash:", old_hash, new_hash, item.hash)
            sys.exit()

        session.add(item)
        session.commit()

        os.remove(new_ish_path)















if __name__ == "__main__":
    main()





