import argparse
import glob
import os
import sys
import tempfile

from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker
from tqdm import tqdm

from SQLTypes import Item, Base, download_urls

"""
$ python database_builder.py /mnt/nas/datasets/database.db /mnt/nas/datasets/visualgenome/VG_100K/
python database_builder.py --wipe /mnt/nas/datasets/database.db /mnt/nas/datasets/test_data/
"""


def store_image_paths(base_dir, session):
    types = [".jpg", ".png", ".gif"]
    for root, dirs, files in tqdm(os.walk(base_dir)):
        # print(files)
        basenames = [os.path.splitext(file) for file in files]
        # print(basenames)
        image_files = filter(lambda x: os.path.splitext(x)[1] in types, files)
        for file in tqdm(image_files):
            full_path = os.path.join(root, file)
            try:
                item = Item(path=full_path)
                session.add(item)
                session.commit()
            except IntegrityError as e:
                session.rollback()
                # print("image already exists: %s" % (full_path,))


def store_item_urls(base_dir, session):
    for root, dirs, files in tqdm(os.walk(base_dir)):
        # print(root, dirs, files)
        images_dir = os.path.join(root, "IMAGES")
        # find images files
        for file in glob.iglob(root + "/urls.txt"):
            print("found url file: %s" % (file,))

            with open(file) as f:
                for line in tqdm(f):
                    line = line.strip()
                    try:
                        item = Item(url=line)
                        session.add(item)
                        session.commit()
                    except IntegrityError as e:
                        session.rollback()
                        # print("url already exists: %s" % (line,))


"""
python database_builder.py --urls ./data/database.db /mnt/nas/datasets/nsfw_data_source_urls/raw_data/ethnicity_asian/
"""


def main():
    parser = argparse.ArgumentParser(description='Short sample app')

    parser.add_argument('--wipe', action="store_true")
    parser.add_argument('--urls', action="store_true")
    parser.add_argument('--images', action="store_true")
    parser.add_argument('--hash', action="store_true")
    parser.add_argument('--count', action="store_true")
    parser.add_argument('--download', action="store_true")
    parser.add_argument('--data-dir')

    parser.add_argument('database')
    parser.add_argument('base_dir')

    args = parser.parse_args()

    print(args)

    database_path = args.database
    base_dir = args.base_dir

    if args.wipe:
        print("Wiping database at %s" % (database_path,))
        try:
            os.remove(database_path)
        except:
            pass

    engine = create_engine('sqlite:///%s' % (database_path,))
    Base.metadata.create_all(engine)
    DBSession = sessionmaker(bind=engine)
    session = DBSession()

    if args.images:
        store_image_paths(args.base_dir, session)

    if args.urls:
        store_item_urls(base_dir, session)

    if args.count:
        item_count = session.query(Item).count()
        print("Item Count: %s" % (item_count,))

    if args.download:
        if not args.data_dir:
            print("Error: missing data directory")
            sys.exit(1)
        download_urls(session, args.data_dir)

    # download missing urls
    temp_dir = tempfile.mkdtemp()
    print("Done!")


if __name__ == "__main__":
    main()




