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


def import_images(args):
    session = get_session(args)
    store_image_paths(args.source_dir, session)


def import_urls(args):
    session = get_session(args)
    store_item_urls(args.source_dir, session)


def download(args):
    session = get_session(args)
    download_urls(session, args.data_dir)


def count(args):
    session = get_session(args)
    item_count = session.query(Item).count()
    print("Item Count: %s" % (item_count,))


def get_session(args):
    database_path = args.data_dir + "/database.db"

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
    return session


def main():
    parser = argparse.ArgumentParser(description="Builds a machine learning database")

    parser.add_argument('--wipe', action="store_true", help="wipe the database")
    parser.add_argument('--hash', action="store_true")
    parser.set_defaults(func=lambda *_, **__: None)  # need a default function that will never fail

    subparsers = parser.add_subparsers(title="subcommands", dest="subparser_name")

    images_parser = subparsers.add_parser("images", help="import item info from files")
    images_parser.add_argument("data_dir")
    images_parser.add_argument("source_dir")
    images_parser.set_defaults(func=import_images)

    urls_parser = subparsers.add_parser("urls", help="import url info from files")
    urls_parser.add_argument("data_dir")
    urls_parser.add_argument("source_dir")
    urls_parser.set_defaults(func=import_urls)

    download_parser = subparsers.add_parser("download", help="Download missing files from URLs")
    download_parser.add_argument("data_dir")
    download_parser.set_defaults(func=download)

    count_parser = subparsers.add_parser("count", help="count the number of items")
    count_parser.add_argument("data_dir")
    count_parser.set_defaults(func=count)


    args = parser.parse_args()

    if not args.subparser_name:
        parser.parse_args(["-h"])
        sys.exit(1)

    print(args)


    args.func(args)
    print("Done!")


if __name__ == "__main__":
    main()




