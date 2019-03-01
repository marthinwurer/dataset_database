import argparse
import glob
import os
import tempfile

from sqlalchemy.exc import IntegrityError

from SQLTypes import Item

"""
$ python database_builder.py /mnt/nas/datasets/database.db /mnt/nas/datasets/visualgenome/VG_100K/
python database_builder.py --wipe /mnt/nas/datasets/database.db /mnt/nas/datasets/test_data/
"""


def store_image_paths(base_dir, session):
    types = [".jpg", ".png", ".gif"]
    for root, dirs, files in os.walk(base_dir):
        print(files)
        basenames = [os.path.splitext(file) for file in files]
        print(basenames)
        image_files = filter(lambda x: os.path.splitext(x)[1] in types, files)
        for file in image_files:
            full_path = os.path.join(root, file)
            try:
                item = Item(path=full_path)
                session.add(item)
            except IntegrityError as e:
                print("image already exists: %s" % (full_path,))


def store_item_urls(base_dir, session):
    for root, dirs, files in os.walk(base_dir):
        print(root, dirs, files)
        images_dir = os.path.join(root, "IMAGES")
        # find images files
        for file in glob.iglob(root + "/urls.txt"):
            print("found url file: %s" % (file,))

            with open(file) as f:
                for line in f:
                    line = line.strip()
                    try:
                        item = Item(url=line)
                        session.add(item)
                    except IntegrityError as e:
                        print("url already exists: %s" % (line,))



def main():
    parser = argparse.ArgumentParser(description='Short sample app')

    parser.add_argument('--wipe', action="store_true")
    parser.add_argument('database')
    parser.add_argument('base_dir')

    args = parser.parse_args()

    print(args)

    database_path = args.database
    base_dir = args.base_dir
    conn = sqlite3.connect(database_path)
    # create a table
    if args.wipe:

        conn.close()  # close then reopen after it's wiped
        print("Wiping database at %s" % (database_path,))
        os.remove(database_path)
        conn = sqlite3.connect(database_path)
        Item.build_table(conn)

    # store_image_paths(args.base_dir, conn)
    store_item_urls(base_dir, conn)

    # download missing urls
    temp_dir = tempfile.mkdtemp()
    print("Done!")


if __name__ == "__main__":
    main()




