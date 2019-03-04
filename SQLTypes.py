import base64
import hashlib
import os
from urllib.parse import urlparse

import requests
from PIL import Image
from sqlalchemy import Column, ForeignKey, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker, Query
from sqlalchemy import create_engine

Base = declarative_base()


class Item(Base):
    __tablename__ = 'item'
    # Here we define columns for the table person
    # Notice that each column is also a normal Python instance attribute.
    id = Column(Integer, primary_key=True)
    path = Column(String, unique=True)
    url = Column(String, unique=True)
    hash = Column(String, unique=True)
    author = Column(String)
    title = Column(String)
    description = Column(String)
    alias = Column(Integer)
    success = Column(Integer)
    tags = relationship("ItemTag", back_populates="item")

    def __repr__(self):
        return "Item(path=%r, url=%s, hash=%s, author=%s)" % (self.path, self.url, self.hash, self.author)

    def load_image(self):
        """Load the file into a PIL image"""
        return Image.open(self.path)


class Tag(Base):
    __tablename__ = 'tag'
    # Here we define columns for the table person
    # Notice that each column is also a normal Python instance attribute.
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)
    description = Column(String)
    alias = Column(Integer)
    items = relationship("ItemTag", back_populates="tag")


class ItemTag(Base):
    __tablename__ = 'itemtag'
    # Here we define columns for the table person
    # Notice that each column is also a normal Python instance attribute.
    id = Column(Integer, primary_key=True)
    item_id = Column(Integer, ForeignKey('item.id'))
    tag_id = Column(Integer, ForeignKey('tag.id'))
    item = relationship("Item", back_populates="tags")
    tag = relationship("Tag", back_populates="items")




def hash_path(digest, dir_len, subdirs=4):
    dirs = [digest[i:i + dir_len] for i in range(0, len(digest), dir_len)]
    for i in range(subdirs):
        dirs.append(digest[i*4:(i+1)*4])
    # print(dirs)
    return (os.path.join(*dirs[:subdirs]), "".join(dirs[subdirs:]))


def next_hashless(session):
    return query_iterator(
        session.query(Item).filter(
                                Item.hash == None,
                                Item.url != None,
                                Item.alias == None,
                                Item.success == None))


def query_iterator(query: Query):
    finished = False
    while not finished:
        item = query.first()
        if item is None:
            finished = True
        else:
            yield item


def download_urls(session, data_dir):
    for item in next_hashless(session):
        try:
            print("Downloading %s" % (item.url,))
            download_item(item, data_dir, session)
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            print("Failed")
            item.success = 0

        # update database
        session.add(item)
        session.commit()


def download_item(item: Item, dir, session):
    skip_types = [".mp4"]


    # download file
    filename = os.path.basename(urlparse(item.url).path)
    # print("Filename: %s" % (filename,))
    extension = os.path.splitext(filename)[1]
    if extension in skip_types:
        raise ValueError(extension)
    file_path = os.path.join(dir, filename)

    r = requests.get(item.url, stream=True)
    # print("Headers: %s" % (r.headers,))

    hasher = get_hasher()

    with open(file_path, 'wb') as fd:
        for chunk in r.iter_content(chunk_size=128):
            fd.write(chunk)
            # hash file
            hasher.update(chunk)

    digest = hasher.hexdigest()

    # print(digest)
    # print(base64.b32encode(hasher.digest()))

    # check database to see if the hash exists
    exists = session.query(Item).filter_by(hash=digest).first()
    if not exists:
        # new image, save and move it
        # compute path
        path, new_name = hash_path(digest, 2)
        new_filename = new_name+ os.path.splitext(filename)[1]

        # move file
        new_path = os.path.join(".", path, new_filename)
        new_dir = os.path.join(dir, path)
        new_location = os.path.join(dir, new_path)

        os.makedirs(new_dir, exist_ok=True)
        os.rename(file_path, new_location)

        item.hash = digest
        item.path = new_path
    else:
        os.remove(file_path)
        # image already exists, set it as an alias
        item.alias = exists.id


def default_session(database_path=None):
    if database_path is None:
        database_path = "/mnt/nas/datasets/my_db/data/database.db"
    engine = create_engine('sqlite:///%s' % (database_path,))
    Base.metadata.create_all(engine)
    DBSession = sessionmaker(bind=engine)
    session = DBSession()
    return session


def hash_file(path):
    hasher = get_hasher()
    with open(path, mode='rb') as file:
        for chunk in file:
            hasher.update(chunk)
    return hasher


def buffer_iterator(buffer, max_size=1024):
    while True:
        buf = buffer.read(max_size)
        if not buf:
            break
        yield buf



def get_hasher():
    salt = b"cat"
    hasher = hashlib.md5()
    hasher.update(salt)  # this is not secure, but just because I want to
    return hasher

def main():
    database_path = "/mnt/nas/datasets/my_db/data/database.db"
    engine = create_engine('sqlite:///%s' % (database_path,))
    Base.metadata.create_all(engine)
    DBSession = sessionmaker(bind=engine)
    session = DBSession()
    print(session.query(Item).count())


if __name__ == "__main__":
    main()






