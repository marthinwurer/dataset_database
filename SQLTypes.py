import base64
import hashlib
import os
from urllib.parse import urlparse

import requests
from PIL import Image
from sqlalchemy import Column, ForeignKey, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
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
    tags = relationship("ItemTag", back_populates="item")

    def __repr__(self):
        return "Item(path=%r, url=%s, hash=%s, author=%s)" % (self.path, self.url, self.hash, self.author)

    def load_image(self):
        """Load the file into a PIL image"""
        return Image.open(self.path)

    def hash_file(self):
        """load the file and hash it"""
        pass


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


def main():
    # Create an engine that stores data in the local directory's
    # sqlalchemy_example.db file.
    engine = create_engine('sqlite:///sqlalchemy_example.db')

    # Create all tables in the engine. This is equivalent to "Create Table"
    # statements in raw SQL.
    Base.metadata.create_all(engine)


def hash_path(digest, subdirs=4, dir_len=4):
    dirs = [digest[i:i + dir_len] for i in range(0, len(digest), dir_len)]
    for i in range(subdirs):
        dirs.append(digest[i*4:(i+1)*4])
    # print(dirs)
    return (os.path.join(*dirs[:subdirs]), "".join(dirs[subdirs:]))


def download_item(item: Item, dir, session):

    # download file
    filename = os.path.basename(urlparse(item.url).path)
    # print("Filename: %s" % (filename,))
    file_path = os.path.join(dir, filename)

    r = requests.get(item.url, stream=True)
    # print("Headers: %s" % (r.headers,))

    salt = b"cat"
    hasher = hashlib.md5()
    hasher.update(salt)  # this is not secure, but just because I want to

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
        path, new_name = hash_path(digest)
        new_name += os.path.splitext(filename)[1]
        # print(path, new_name)
        # move file
        new_dir = os.path.join(dir, path)
        new_path = os.path.join(new_dir, new_name)
        os.makedirs(new_dir, exist_ok=True)
        os.rename(file_path, new_path)

        item.hash = digest
        item.path = new_path
    else:
        # image already exists, set it as an alias
        item.alias = exists.id
        session.update(item)

    # update database
    session.add(item)
    session.commit()






if __name__ == "__main__":
    main()






