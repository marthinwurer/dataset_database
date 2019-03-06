import json
import random
import tkinter as tk
from PIL import Image, ImageTk  # Place this at the end (to avoid any conflicts/errors)
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from SQLTypes import Base, Item


def resize_image(desired, image: Image):
    max_size = max(image.size)
    scale = min(1, desired / max_size)
    out = image.resize([int(scale * s) for s in image.size], Image.ANTIALIAS)
    return out


IMAGE_SIZE = 950


def get_image_list():
    creds_file = "creds_remote.json"
    with open(creds_file) as cred_file:
        creds = json.load(cred_file)

    user = creds['user']
    password = creds['pass']
    hostname = creds['host']
    db_name = creds['db']

    engine = create_engine('postgresql://%s:%s@%s/%s' % (user, password, hostname, db_name))
    Base.metadata.create_all(engine)
    DBSession = sessionmaker(bind=engine)
    session = DBSession()

    data_dir = "/mnt/nas/datasets/my_db/data/"
    print("loading images")
    items = session.query(Item).filter(Item.path != None).all()
    random.shuffle(items)
    paths = list(map(lambda x: data_dir + x.path, items))

    return paths

class UI(object):
    def __init__(self):
        window = tk.Tk()
        #window.geometry("500x500") # (optional)
        self.filelist = get_image_list()
        # self.filelist = [
        #     "/mnt/nas/datasets/test_data/o021b36njyi21.jpg",
        #     "/mnt/nas/datasets/test_data/g7hlwqiehxi21.jpg",
        # ]
        self.image_index = 0
        imagefile = self.filelist[self.image_index]
        image = Image.open(self.filelist[self.image_index])
        print(image)
        image = resize_image(IMAGE_SIZE, image)
        # image.show()
        img = ImageTk.PhotoImage(image)
        print(img)
        self.tk_image = img  # need to assign the image to prevent garbage collection
        lbl = tk.Label(window, image=img)
        lbl.grid(column=0, row=1)
        # lbl.pack()

        self.lbl = lbl

        def clicked():
            self.image_index += 1
            self.image_index = self.image_index % len(self.filelist)
            image = Image.open(self.filelist[self.image_index])
            image = resize_image(IMAGE_SIZE, image)
            img = ImageTk.PhotoImage(image)
            self.tk_image = img
            self.lbl.configure(image=img)

        btn = tk.Button(window, text="Next", command=clicked)
        btn.grid(column=0, row=0)

        self.window = window


def main():
    ui = UI()
    ui.window.mainloop()

if __name__ == "__main__":
    main()

