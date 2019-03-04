import os
import shutil

from tqdm import tqdm


def clean_dirs(base_dir):
    paths_to_wipe = []
    for root, dirs, files in tqdm(os.walk(base_dir)):
        # print(files, dirs, root)
        if len(dirs) + len(files) == 0:
            print("%s is empty!" % (root,))
            paths_to_wipe.append(root)

    return paths_to_wipe


def main():

    new_base_dir = "/mnt/nas/datasets/my_db/data/"
    # test_dir = "/mnt/nas/datasets/test_data"
    # copy_dir = "/mnt/nas/datasets/temp"
    # try:
    #     shutil.rmtree(copy_dir)
    # except Exception as e:
    #     print(e)
    #     pass
    # shutil.copytree(test_dir, copy_dir)
    # target_dir = copy_dir
    target_dir = new_base_dir
    paths = clean_dirs(target_dir)

    for path in tqdm(paths):
        os.removedirs(path)

    paths = clean_dirs(target_dir)
    print(paths)



























if __name__ == "__main__":
    main()
