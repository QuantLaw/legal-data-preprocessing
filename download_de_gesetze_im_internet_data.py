import argparse
import os
from multiprocessing.pool import Pool

from git import Git, Repo

from statics import DE_ORIGINAL_PATH
from utils.simplify_gii_xml import simplify_gii_xml

REPO_PATH = "../gesetze-im-internet"
REPO_PARENT_PATH = "../"
ITEMS_PATH = f"{REPO_PATH}/data/items/"

GII_REPO_URL = "https://github.com/QuantLaw/gesetze-im-internet.git"


def copy_and_simplify_file(xml_file):
    doknr = xml_file.split(".")[0]
    file_path = os.path.join(ITEMS_PATH, folder, xml_file)
    stripped_date = date.replace("-", "")
    target_file = os.path.join(
        DE_ORIGINAL_PATH, f"{doknr}_{stripped_date}_{stripped_date}.xml"
    )
    simplify_gii_xml(file_path, target_file)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-d", "--dates", nargs="*", help="List dates in format YYYY=mm-dd"
    )
    parser.add_argument(
        "-i",
        "--ignore-not-found",
        help="Ignore that some files are not included in this snapshot",
        action="store_true",
    )

    args = parser.parse_args()

    if os.path.exists(REPO_PATH):
        print(f"Please remove the folder {REPO_PATH}")
        exit(1)

    Git(REPO_PARENT_PATH).clone(GII_REPO_URL)

    repo = Repo(REPO_PATH)
    available_dates = [d.name for d in repo.tags]

    if not args.dates:
        print("Please choose dates to import\nOptions:\n")
        for t in available_dates:
            print(t)
        exit(1)

    for date in args.dates:
        if date not in available_dates:
            raise Exception(f"{date} is not available")

    git = Git(REPO_PATH)
    os.makedirs(DE_ORIGINAL_PATH)

    for date in args.dates:
        git.checkout(date)

        with open(f"{REPO_PATH}/data/not_found.txt") as f:
            not_found = f.read()

        if not args.ignore_not_found and len(not_found.strip()):
            raise Exception(
                f"Some files are not included in snapshot {date}. "
                f"Use another snapshot or --ignore-not-found"
            )

        for folder in [
            f for f in os.listdir(ITEMS_PATH) if os.path.isdir(ITEMS_PATH + f)
        ]:
            folder_path = ITEMS_PATH + folder
            xml_files = [f for f in os.listdir(folder_path) if f.endswith(".xml")]

            with Pool() as p:
                p.map(copy_and_simplify_file, xml_files)

        print(date, "imported")

    print(f"Done. You may now remove `{REPO_PATH}`")
