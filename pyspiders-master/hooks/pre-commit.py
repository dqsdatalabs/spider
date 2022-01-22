#!/usr/bin/python

import sys
import subprocess

file_name = subprocess.check_output(
    'git diff --staged --name-only --diff-filter=d -- "*.py"')

remove_comments = False


def main():
    for file in file_name.decode('utf-8').split('\n')[:-1]:
        try:
            open(file, "r")
        except:
            continue
        with open(file, "r") as fp:
            lines = fp.readlines()

            for idx, line in enumerate(lines):

                if "print('" in line.strip():
                    show_rules()
                    sys.exit(1)

                if line.strip() != '' and line.strip()[0] == "#":
                    continue

    sys.exit(0)


rules = '''Please make sure to delete all prints before pushing; So that we can merge your code :D'''


def show_rules():
    print(rules)


if __name__ == "__main__":
    main()

sys.exit(1)
