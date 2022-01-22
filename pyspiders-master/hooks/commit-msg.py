#!/usr/bin/python

import sys

rules = """The commit must be preceeded with one of the following:
fix:
chore:
dev:
feat:"""


def main():
    with open(sys.argv[1], "r") as fp:
        lines = fp.readlines()

        for idx, line in enumerate(lines):
            if line_valid(idx, line) is False:
                show_rules()
                sys.exit(1)

    sys.exit(0)


prefixes = ["fix:", "chore:", "feat:", "dev:"]


def line_valid(idx, line):
    for item in prefixes:
        if item in line:
            return True
    return False


def show_rules():
    print(rules)


if __name__ == "__main__":
    main()
