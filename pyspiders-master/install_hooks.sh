#!/bin/bash

rm -f .git/hooks/commit-msg
rm -f .git/hooks/pre-commit
ln -s -f ../../hooks/commit-msg.py .git/hooks/commit-msg
ln -s -f ../../hooks/pre-commit.py .git/hooks/pre-commit
