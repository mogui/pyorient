#!/bin/bash
set -e

PARENT_DIR=$(dirname $(cd "$(dirname "$0")"; pwd))
DOCS_DIR="$PARENT_DIR/docs"

GHPAGES_DIR="$DOCS_DIR/build/html"


if [ ! -d "$GHPAGES_DIR" ]; then
  echo "--- Cloning repo ---"
  git clone https://mogui:$GH_TOKEN@github.com/mogui/pyorient.git $GHPAGES_DIR
  cd $GHPAGES_DIR
  git checkout gh-pages
  git config user.email "mogui-ci@example.com"
  git config user.name "mogui CI"
else
  echo "--- Already cloned repository ---"
fi

# Update branch
cd $GHPAGES_DIR
git checkout gh-pages
git pull

# generate docs
cd $DOCS_DIR
make html

# push them
cd $GHPAGES_DIR
git commit -a -m 'regenerate docs'
git push origin gh-pages
