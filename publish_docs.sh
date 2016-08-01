#!/bin/bash

# Any individual commands that fail will cause the script to exit
set -e

BRANCH=`git branch | sed -n -e 's/^\* \(.*\)/\1/p'`

make docs

# Initialize a blank branch or change to the existing one
# Exit if all fail as a precautionary measure to not trash
# the current branch
git fetch origin
git checkout --orphan gh-pages || git checkout -f gh-pages || exit 1

# Go back to the original branch. Set this as a trap to happen if any
# errors occur
function clean_up() {
   git checkout -f $BRANCH 
}
trap clean_up EXIT

# Start docs from scratch
git ls-files | xargs rm -f
git add -u

# The .nojekyll file prevents github from parsing docs as jekyll format
touch .nojekyll

# Move doc files into root dir, add them, and push them. Exlude hidden
# files and the dot folder
DOC_FILES=`cd docs/_build/html && find . -not -path '*/\.*' -not -path .`
cp -r docs/_build/html/* .
git add -f .nojekyll $DOC_FILES
git commit -m "Published docs" --allow-empty
git push origin gh-pages --force

# Remove the trap and exit normally
trap - EXIT
clean_up
exit 0

