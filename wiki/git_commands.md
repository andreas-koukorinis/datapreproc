# To checkout ( switch to ) a branch :
git checkout <branch_name>

# To commit a file :
git commit -m "Message explaining change" file-path

# To see what files need to be committed :
git status

# To pull from origin
git pull # if it is the same branch
git pull origin branch_name

# To merge from another branch from origin
git merge origin/branch_name

# see differences from a branch from origin
git diff origin/branch_name

# see differences from a branch to another
git diff -w ee4337ca401513ac54e1aeee73d288131f1a9368 fe6eb3a5805d96d87fd0daf9c22980753d2588dd

# to revert a commit
git revert <commithash>
# push if needed 
git push

# to push changes
git push origin branch_name
