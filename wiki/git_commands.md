# To create a branch :
git checkout -b <branch_name>

# To delete a branch locally :
git branch -d <branch_name> # note that this will not delete the branch from origin

# To delete a branch from origin
git push origin :<branch_name>

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

# The most powerful command of them all!
git stash #this saves the changes you have made since the last commit, and alows you to checkout to another branch and then come back to work on this branch. then you can apply the stash
