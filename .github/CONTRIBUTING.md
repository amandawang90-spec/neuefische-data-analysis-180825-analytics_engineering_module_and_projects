## Handling Merge Conflicts in Jupyter Notebooks

Sometimes, when multiple people edit the same notebook, merge conflicts can occur. These conflicts are shown inside notebooks as special markers like:

```bash
<<<<<<< HEAD
print("A")
=========
print("B")

<<<<<<feature-branch
```


### How to fix notebook conflicts:

1. Open the notebook file in a text editor (VS Code recommended).
2. Find the conflict markers (`<<<<<<<`, `=======`, `>>>>>>>`) inside the notebook's JSON code cells.
3. Carefully edit the notebook to remove these markers and decide which code to keep.
4. Save the notebook, test it if possible.
5. Commit the fixed notebook and push to your branch.
6. The automated workflow will detect the fix and continue syncing branches.

---

**If you push notebooks with conflict markers, the automated workflow will fail until conflicts are resolved. Please fix conflicts as soon as possible to avoid blocking merges.**
