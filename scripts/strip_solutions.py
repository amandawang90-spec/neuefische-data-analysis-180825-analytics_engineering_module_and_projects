import nbformat
import os
import glob

def strip_cells_by_tag(nb_path, tags_to_remove):
    with open(nb_path, 'r', encoding='utf-8') as f:
        nb = nbformat.read(f, as_version=4)

    original_cell_count = len(nb.cells)

    # Remove cells that have any of the tags
    nb.cells = [
        cell for cell in nb.cells
        if not any(tag in cell.metadata.get('tags', []) for tag in tags_to_remove)
    ]

    if len(nb.cells) == 0:
        print(f"📭 All cells removed in {nb_path}, deleting file.")
        os.remove(nb_path)
    else:
        with open(nb_path, 'w', encoding='utf-8') as f:
            nbformat.write(nb, f)

tags_to_strip = ['solution', 'teacher']
notebooks = glob.glob('**/*.ipynb', recursive=True)

for notebook in notebooks:
    strip_cells_by_tag(notebook, tags_to_strip)
