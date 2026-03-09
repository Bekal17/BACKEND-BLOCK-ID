import os

def tree(path, prefix=''):
    try:
        entries = sorted(os.listdir(path))
    except PermissionError:
        return
    for i, entry in enumerate(entries):
        full = os.path.join(path, entry)
        connector = '+---' if i < len(entries)-1 else '\\---'
        print(prefix + connector + entry)
        if os.path.isdir(full):
            extension = '|   ' if i < len(entries)-1 else '    '
            tree(full, prefix + extension)

root = r'D:\BACKENDBLOCKID\backend_blockid'
print(root)
tree(root)