import errno, stat
import os
import traceback
import uuid

import fuse
import iroh
import logging
from fuse import Fuse
# from cbor2 import dumps, loads
from json import dumps, loads

fuse.fuse_python_api = (0, 2)

class IrohStat(fuse.Stat):
    def __init__(self, *initial_data, **kwargs):

        self.st_mode = 0
        self.st_ino = 0
        self.st_dev = 0
        self.st_nlink = 0
        self.st_uid = 0
        self.st_gid = 0
        self.st_size = 0
        self.st_atime = 0
        self.st_mtime = 0
        self.st_ctime = 0

        for dictionary in initial_data:
            for key in dictionary:
                setattr(self, key, dictionary[key])
        for key in kwargs:
            setattr(self, key, kwargs[key])

    def to_dict(self):
        return vars(self)


class IrohFS(Fuse):
    pass


if __name__ == '__main__':
    usage = """
    Naive FUSE-on-Iroh Test
    """ + Fuse.fusage

    xdg_data_home = os.environ.get('XDG_DATA_HOME', os.path.expanduser('~/.local/share'))
    iroh_data_dir = os.environ.get('IROH_DATA_DIR', os.path.join(xdg_data_home, 'iroh'))

    iroh_node = iroh.IrohNode(iroh_data_dir)
    print("Started Iroh node: {}".format(iroh_node.node_id()))

    authors = iroh_node.author_list()
    author = authors[0]
    print("Assumed author id: {}".format(author.to_string()))

    doc_id = 'rschuirxucckrvvesyrgq5rqvlaskfwvmdgoekcbjdjlvem5k7wq'
    doc = iroh_node.doc_open(doc_id)
    print("Opened doc: {}".format(doc.id()))

    query = iroh.Query.key_exact(b'root.json', None)
    root_node = doc.get_one(query)

    if not root_node:
        root_stat = IrohStat()
        root_stat.st_mode = stat.S_IFDIR | 0o755
        root_stat.st_nlink = 2
        newfs = {
            'type': 'dir',
            "stat": root_stat.to_dict(),
            'uuid': str(uuid.uuid4())
        }
        doc.set_bytes(author,  b'root.json', dumps(newfs).encode('utf-8'))

    server = IrohFS(
        version="%prog " + fuse.__version__,
        usage=usage,
        dash_s_do='setsingle',
        doc=doc, author=author,
    )

    server.parse(errex=1)
    server.main()
