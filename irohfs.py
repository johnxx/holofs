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
    def __init__(self, *args, **kwargs):
        self.iroh_doc = kwargs.pop('doc')
        self.iroh_author = kwargs.pop('author')
        super(IrohFS, self).__init__(*args, **kwargs)
        # Debug logging
        log_level = logging.WARN
        self.logger = self._setup_logging(log_level)

    def _setup_logging(self, log_level):
        logger = logging.getLogger('IrohFS')
        logger.setLevel(log_level)
        console = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s %(levelname)s | %(message)s')
        console.setFormatter(formatter)
        console.setLevel(log_level)
        logger.addHandler(console)
        return logger

    def main(self, *args, **kwargs):
        self.logger.info("entered: Fuse.main()")
        return Fuse.main(self, *args, **kwargs)

    # @TODO: Draw the rest of the fucking owl
    def _on_change(self):
        self.logger.info("event: on_change")

    def getattr(self, path):
        self.logger.info("getattr: " + path)
        try:
            # self._sync(path)
            _, node = self._walk(path)
            st = BaseStat(node.get('stat'))
            return st
        except Exception as e:
            return -errno.ENOENT

    def readdir(self, path, offset):
        self.logger.info("readdir: " + path)
        _, node = self._walk(path)
        return self._dir_entries(node)

    def utime(self, path, times):
        self.logger.info("utime (unimplemented): " + path)
        # (utime, mtime) = times
        # self.logger.info('mtime: ' + path + "(" + str(mtime) + ")")

    def _load_root(self):
        key = b'root.json'
        self.logger.info("load: " + str(key))
        query = iroh.Query.key_exact(key, None)
        return key, loads(self.iroh_doc.get_one(query).content_bytes(self.iroh_doc))

    def _latest_contents(self, key):
        query = iroh.Query.key_exact(key.encode('utf-8'), None)
        return self.iroh_doc.get_one(query).content_bytes(self.iroh_doc)

    def _load_node(self, dir_uuid, name):
        key = "fs:%s:%s.json" % (dir_uuid, name)
        self.logger.info("load: " + key)
        return key, loads(self._latest_contents(key))

    def _dir_entries(self, node):
        children = self._list_children(node)
        entries = [
            fuse.Direntry('.'),
            fuse.Direntry('..')
        ]
        for child in children:
            name = child.key().decode('utf-8').split(':', 2)[2].removesuffix('.json')
            entries.append(fuse.Direntry(name))
        return entries

    def _list_children(self, node):
        prefix = "fs:%s:" % node.get('uuid')
        query = iroh.Query.key_prefix(prefix.encode('utf-8'), None)
        return self.iroh_doc.get_many(query)

    def _walk_from_node(self, node, path):
        self.logger.info("_walk_from_node: " + str(path))

        lookup_el = path.pop(0)
        new_key, new_node = self._load_node(node.get('uuid'), lookup_el)

        # We found what we're looking for!
        if len(path) == 0:
            return new_key, new_node
        # Keep looking ...
        else:
            return self._walk_from_node(new_node, path)

    def _walk(self, path):
        self.logger.info("_walk: " + path)
        path = os.path.normpath(path)
        try:
            if os.path.dirname(path) == '/' and os.path.basename(path) == '':
                return 'root.json', self.root_node
            else:
                return self._walk_from_node(self.root_node, path.removeprefix('/').split('/'))
        except Exception as e:
            # print(traceback.format_exc())
            self.logger.info("_walk: " + path + ": file not found")
            return None, None

    def _persist(self, parent, name, node):
        key = "fs:%s:%s.json" % (parent.get('uuid'), name)
        self.logger.info("persist: " + key)
        return self.iroh_doc.set_bytes(self.iroh_author, key.encode('utf-8'), dumps(node).encode('utf-8'))

    def mkdir(self, path, mode):
        self.logger.info("mkdir: " + path)
        node_uuid = str(uuid.uuid4())
        parent_path = os.path.dirname(path)
        name = os.path.basename(path)
        _, parent_node = self._walk(parent_path)
        new_stat = IrohStat()
        new_stat.st_mode = stat.S_IFDIR | 0o755
        new_stat.st_nlink = 2
        new_dir = {
            "type": "dir",
            "stat": new_stat.to_dict(),
            "uuid": dir_uuid
        }
        try:
            self._persist(parent_node, name, new_dir)
            self._on_change()
        except Exception as e:
            print(traceback.format_exc())
            return -errno.EIO

    def open(self, path, flags):
        self.logger.info("open: " + path)


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
