import errno, stat
import io
import os
import uuid

import fuse
import iroh
import logging
from fuse import Fuse
# from cbor2 import dumps, loads
from json import dumps, loads

IROH_DATA_DIR = "./iroh_data_dir"

class BaseStat(fuse.Stat):
    def __init__(self):
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

    def to_dict(self):
        return vars(self)

class IrohDocFS:

    write_buffer = {}

    chunk_size = 262144

    def __init__(self, *args, **kwargs):
        self.iroh_doc = kwargs.pop('iroh_doc')
        self.iroh_author = kwargs.pop('iroh_author')
        super(IrohDocFS, self).__init__(*args, **kwargs)
        # Debug logging
        log_level = logging.DEBUG
        self.logger = self._setup_logging(log_level)

        # Find the root of the file system
        self.root_node = self._load_root()

    def _setup_logging(self, log_level):
        logger = logging.getLogger('IrohDocFS')
        logger.setLevel(log_level)
        console = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s %(levelname)s | %(message)s')
        console.setFormatter(formatter)
        console.setLevel(log_level)
        logger.addHandler(console)
        return logger

    def main(self, *args, **kwargs):
        return Fuse.main(self, *args, **kwargs)

    def _on_change(self):
        self.logger.debug("event: on_change")

    def _sync(self, path):
        self.logger.debug("sync: " + path)
        stat_block = loads(self.iroh_doc.get_exact(self.iroh_author, path + '.stat'))
        if path in self.write_buffer:
            try:
                existing = self.iroh_doc.get_exact(self.iroh_author, path + '.data')
                self.iroh_doc.set_bytes(self.iroh_author, path + '.data', existing + io.BytesIO(self.write_buffer[path]))
                self._on_change()
                del self.write_buffer[path]
            except Exception as e:
                return -errno.EIO

    def getattr(self, path):
        self.logger.debug("getattr: " + path)
        st = BaseStat()
        try:
            self._sync(path)
            stat_block = loads(self.iroh_doc.get_exact(self.iroh_author, path + '.stat'))
        except Exception as e:
            return -errno.ENOENT

        if stat_block.get('Type') == 'directory':
            st.st_mode = stat.S_IFDIR | 0o755
            st.st_nlink = 2
        else:
            st.st_mode = stat.S_IFREG | 0o644
            st.st_nlink = 1
            st.st_size = stat_block.get('Size', 0)
        return st

    def readdir(self, path, offset):
        self.logger.debug("readdir: " + path)
        entries = loads(self.iroh_doc.get_exact(self.iroh_author, path + '.dir')).get('Entries', None)
        result = [{

        }]
        if entries:
            for entry in entries:
                result.append(fuse.Direntry(entry.get('Name')))
                # @TODO: Does not play nice with cache
                # yield fuse.Direntry(entry.get('Name'))
        return result

    def utime(self, path, times):
        (utime, mtime) = times
        self.logger.debug('mtime: ' + path + "(" + str(mtime) + ")")

    def _load_root(self):
        return loads(self.iroh_doc.get_exact(self.iroh_author, 'root.json'))

    def _load_node(self, dir_uuid, name):
        key = "fs:%s:%s.json" % (dir_uuid, name)
        return key, self.iroh_doc.get_exact(self.iroh_author, key)

    def _list_children(self, node):
        prefix = "fs:%s:" % node.get('uuid')
        query = iroh.Query.key_prefix(prefix)
        return self.iroh_doc.get_many(self.iroh_author, query)

    def _walk_from_node(self, node, path):
        if len(path) == 0:
            return node
        elif len(path) == 1:
            return self._load_node(node.get('uuid'), path[0])
        else:
            _, target = self._load_node(node.get('uuid'), path[0])
            return self._walk_from_node(target, path[1:])

    def _walk(self, path):
        self.logger.debug("walk: " + path)
        return self._walk_from_node(self.root_node, path.split("/"))

    def _persist(self, parent, name, node):
        return self.iroh_doc.set_bytes(self.iroh_author, "fs:%s:%s.json" % (parent.get('uuid'), name), dumps(node))

    def mkdir(self, path, mode):
        self.logger.debug("mkdir: " + path)
        dir_uuid = uuid.uuid4()
        parent_path, name = path.rsplit('/', 1)
        _, parent_node = self._walk(parent_path)
        new_stat = BaseStat()
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
            return -errno.EIO

    def _read(self, node, size, offset):
        if node.get('type') == 'dir':
            return -errno.EIO
        key = "data:%s" % node.get('data')
        # @TODO: see if offset and size handling is even vaguely correct
        return self.iroh_doc.get_exact(self.iroh_author, key)[offset:offset + size]

    def read(self, path, size, offset):
        self.logger.debug("read: " + path)
        try:
            _, node = self._walk(path)
            some_bytes = self._read(node, size, offset)
        except Exception as e:
            return -errno.ENOENT
        return some_bytes

    def rename(self, path, path1):
        self.logger.debug('moving: ' + path + ' -> ' + path1)
        # Reference: https://www.man7.org/linux/man-pages/man2/rename.2.html
        # Load the source
        try:
            from_key, from_node = self._walk(path)
        except Exception as e:
            return -errno.EIO

        # Try to load the dest, could fail non-fatally
        to_exists = True
        try:
            to_key, to_node = self._walk(path1)
        except Exception as e:
            to_exists = False

        # Do the stuff from the manpage here
        if to_exists:
            pass
        else:
            # Walk to parent and construct to_key
            to_key = ''
            pass

        # OK, try the "rename" (copy & delete)
        try:
            # @TODO: Ugh, ok, we need to rethink this
            # Probably need to return key, node from _walk)
            # And ... finding the key for the target path :(
            self.iroh_doc.set_bytes(self.iroh_author, to_key,
                                    self.iroh_doc.get_exact(self.iroh_author, path + '.stat'))
            self._on_change()
            self.logger.debug('moved: ' + path + ' -> ' + path1)
        except Exception as e:
            return -errno.EIO

def main():
    usage = """
    Naive FUSE-on-Iroh Test
    """ + Fuse.fusage

    node = iroh.IrohNode(IROH_DATA_DIR)
    print("Started Iroh node: {}".format(node.node_id()))

    author = node.author_create()
    print("Created author: {}".format(author.to_string()))

    doc = node.doc_create()
    print("Created doc: {}".format(doc.id().to_string()))
    server = IrohDocFS(
        version="%prog " + fuse.__version__,
        usage=usage,
        dash_s_do='setsingle',
        iroh_doc=doc, iroh_author=author,
    )

    server.parse(errex=1)
    server.main()


if __name__ == '__main__':
    main()
