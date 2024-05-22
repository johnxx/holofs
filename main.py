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

class IrohDocFS(Fuse):

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

    # def _sync(self, path):
    #     self.logger.debug("sync: " + path)
    #     stat_block = loads(self.iroh_doc.get_exact(self.iroh_author, path + '.stat'))
    #     if path in self.write_buffer:
    #         try:
    #             existing = self.iroh_doc.get_exact(self.iroh_author, path + '.data')
    #             self.iroh_doc.set_bytes(self.iroh_author, path + '.data', existing + io.BytesIO(self.write_buffer[path]))
    #             self._on_change()
    #             del self.write_buffer[path]
    #         except Exception as e:
    #             return -errno.EIO

    def getattr(self, path):
        self.logger.debug("getattr: " + path)
        st = BaseStat()
        try:
            # self._sync(path)
            _, node = self._walk(path)
            stat_block = node.get('stat')
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
        _, node = self._walk(path)
        return self._dir_entries(node)

    def utime(self, path, times):
        (utime, mtime) = times
        self.logger.debug('mtime: ' + path + "(" + str(mtime) + ")")

    def _load_root(self):
        key = 'root.json'
        return key, loads(self.iroh_doc.get_exact(self.iroh_author, key).content_bytes(self.iroh_doc))

    def _load_node(self, dir_uuid, name):
        key = "fs:%s:%s.json" % (dir_uuid, name)
        return key, loads(self.iroh_doc.get_exact(self.iroh_author, key).content_bytes(self.iroh_doc))

    def _dir_entries(self, node):
        children = self._list_children(node)
        entries = [
            fuse.Direntry('.'),
            fuse.Direntry('..')
        ]
        for child in children:
            name = child.get('key').split(':', 2)[2].removesuffix('.json')
            entries.append(fuse.Direntry(name))
        return entries

    def _list_children(self, node):
        prefix = "fs:%s:" % node.get('uuid')
        query = iroh.Query.key_prefix(prefix, None)
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
        try:
            return self._walk_from_node(self.root_node, path.split("/"))
        except Exception as e:
            return None, None

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

    def _read(self, node, size=0, offset=0):
        if node.get('type') == 'dir':
            return -errno.EIO
        key = "data:%s" % node.get('data')
        # @TODO: see if offset and size handling is even vaguely correct
        if size == 0:
            return self.iroh_doc.get_exact(self.iroh_author, key).content_bytes(self.iroh_doc)[offset:]
        else:
            return self.iroh_doc.get_exact(self.iroh_author, key).content_bytes(self.iroh_doc)[offset:offset + size]

    def read(self, path, size, offset):
        self.logger.debug("read: " + path)

        _, node = self._walk(path)
        if not node:
            return -errno.ENOENT

        try:
            return self._read(node, size, offset)
        except Exception as e:
            return -errno.EIO

    def _write(self, node, buf, offset):
        contents = self._read(node)
        contents[offset:offset + len(buf)] += buf
        data_key = node.get('data')
        return self.iroh_doc.set_bytes(self.iroh_author, data_key, contents)

    def write(self, path, buf, offset):
        self.logger.debug("write: " + path + "@" + str(offset))
        key, node = self._walk(path)
        return self._write(node, buf, offset)

    def rename(self, path, path1):
        self.logger.debug('moving: ' + path + ' -> ' + path1)
        # Reference: https://www.man7.org/linux/man-pages/man2/rename.2.html
        # Load the source
        from_key, from_node = self._walk(path)
        if not from_node:
            return -errno.ENOENT

        # Try to load the dest, could fail non-fatally
        to_key, to_node = self._walk(path1)

        if to_node:
            if to_node.get('type') == 'file' and from_node('type') == 'dir':
                return -errno.ENOTDIR
        else:
            # If dest doesh't exist, check if its parent does
            # Walk to parent and construct to_key
            to_parent_key, to_parent_node = self._walk(os.path.dirname(path1))
            if not to_parent_node:
                return -errno.ENOENT
            to_key = "fs:%s:%s.json" % (to_parent_node.get('uuid'), os.path.basename(path1))

        # OK, try the "rename" (copy & delete)
        try:
            self.iroh_doc.set_bytes(self.iroh_author, to_key,
                                    self.iroh_doc.get_exact(self.iroh_author, from_node).content_bytes(self.iroh_doc))
            self.iroh_doc.delete(self.iroh_author, from_key)
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
