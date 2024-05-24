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

fuse.fuse_python_api = (0, 2)

class BaseStat(fuse.Stat):
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
        self.root_key, self.root_node = self._load_root()

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
        self.logger.debug("entered: Fuse.main()")
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
        try:
            # self._sync(path)
            _, node = self._walk(path)
            st = BaseStat(node.get('stat'))
            return st
        except Exception as e:
            return -errno.ENOENT

    def readdir(self, path, offset):
        self.logger.debug("readdir: " + path)
        _, node = self._walk(path)
        return self._dir_entries(node)

    def utime(self, path, times):
        (utime, mtime) = times
        self.logger.debug('mtime: ' + path + "(" + str(mtime) + ")")

    def _load_root(self):
        key = b'root.json'
        self.logger.debug("load: " + str(key))
        return key, loads(self.iroh_doc.get_exact(self.iroh_author, key, include_empty=True).content_bytes(self.iroh_doc))

    def _load_node(self, dir_uuid, name):
        key = "fs:%s:%s.json" % (dir_uuid, name)
        self.logger.debug("load: " + key)
        return key, loads(self.iroh_doc.get_exact(self.iroh_author, key.encode('utf-8'), include_empty=True).content_bytes(self.iroh_doc))

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
        self.logger.debug("walk_from_node: " + str(path))
        if path[0] == '':
            return 'root.json', node
        elif len(path) == 1:
            return self._load_node(node.get('uuid'), path[0])
        else:
            _, target = self._load_node(node.get('uuid'), path[0])
            return self._walk_from_node(target, path[1:])

    def _walk(self, path):
        self.logger.debug("walk: " + path)
        try:
            return self._walk_from_node(self.root_node, path.split("/")[1:])
        except Exception as e:
            print(str(e))
            return None, None

    def _persist(self, parent, name, node):
        key = "fs:%s:%s.json" % (parent.get('uuid'), name)
        self.logger.debug("persist: " + key)
        return self.iroh_doc.set_bytes(self.iroh_author, key.encode('utf-8'), dumps(node).encode('utf-8'))

    def mkdir(self, path, mode):
        self.logger.debug("mkdir: " + path)
        dir_uuid = str(uuid.uuid4())
        parent_path = os.path.dirname(path)
        name = os.path.basename(path)
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

    def open(self, path, flags):
        self.logger.debug("open: " + path)

    def _data_key(self, node):
        return "data:%s" % node.get('data')

    def _node_key(self, parent_uuid, name):
       return "fs:%s:%s.json" % parent_uuid, name

    def _read(self, node, size=0, offset=0):
        if node.get('type') == 'dir':
            print("HONK!")
            return -errno.EIO
        key = self._data_key(node)
        # @TODO: see if offset and size handling is even vaguely correct
        if size == 0:
            print("HONK1")
            return self.iroh_doc.get_exact(self.iroh_author, key, include_empty=True).content_bytes(self.iroh_doc)[offset:]
        else:
            print("HONK2")
            return self.iroh_doc.get_exact(self.iroh_author, key, include_empty=True).content_bytes(self.iroh_doc)[offset:offset + size]

    def read(self, path, size, offset):
        self.logger.debug("read: " + path)

        _, node = self._walk(path)
        if not node:
            return -errno.ENOENT

        try:
            return self._read(node, size, offset)
        except Exception as e:
            print(str(e))
            return -errno.EIO

    def mknod(self, path, mode, dev):
        self.logger.debug("mknod: " + path)

        _, node_exists = self._walk(path)
        if node_exists:
            return -errno.EEXIST

        data_uuid = str(uuid.uuid4())
        parent_path = os.path.dirname(path)
        name = os.path.basename(path)
        _, parent_node = self._walk(parent_path)
        new_stat = BaseStat()
        new_stat.st_mode = stat.S_IFREG | 0o644
        new_stat.st_nlink = 1
        new_file = {
            "type": "file",
            "stat": new_stat.to_dict(),
            "data": data_uuid
        }
        try:
            self._persist(parent_node, name, new_file)
            data_key = 'data:%s' % new_file.get('data')
            print('set key: ' + data_key)
            self.iroh_doc.set_bytes(self.iroh_author, data_key.encode('utf-8'), b'\x00')
            self._on_change()
        except Exception as e:
            print(str(e))
            return -errno.EIO

    def _write(self, node, buf, offset):
        # Yeah, that's definitely not ... write
        contents = self._read(node)
        contents = contents[:offset] + buf + contents[offset + len(buf):]
        data_key = self._data_key(node)
        print('key: ' + data_key + "@" + str(offset) + " contents: " + str(contents))
        self.iroh_doc.set_bytes(self.iroh_author, data_key.encode('utf-8'), contents)

    def write(self, path, buf, offset):
        self.logger.debug("write: " + path + "@" + str(offset))
        try:
            key, node = self._walk(path)
            self._write(node, buf, offset)
            self._on_change()
        except Exception as e:
            return -errno.EIO

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
                                    self.iroh_doc.get_exact(self.iroh_author, from_key, include_empty=True).content_bytes(self.iroh_doc))
            self.iroh_doc.delete(self.iroh_author, from_key)
            self._on_change()
            self.logger.debug('moved: ' + path + ' -> ' + path1)
        except Exception as e:
            return -errno.EIO

    def fsync(self, path, isfsyncfile):
        pass

    def statfs(self):
        return os.statvfs(".")

    def truncate(self, path, length):
        self.logger.debug("truncate: " + path)
        if length == 0:
            try:
                key, node = self._walk(path)
                self._write(node, b'\x00', 0)
                self._on_change()
            except Exception as e:
                print(str(e))
                return -errno.EIO
        else:
            return -errno.EIO

if __name__ == '__main__':
    usage = """
    Naive FUSE-on-Iroh Test
    """ + Fuse.fusage

    node = iroh.IrohNode(IROH_DATA_DIR)
    print("Started Iroh node: {}".format(node.node_id()))

    # author = node.author_create()
    # print("Created author: {}".format(author.to_string()))

    # doc = node.doc_create()
    # print("Created doc: {}".format(doc.id()))

    author_id = 'a3cooqmeejt4azujv7a3vmwibne7ibxxwxd56fqu6gtw3uq2faea'
    author = iroh.AuthorId.from_string(author_id)
    print("Assumed author: {}".format(author.to_string()))

    doc_id = '5scnsj263r5mfg3rtll7opgb7c2lt2vl6azhpy2drkov2uyi57aq'
    doc = node.doc_open(doc_id)
    print("Opened doc: {}".format(doc.id()))

    root_node = doc.get_exact(author, b'root.json', include_empty=False)
    if not root_node:
        root_stat = BaseStat()
        root_stat.st_mode = stat.S_IFDIR | 0o755
        root_stat.st_nlink = 2
        newfs = {
            'type': 'dir',
            "stat": root_stat.to_dict(),
            'uuid': str(uuid.uuid4())
        }
        doc.set_bytes(author,  b'root.json', dumps(newfs).encode('utf-8'))
    server = IrohDocFS(
        version="%prog " + fuse.__version__,
        usage=usage,
        dash_s_do='setsingle',
        iroh_doc=doc, iroh_author=author,
    )

    server.parse(errex=1)
    server.main()
