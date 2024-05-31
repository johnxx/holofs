import errno, stat
import functools
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

def _flag2mode(flags):
    md = {os.O_RDONLY: 'rb', os.O_WRONLY: 'wb', os.O_RDWR: 'wb+'}
    m = md[flags & (os.O_RDONLY | os.O_WRONLY | os.O_RDWR)]

    if flags | os.O_APPEND:
        m = m.replace('w', 'a', 1)

    return m

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


class IrohFileHandle(object):
    def __init__(self, key, node, file):
        self.key = key
        self.node = node
        self.file = file
        self.fd = self.file.fileno()


class IrohFS(Fuse):
    def __init__(self, *args, **kwargs):
        self.iroh_doc = kwargs.pop('doc')
        self.iroh_author = kwargs.pop('author')
        self.state_dir = kwargs.pop('state_dir')

        super(IrohFS, self).__init__(*args, **kwargs)

        # Debug logging
        log_level = logging.INFO

        self.logger = self._setup_logging(log_level)
        self.root_key, self.root_node = self._load_root()

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

    def _on_change(self):
        self.logger.info("event: on_change")

    def fgetattr(self, path, fh):
        self.logger.info("fgetattr: " + path)
        # if not node:
        #     try:
        #         self.logger.warning("fgetattr: called without fh!")
        #         _, node = self._walk(path)
        #     except Exception as e:
        #         return -errno.ENOENT
        return IrohStat(fh.node.get('stat'))


    def getattr(self, path):
        self.logger.info("getattr: " + path)
        try:
            _, node = self._walk(path)
            st = IrohStat(node.get('stat'))
            return st
        except Exception as e:
            self.logger.info("getattr: " + path + ": no such file or directory")
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

    def _find_entry(self, dir_uuid, name):
        entry_key_prefix = "fs/%s/%s/" % (dir_uuid, name)
        self.logger.info("find_entry: " + entry_key_prefix)
        query = iroh.Query.key_prefix(entry_key_prefix.encode('utf-8'), None)
        # @TODO: This should be a get many that we filter
        entry = self.iroh_doc.get_one(query)
        if entry:
            elements = entry.key().decode('utf-8').split('/')
            stat_key = self._stat_key(elements[-1].removesuffix('.json'))
            return stat_key
        else:
            return None

    def _load_node(self, stat_key):
        self.logger.info("load: " + stat_key)
        return stat_key, loads(self._latest_contents(stat_key))

    def _dir_entries(self, node):
        children = self._list_children(node)
        entries = [
            fuse.Direntry('.'),
            fuse.Direntry('..')
        ]
        for child in children:
            entries.append(self._dir_entry_from_key(child.key()))
        return entries

    def _list_children(self, node):
        prefix = "fs/%s/" % node.get('uuid')
        query = iroh.Query.key_prefix(prefix.encode('utf-8'), None)
        return self.iroh_doc.get_many(query)

    def _walk_from_node(self, node, path):
        self.logger.info("_walk_from_node: " + str(path))

        lookup_el = path.pop(0)
        stat_key = self._find_entry(node.get('uuid'), lookup_el)
        if not stat_key:
            return None, None

        new_key, new_node = self._load_node(stat_key)

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
            print(traceback.format_exc())
            self.logger.info("_walk: " + path + ": file not found")
            return None, None

    def _stat_key(self, node_uuid):
        return "stat/%s.json" % node_uuid

    def _data_key(self, node_uuid):
        return "data/%s" % node_uuid

    def _entry_key(self, parent_uuid, name, node_uuid):
        return "fs/%s/%s/%s.json" % (parent_uuid, name, node_uuid)

    def _dir_entry_from_key(self, key):
        elements = key.decode('utf-8').split('/')
        name = elements[2].removesuffix('.json')
        return fuse.Direntry(name)

    def _persist(self, parent, name, node):
        stat_key = self._stat_key(node.get('uuid'))
        entry_key = self._entry_key(parent.get('uuid'), name, node.get('uuid'))
        self.logger.info("dir entry: " + entry_key + " stat: " + stat_key)
        self.iroh_doc.set_bytes(self.iroh_author, stat_key.encode('utf-8'), dumps(node).encode('utf-8'))
        self.iroh_doc.set_bytes(self.iroh_author, entry_key.encode('utf-8'), b'\x00')
        return stat_key, node

    def mkdir(self, path, mode):
        self.logger.info("mkdir: " + path)

        parent_path = os.path.dirname(path)
        _, parent_node = self._walk(parent_path)

        name = os.path.basename(path)
        node_uuid = str(uuid.uuid4())

        new_stat = IrohStat()
        new_stat.st_mode = stat.S_IFDIR | 0o755
        new_stat.st_nlink = 2
        new_dir = {
            "type": "dir",
            "stat": new_stat.to_dict(),
            "uuid": node_uuid
        }
        try:
            self._persist(parent_node, name, new_dir)
            self._on_change()
        except Exception as e:
            print(traceback.format_exc())
            return -errno.EIO

    def _new_node(self, type, name):
        if type not in ['file', 'dir']:
            raise Exception("Unknown node type: " + type)
        new_stat = IrohStat()
        new_stat.st_mode = stat.S_IFREG | 0o644
        new_stat.st_nlink = 1
        return {
            "type": type,
            "stat": new_stat.to_dict(),
            "uuid": str(uuid.uuid4())
        }

    def create(self, path, flags, mode):
        self.logger.info("create: " + path)

        _, node_exists = self._walk(path)
        if node_exists:
            return -errno.EEXIST

        parent_path = os.path.dirname(path)
        name = os.path.basename(path)
        _, parent_node = self._walk(parent_path)

        new_file = self._new_node('file', name)

        try:
            key, node = self._persist(parent_node, name, new_file)
            self._commit(node)
            self._refresh_if_stale(node)
            real_path = self._real_path(node)
            file = os.fdopen(os.open(real_path, flags))
            fh = IrohFileHandle(key, node, file)
            self._on_change()
            return fh
        except Exception as e:
            print(traceback.format_exc())
            return -errno.EIO

    def _real_path(self, node):
        data_file = self._data_key(node.get('uuid'))
        return os.path.join(self.state_dir, data_file)

    def open(self, path, flags):
        self.logger.info("open: " + path)
        key, node = self._walk(path)
        if not node:
            self.logger.info("open: " + path + ": no such file or directory")
            return -errno.ENOENT
        real_path = self._real_path(node)
        self._refresh_if_stale(node)
        file = os.fdopen(os.open(real_path, flags))
        fh = IrohFileHandle(key, node, file)
        return fh

    def _refresh(self, node):
        real_path = self._real_path(node)

        # @TODO: This is where we should check that the existing file matches the type of the node.stat
        try:
            os.mknod(real_path, mode=0o600 | stat.S_IFREG)
        except FileExistsError:
            pass

        if node.get('stat').get('st_size') == 0:
            os.truncate(real_path, 0)
        else:
            data_key = self._data_key(node.get('uuid'))
            query = iroh.Query.key_exact(data_key.encode('utf-8'), None)
            data_entry = self.iroh_doc.get_one(query)
            self.iroh_doc.export_file(data_entry, real_path, None)

    def _refresh_if_stale(self, node):
        # @TODO: This should conditionally refresh the local file only if needed
        return self._refresh(node)

    def read(self, path, length, offset, fh):
        self.logger.info("read: " + path)
        self._refresh_if_stale(fh.node)
        return os.pread(fh.fd, length, offset)

    def ftruncate(self, path, length, fh):
        self.logger.info("ftruncate: " + path + " to" + str(length))
        self._refresh_if_stale(fh.node)
        fh.file.truncate(length)
        self._commit(fh.node)

    def truncate(self, path, length):
        self.logger.info("truncate: " + path + " to" + str(length))
        try:
            _, node = self._walk(path)
            self._refresh_if_stale(node)
            real_path = self._real_path(node)
            os.truncate(real_path, length)
        except Exception as e:
            return -errno.ENOENT

    def _update(self, node):
        stat_key = self._stat_key(node.get('uuid'))
        self.iroh_doc.set_bytes(self.iroh_author, stat_key.encode('utf-8'), dumps(node).encode('utf-8'))

    def _commit(self, node):
        data_key = self._data_key(node.get('uuid'))
        real_path = self._real_path(node)
        real_stat = os.stat(real_path)
        if real_stat.st_size == 0:
            self.iroh_doc.delete(self.iroh_author, data_key.encode('utf-8'))
        else:
            self.iroh_doc.import_file(self.iroh_author, data_key.encode('utf-8'), real_path, True, None)
        node.get('stat')['st_size'] = os.stat(real_path).st_size
        self._update(node)

    def write(self, path, buf, offset, fh):
        self.logger.info("write: " + path + " " + str(len(buf)) + "@" + str(offset))
        res = os.pwrite(fh.fd, buf, offset)
        self._commit(fh.node)
        return res


if __name__ == '__main__':
    usage = """
    Naive FUSE-on-Iroh Test
    """ + Fuse.fusage

    xdg_data_home = os.environ.get('XDG_DATA_HOME', os.path.expanduser('~/.local/share'))
    iroh_data_dir = os.environ.get('IROH_DATA_DIR', os.path.join(xdg_data_home, 'iroh'))

    xdg_state_home = os.environ.get('XDG_STATE_HOME', os.path.expanduser('~/.local/state'))
    irohfs_state_dir = os.environ.get('IROHFS_STATE_DIR', os.path.join(xdg_state_home, 'irohfs'))
    os.makedirs(os.path.join(irohfs_state_dir, 'data'), exist_ok=True)

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
        doc=doc, author=author, state_dir=irohfs_state_dir
    )

    server.parse(errex=1)
    server.main()
