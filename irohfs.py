import errno, stat
import os
import queue
import time
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
        self.state_dir = kwargs.pop('state_dir')

        self.iroh_node = None
        self.iroh_author = None
        self.iroh_doc = None

        self.root_key = None
        self.root_node = None

        self.last_refresh = 0
        self.last_resync = 0

        self.resync_interval = 3
        self.refresh_interval = 3

        super(IrohFS, self).__init__(*args, **kwargs)

        # Debug logging
        log_level = logging.DEBUG
        # log_level = logging.INFO

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

    def event(self, e):
        t = e.type()
        if t == iroh.LiveEventType.INSERT_LOCAL:
            entry = e.as_insert_local()
            self.logger.info(f"LiveEvent - InsertLocal: entry hash {entry.content_hash().to_string()}")
        elif t == iroh.LiveEventType.INSERT_REMOTE:
            insert_remove_event = e.as_insert_remote()
            self.logger.info(f"LiveEvent - InsertRemote:\n\tfrom: {insert_remove_event._from}\n\tentry hash:\n\t{insert_remove_event.entry.content_hash().to_string()}\n\tcontent_status: {insert_remove_event.content_status}")
        elif t == iroh.LiveEventType.CONTENT_READY:
            hash_val = e.as_content_ready()
            self.logger.info(f"LiveEvent - ContentReady: hash {hash_val.to_string()}")
        elif t == iroh.LiveEventType.NEIGHBOR_UP:
            node_id = e.as_neighbor_up()
            self.logger.info(f"LiveEvent - NeighborUp: node id {node_id.to_string()}")
        elif t == iroh.LiveEventType.NEIGHBOR_DOWN:
            node_id = e.as_neighbor_down()
            self.logger.info(f"LiveEvent - NeighborDown: node id {node_id.to_string()}")
        elif t == iroh.LiveEventType.SYNC_FINISHED:
            sync_event = e.as_sync_finished()
            self.logger.info(f"Live Event - SyncFinished: synced peer: {sync_event.peer.to_string()}")
        else:
            raise Exception("unknown LiveEventType")

    def main(self, *args, **kwargs):
        self.iroh_node = kwargs.pop('iroh_node')
        self.iroh_author = kwargs.pop('author')
        self.iroh_doc = kwargs.pop('doc')

        self.iroh_doc.subscribe(self)

        self.root_key, self.root_node = self._load_root()
        self._resync()

        self.logger.info("entered: Fuse.main()")
        return Fuse.main(self, *args, **kwargs)

    def _on_change(self):
        self.logger.info("event: on_change")

    def fgetattr(self, path, fh):
        self.logger.info("fgetattr: " + path)
        self._resync_if_stale()
        # if not node:
        #     try:
        #         self.logger.warning("fgetattr: called without fh!")
        #         _, node = self._walk(path)
        #     except Exception as e:
        #         return -errno.ENOENT
        return IrohStat(fh.node.get('stat'))


    def getattr(self, path):
        self.logger.info("getattr: " + path)
        self._resync_if_stale()
        try:
            _, node = self._walk(path)
            st = IrohStat(node.get('stat'))
            return st
        except Exception as e:
            self.logger.info("getattr: " + path + ": no such file or directory")
            return -errno.ENOENT

    def readdir(self, path, offset):
        self.logger.info("readdir: " + path)
        self._resync_if_stale()
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
            real_path = self._real_path(node)
            os.mknod(real_path, mode=0o600 | stat.S_IFREG)
            self._commit(node)
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

    def _resync_if_stale(self):
        current_time = time.monotonic()
        if current_time > self.last_resync + self.resync_interval:
            self._resync()

    def _resync(self):
        conns = iroh_node.connections()
        node_addrs = []
        self.logger.debug("open connections: ")
        for conn in conns:
            addrs = []
            for addr in conn.addrs:
                addrs.append(addr.addr())
            node_addrs.append(iroh.NodeAddr(node_id=conn.node_id, relay_url=conn.relay_url, addresses=addrs))
            self.logger.debug("     " + conn.node_id.fmt_short())
        self.iroh_doc.start_sync(node_addrs)
        self.last_resync = time.monotonic()

    def _refresh(self, node):
        real_path = self._real_path(node)

        self._resync_if_stale()

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
            self.logger.info("export " + str(data_key) + " to " + real_path + " size=" + str(
                node.get('stat').get('st_size')))
            self.iroh_doc.export_file(data_entry, real_path, None)
        self.last_refresh = time.monotonic()

    def _refresh_if_stale(self, node):
        # @TODO: This should conditionally refresh the local file only if needed
        # RIght now I'm thinking we should compare mtime of the "real" file and the mtime of the iroh stat entry
        # current_time = time.monotonic()
        # if current_time > self.last_refresh + self.refresh_interval:
        #     return self._refresh(node)
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

    def _persist(self, parent, name, node):
        stat_key = self._stat_key(node.get('uuid'))
        entry_key = self._entry_key(parent.get('uuid'), name, node.get('uuid'))
        self.logger.info("dir entry: " + entry_key + " stat: " + stat_key)
        self.iroh_doc.set_bytes(self.iroh_author, stat_key.encode('utf-8'), dumps(node).encode('utf-8'))
        self.iroh_doc.set_bytes(self.iroh_author, entry_key.encode('utf-8'), b'\x00')
        return stat_key, node

    def _update(self, node):
        stat_key = self._stat_key(node.get('uuid'))
        self.iroh_doc.set_bytes(self.iroh_author, stat_key.encode('utf-8'), dumps(node).encode('utf-8'))

    def _commit(self, node):
        data_key = self._data_key(node.get('uuid'))
        real_path = self._real_path(node)
        real_stat = os.stat(real_path)
        data_entry = self.iroh_doc.get_one(iroh.Query.key_exact(data_key.encode('utf-8'), None))
        real_size = real_stat.st_size
        if data_entry and real_size == 0:
            self.iroh_doc._del(self.iroh_author, data_key.encode('utf-8'))
        else:
            self.iroh_doc.import_file(self.iroh_author, data_key.encode('utf-8'), real_path, True, None)
        node['stat']['st_size'] = os.stat(real_path).st_size
        self._on_change()
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

    server = IrohFS(
        version="%prog " + fuse.__version__,
        usage=usage,
        dash_s_do='setsingle',
        state_dir=irohfs_state_dir
    )

    server.parser.add_option('--author', dest='author_id', action="store", type="string",
                             help='Set Iroh Author ID', default='')
    server.parser.add_option('--create', dest='create', action='store_true',
                             help='Create Iroh Doc if it does not exist', default=False)
    server.parser.add_option('--doc', dest='doc_id', action="store", type="string",
                             help='Specify Doc ID to open', default='')
    server.parser.add_option('--share', dest='share', action='store_true',
                             help='Print shareable ticket', default=False)
    server.parser.add_option('--join', dest='ticket_id', action="store", type="string",
                             help='Join Iroh Doc from shareable ticket', default='')

    server.parse(errex=1)
    options = server.cmdline[0]

    author_id = options.author_id
    create = options.create
    doc_id = options.doc_id
    share = options.share
    ticket_id = options.ticket_id

    os.makedirs(os.path.join(irohfs_state_dir, 'data'), exist_ok=True)

    iroh_node = iroh.IrohNode(iroh_data_dir)
    print("Started Iroh node: {}".format(iroh_node.node_id()))

    if author_id:
        author = iroh.AuthorId.from_string(author_id)
    else:
        authors = iroh_node.author_list()
        author = authors[0]
    print("Assumed author id: {}".format(author.to_string()))

    if ticket_id:
        doc = iroh_node.doc_join(ticket_id)
        print("Joined doc: {}".format(doc.id()))
    else:
        if not doc_id and create:
            doc = iroh_node.doc_create()
            print("Created doc: {}".format(doc.id()))
        elif doc_id:
            doc = iroh_node.doc_open(doc_id)
            print("Opened doc: {}".format(doc.id()))
        else:
            raise Exception("No Doc ID specified. Did you mean to create one with --create?")

    if share:
        shareable_ticket_id = doc.share(iroh.ShareMode.WRITE)
        print("You can share write access to the document with the following ticket:")
        print("  " + shareable_ticket_id)

    dl_pol = doc.get_download_policy()
    doc.set_download_policy(dl_pol.everything())

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

    server.main(doc=doc, author=author, iroh_node=iroh_node)
