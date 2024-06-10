import errno
import logging
import os
import queue
import stat
import time
import traceback
import uuid
from json import dumps, loads

import fuse
import iroh
from fuse import Fuse

import pydevd_pycharm

fuse.fuse_python_api = (0, 2)


def _flag2mode(flags):
    md = {os.O_RDONLY: 'rb', os.O_WRONLY: 'wb', os.O_RDWR: 'wb+'}
    m = md[flags & (os.O_RDONLY | os.O_WRONLY | os.O_RDWR)]

    if flags | os.O_APPEND:
        m = m.replace('w', 'a', 1)

    return m


class HoloFSStat(fuse.Stat):
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


class HoloFS(Fuse):
    def __init__(self, *args, **kwargs):
        self.state_dir = kwargs.pop('state_dir')

        self.iroh_node = None
        self.iroh_author = None
        self.iroh_doc = None

        self.root_direntry = None
        self.root_node = None

        self.last_resync = 0

        self.resync_interval = 3
        self.refresh_interval = 3

        super(HoloFS, self).__init__(*args, **kwargs)

        # Debug logging
        log_level = logging.DEBUG
        # log_level = logging.INFO
        # log_level = logging.WARNING
        self.logger = self._setup_logging(log_level)

        self.queue = queue.Queue()

    def _setup_logging(self, log_level):
        logger = logging.getLogger('HoloFS')
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
            self.logger.info(
                f"LiveEvent - InsertRemote:\n\tfrom: {insert_remove_event._from}\n\tentry hash:\n\t{insert_remove_event.entry.content_hash().to_string()}\n\tcontent_status: {insert_remove_event.content_status}")
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
            self.logger.info(f"LiveEvent - SyncFinished: synced peer: {sync_event.peer.to_string()}")
        elif t == iroh.LiveEventType.PENDING_CONTENT_READY:
            self.logger.info("LiveEvent - Pending content ready!")
        else:
            self.logger.error(str(t))
            raise Exception("unknown LiveEventType")

    def iroh_init(self, iroh_node, author, doc):
        self.iroh_node = iroh_node
        self.iroh_author = author
        self.iroh_doc = doc

        self.iroh_doc.subscribe(self)

        self._resync()

    def makefs(self):
        print("Initializing filesystem...")
        root_dir = HoloFS.Dir.mkdir(self, 0o755)
        root_dir.persist()
        doc.set_bytes(author, b'root_uuid', root_dir.uuid.encode('utf-8'))

    def main(self, *args, **kwargs):
        print("Loading the filesystem...")
        retries = 0
        max_retries = 3

        while retries < max_retries:
            try:
                self.root_node = self._load_root()
                if self.root_node:
                    self.root_direntry = HoloFS.RootDirEntry(self)
                    break
            except Exception as e:
                print(traceback.format_exc())
                exit(1)
            self._resync()
            print("Trying %s more times to load the filesystem" % (max_retries - retries))
            time.sleep(3)
            retries += 1
        else:
            raise Exception("failed to load the filesystem")
        print("Connected to filesystem!")

        self.logger.debug("entered: Fuse.main()")
        return Fuse.main(self, *args, **kwargs)

    def _latest_prefix_many(self, prefix):
        self.logger.debug("query latest entries matching prefix: %s" % prefix)
        query = iroh.Query.single_latest_per_key_prefix(prefix.encode('utf-8'), None)
        return self.iroh_doc.get_many(query)

    def _latest_prefix_one(self, prefix):
        self.logger.debug("query latest one entry matching prefix: %s" % prefix)
        query = iroh.Query.single_latest_per_key_prefix(prefix.encode('utf-8'), None)
        return self.iroh_doc.get_one(query)

    def _latest_key_one(self, key):
        self.logger.debug("query latest entry for key: %s" % key)
        query = iroh.Query.single_latest_per_key_exact(key.encode('utf-8'))
        return self.iroh_doc.get_one(query)

    def release(self, path, flags, fh):
        self.logger.info(f"release: {path}")
        fh.release(flags)

    def fsync(self, path, isfsyncfile, fh):
        self.logger.info(f"fsync: {path}")
        fh.release()

    def flush(self, path, fh):
        self.logger.info(f"flush: {path}")
        fh.flush()

    def getattr(self, path):
        self.logger.debug("getattr: " + path)
        self._resync_if_stale()
        try:
            direntry = self.root_direntry.walk(path)
            if not direntry:
                self.logger.debug("getattr: " + path + ": no such file or directory")
                return -errno.ENOENT
            return direntry.node().stat
        except Exception as e:
            print(traceback.format_exc())
            self.logger.debug("getattr: " + path + ": no such file or directory")
            return -errno.ENOENT

    def readdir(self, path, offset):
        self.logger.debug("readdir: " + path)
        self._resync_if_stale()
        try:
            dir_node = self.root_direntry.walk(path).node()
            children = dir_node.children()
            entries = [
                fuse.Direntry('.'),
                fuse.Direntry('..')
            ]
            for child in children:
                self.logger.debug(f"readdir: {child.name}")
                entries.append(child.to_fuse_direntry())
            return entries
        except Exception as e:
            print(traceback.format_exc())
            return -errno.EIO

    def _load_root(self):
        key = 'root_uuid'
        self.logger.debug("load: " + key)
        root_uuid = self._latest_contents(key).decode('utf-8')
        root_node = HoloFS.FSNode.load(self, root_uuid)
        if not isinstance(root_node, HoloFS.Dir):
            raise Exception("Root node must be a directory!")
        return root_node

    def _latest_contents(self, key):
        return self._latest_key_one(key).content_bytes(self.iroh_doc)

    def _set_key(self, key, contents):
        return self.iroh_doc.set_bytes(self.iroh_author, key.encode('utf-8'), contents)

    def rename(self, path, path1):
        self.logger.info('rename: ' + path + ' -> ' + path1)
        # pydevd_pycharm.settrace('localhost', port=30303, stdoutToServer=True, stderrToServer=True)

        from_direntry = self.root_direntry.walk(path)
        if not from_direntry:
            return -errno.ENOENT
        from_node = from_direntry.node()

        to_parent_path = os.path.dirname(path1)
        to_parent_direntry = self.root_direntry.walk(to_parent_path)
        if not to_parent_direntry:
            return -errno.ENOENT
        to_parent_node = to_parent_direntry.node()

        to_name = os.path.basename(path1)
        to_direntry = to_parent_direntry.node().child(to_name)
        if to_direntry:
            to_node = to_direntry.node()
            if isinstance(to_node, HoloFS.File) and isinstance(from_node, HoloFS.Dir):
                return -errno.ENOTDIR
            elif isinstance(to_node, HoloFS.Dir) and isinstance(from_node, HoloFS.File):
                return -errno.EISDIR
        try:
            if to_direntry:
                to_direntry.unlink()
            to_direntry = to_parent_node.add_child(to_name, from_direntry.node().uuid)
            to_direntry.persist()
            from_direntry.unlink()
        except Exception as e:
            print(traceback.format_exc())
            return -errno.EIO

    def mkdir(self, path, mode):
        self.logger.info("mkdir: " + path)
        # pydevd_pycharm.settrace('localhost', port=30303, stdoutToServer=True, stderrToServer=True)

        parent_path = os.path.dirname(path)
        parent_dir = self.root_direntry.walk(parent_path)
        if not parent_dir:
            return -errno.ENOENT
        parent_node = parent_dir.node()
        name = os.path.basename(path)
        try:
            new_dir = HoloFS.Dir.mkdir(self, mode)
            new_direntry = parent_node.add_child(name, new_dir.uuid)
            new_direntry.persist()
        except Exception as e:
            print(traceback.format_exc())
            return -errno.EIO

    def utime(self, path, times):
        self.logger.warning("utime: unimplemented")

    def create(self, path, flags, mode):
        self.logger.info("create: " + path)

        parent_path = os.path.dirname(path)
        name = os.path.basename(path)
        parent_direntry = self.root_direntry.walk(parent_path)
        if not parent_direntry:
            return -errno.ENOENT
        parent_dir = parent_direntry.node()

        direntry_exists = parent_dir.child(name)
        if direntry_exists:
            self.logger.debug(f"Failed creating {path} with {flags}: file exists")
            return -errno.EEXIST

        try:
            new_file = HoloFS.File.mknod(self, mode, 0)
            new_direntry = parent_dir.add_child(name, new_file.uuid)
            new_direntry.persist()
            return new_file.open(flags)
        except Exception as e:
            print(traceback.format_exc())
            return -errno.EIO

    def open(self, path, flags):
        self.logger.info("open: " + path)
        direntry = self.root_direntry.walk(path)
        if not direntry:
            return -errno.ENOENT

        return direntry.node().open(flags)

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

    def unlink(self, path):
        self.logger.info("unlink: " + path)
        direntry = self.root_direntry.walk(path)
        if not direntry:
            return -errno.ENOENT
        try:
            direntry.unlink()
        except Exception as e:
            print(traceback.format_exc())
            return -errno.EIO

    def read(self, path, length, offset, fh):
        try:
            return fh.read(length, offset)
        except Exception as e:
            print(traceback.format_exc())
            return -errno.EIO

    def ftruncate(self, path, length, fh):
        self.logger.info("ftruncate: " + path + " to " + str(length))
        try:
            fh.ftruncate(length)
        except Exception as e:
            print(traceback.format_exc())
            return -errno.EIO

    def truncate(self, path, length):
        self.logger.info("truncate: " + path + " to" + str(length))
        try:
            dir_entry = self.root_direntry.walk(path)
            dir_entry.node().truncate(length)
        except Exception as e:
            print(traceback.format_exc())
            return -errno.ENOENT

    def write(self, path, buf, offset, fh):
        self.logger.info("write: " + path + " " + str(len(buf)) + "@" + str(offset))
        try:
            return fh.write(buf, offset)
        except Exception as e:
            print(traceback.format_exc())
            return -errno.EIO

    class WalkableDirEntry(object):
        def walk(self, path):
            if type(path) == str:
                path = list(filter(None, path.removeprefix('/').split('/')))
            self._fs.logger.debug(f"walk: {path}")
            if len(path) == 0:
                return self
            else:
                first_el = path.pop(0)
                next_direntry = self.node().child(first_el)
                if not next_direntry:
                    return None
                return next_direntry.walk(path)

    class RootDirEntry(WalkableDirEntry):
        def __init__(self, fs):
            self._fs = fs
            if not isinstance(fs, HoloFS):
                raise Exception("fs must be a fully initialized HoloFS")

        def node(self):
            return self._fs.root_node

    class DirEntry(WalkableDirEntry):
        def __init__(self, fs, key):
            self._fs = fs
            if not isinstance(fs, HoloFS):
                raise Exception("fs must be a fully initialized HoloFS")
            self.key = key
            _, self.parent_uuid, self.name, self.node_uuid = self.key.split('/')

        def persist(self):
            self._fs._set_key(self.key, b'\x00')

        def node(self):
            return HoloFS.FSNode.load(self._fs, self.node_uuid)

        def to_fuse_direntry(self):
            return fuse.Direntry(self.name)

        def unlink(self):
            self._fs.iroh_doc._del(self._fs.iroh_author, self.key.encode('utf-8'))

    class FSNode(object):
        def __init__(self, fs, node_stat, node_uuid=None):
            self._fs = fs
            if not node_uuid:
                node_uuid = str(uuid.uuid4())
            self.uuid = node_uuid
            self.key = self.node_key(node_uuid)
            self.stat = node_stat

        @classmethod
        def node_key(cls, node_uuid):
            return f"stat/{node_uuid}.json"

        @classmethod
        def make(cls, fs, node_stat):
            if stat.S_ISDIR(node_stat.st_mode):
                new_node = HoloFS.Dir(fs, node_stat)
            elif stat.S_ISREG(node_stat.st_mode):
                new_node = HoloFS.File(fs, node_stat)
            else:
                node_type = stat.S_IFMT(node_stat.st_mode)
                raise Exception(f"Unknown node type: {node_type}")

            try:
                new_node.persist()
                return new_node
            except Exception as e:
                print(traceback.format_exc())
                return -errno.EIO

        @classmethod
        def load(cls, fs, node_uuid):
            node_key = cls.node_key(node_uuid)
            # @TODO: This is where we'll handle policies that tell us which version of the node entry to load
            contents = loads(fs._latest_contents(node_key))
            node_stat = HoloFSStat(contents.get('stat'))

            if stat.S_ISDIR(node_stat.st_mode):
                return HoloFS.Dir(fs, node_stat, node_uuid)
            elif stat.S_ISREG(node_stat.st_mode):
                return HoloFS.File(fs, node_stat, node_uuid)
            else:
                node_type = stat.S_IFMT(node_stat.st_mode)
                raise Exception(f"Unknown node type: {node_type}")

        def persist(self):
            to_save = {
                'stat': self.stat.to_dict()
            }
            self._fs._set_key(self.key, dumps(to_save).encode('utf-8'))

    class File(FSNode):
        def __init__(self, fs, node_stat, node_uuid=None):
            super().__init__(fs, node_stat, node_uuid)
            self._data_key = f"data/{self.uuid}"
            self._real_path = os.path.join(self._fs.state_dir, self._data_key)
            self._data_entry = None

        def _refresh_if_stale(self):
            should_refresh = True
            try:
                real_stat = os.stat(self._real_path)
                if real_stat.st_mtime >= self.stat.st_mtime:
                    should_refresh = False
            except Exception as e:
                pass
            if should_refresh:
                return self._refresh()

        def _refresh(self):
            self._fs._resync_if_stale()
            try:
                os.mknod(self._real_path, mode=0o600 | stat.S_IFREG)
            except FileExistsError:
                pass

            if self.stat.st_size == 0:
                os.truncate(self._real_path, 0)
            else:
                data_entry = self._fs._latest_key_one(self._data_key)
                self._fs.iroh_doc.export_file(data_entry, self._real_path, None)
            os.utime(self._real_path, (self.stat.st_atime, self.stat.st_mtime))

        def _commit(self):
            real_stat = os.stat(self._real_path)
            real_size = real_stat.st_size
            if self._data_entry and real_size == 0:
                self._fs.iroh_doc._del(self._fs.iroh_author, self._data_key.encode('utf-8'))
            else:
                self._fs.iroh_doc.import_file(self._fs.iroh_author, self._data_key.encode('utf-8'), self._real_path,
                                              False, None)

            self.stat.st_size = real_stat.st_size
            self.stat.st_atime = real_stat.st_atime
            self.stat.st_mtime = real_stat.st_mtime
            self.stat.st_ctime = real_stat.st_ctime
            self.persist()

        @classmethod
        def mknod(cls, fs, mode, dev):
            node_stat = HoloFSStat({
                'st_mode': mode,
                'st_dev': dev,
                'st_nlink': 1,
                'st_uid': os.getuid(),
                'st_gid': os.getgid(),
                'st_size': 0
            })
            return cls.make(fs, node_stat)

        def open(self, flags):
            self._refresh_if_stale()
            return HoloFS.FileHandle(self, flags)

        def truncate(self, length):
            if length > 0:
                self._refresh_if_stale()
            os.truncate(self._real_path, length)
            self._commit()

        def getattr(self):
            return self.stat

    class Dir(FSNode):
        def __init__(self, fs, node_stat, node_uuid=None):
            super().__init__(fs, node_stat, node_uuid)

        def _child_prefix(self):
            return f"fs/{self.uuid}/"

        def _child_search_key(self, name):
            return f"fs/{self.uuid}/{name}/"

        def _child_direntry_key(self, name, node_uuid):
            return f"fs/{self.uuid}/{name}/{node_uuid}"

        @classmethod
        def mkdir(cls, fs, mode):
            dir_stat = HoloFSStat({
                'st_mode': stat.S_IFDIR | mode ,
                'st_dev': 0,
                'st_nlink': 2,
                'st_uid': os.getuid(),
                'st_gid': os.getgid(),
                'st_size': 0
            })
            return cls.make(fs, dir_stat)

        def add_child(self, name: str, node_uuid: str):
            return HoloFS.DirEntry(self._fs, self._child_direntry_key(name, node_uuid))

        def child(self, name):
            entry = self._fs._latest_prefix_one(self._child_search_key(name))
            if not entry:
                return None
            return HoloFS.DirEntry(self._fs, entry.key().decode('utf-8'))

        def children(self):
            entries = self._fs._latest_prefix_many(self._child_prefix())
            dir_entries = []
            for entry in entries:
                dir_entries.append(HoloFS.DirEntry(self._fs, entry.key().decode('utf-8')))
            return dir_entries

        def walk(self, path):
            raise Exception("Old and busted")
            if type(path) == str:
                path = path.removeprefix('/').split('/')
                path = os.path.split(path)
            first_el = path.pop(0)
            if not first_el:
                return self._fs.root_direntry
            child_direntry = self.child(first_el)
            if len(path) == 0:
                return child_direntry
            else:
                child_fsnode = child_direntry.node()
                if not isinstance(child_fsnode, HoloFS.Dir):
                    raise Exception("Not a directory")
                return child_direntry.node().walk(path)

    class FileHandle(object):
        def __init__(self, fsnode, flags):
            self.node = fsnode
            self.file = os.fdopen(os.open(fsnode._real_path, flags))
            self.fd = self.file.fileno()

        def read(self, length, offset):
            self.node._refresh_if_stale()
            return os.pread(self.fd, length, offset)

        def ftruncate(self, length):
            self.node._refresh_if_stale()
            os.truncate(self.node._real_path, length)
            self.node._commit()

        def write(self, buf, offset):
            return os.pwrite(self.fd, buf, offset)

        def release(self, flags):
            self.file.close()
            self.node._commit()

        def fsync(self, issyncfile):
            os.fsync(self.fd)
            self.node._commit()

        def flush(self):
            os.fsync(self.fd)
            self.node._commit()

        def fgetattr(self):
            return self.node.stat


if __name__ == '__main__':
    usage = """
    HoloFS
    """ + Fuse.fusage

    xdg_data_home = os.environ.get('XDG_DATA_HOME', os.path.expanduser('~/.local/share'))
    iroh_data_dir = os.environ.get('IROH_DATA_DIR', os.path.join(xdg_data_home, 'iroh'))

    xdg_state_home = os.environ.get('XDG_STATE_HOME', os.path.expanduser('~/.local/state'))
    irohfs_state_dir = os.environ.get('HOLOFS_STATE_DIR', os.path.join(xdg_state_home, 'holofs'))

    server = HoloFS(
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
        if len(authors) > 0:
            author = authors[0]
        else:
            print("Creating new author ...")
            author = iroh_node.author_create()

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
        shareable_ticket_id = doc.share(iroh.ShareMode.WRITE, iroh.AddrInfoOptions.RELAY_AND_ADDRESSES)
        print("You can share write access to the document with the following ticket: " + shareable_ticket_id)

    dl_pol = doc.get_download_policy()
    doc.set_download_policy(dl_pol.everything())
    # dl_filter = iroh.FilterKind.prefix(b'data/')
    # doc.set_download_policy(dl_pol.everything_except([dl_filter]))

    server.iroh_init(iroh_node, author, doc)
    if create:
        server.makefs()
    server.main()