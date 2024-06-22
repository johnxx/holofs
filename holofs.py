import errno
import logging
import os
import pycrdt
import queue
import stat
import time
import traceback
import uuid
from json import dumps, loads

import fuse
import iroh
from fuse import Fuse

# import pydevd_pycharm

fuse.fuse_python_api = (0, 2)

def flag2mode(flags):
    access_mode = flags & os.O_ACCMODE
    md = {os.O_RDONLY: 'rb', os.O_WRONLY: 'wb', os.O_RDWR: 'wb+'}
    m = md[access_mode & (os.O_RDONLY | os.O_WRONLY | os.O_RDWR)]

    if access_mode | os.O_APPEND:
        m = m.replace('w', 'a', 1)

    return m

class HoloFS(Fuse):
    def __init__(self, *args, **kwargs):
        self.state_dir = kwargs.pop('state_dir')

        self.iroh_node = None
        self.iroh_author = None
        self.iroh_doc = None

        self.crdt_doc = None

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
            self.resync_if_stale()
            key = insert_remove_event.entry.key().decode('utf-8')
            content_hash = insert_remove_event.entry.content_hash().to_string()
            self.logger.info(
                f"LiveEvent - InsertRemote:\n\tfrom: {insert_remove_event._from}\n\tentry_key: {key}\n\tentry hash: {content_hash}\n\tcontent_status: {insert_remove_event.content_status}")
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

        self.crdt_doc = pycrdt.Doc()

        self.iroh_doc.subscribe(self)

        self.resync()

    def makefs(self):
        print("Initializing filesystem...")
        self.crdt_doc['fs'] = pycrdt.Map()
        root_dir = HoloFS.Dir.mkdir(self, 0o755)
        root_dir.persist()
        self.crdt_doc['fs']['root_uuid'] = root_dir.uuid.encode('utf-8')
        self.iroh_doc.set_bytes(author, b'updates', self.crdt_doc.get_update())

    def on_change(self):
        self._sync_fs()
        self.resync()

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
                print("Trying %s more times to load the filesystem" % (max_retries - retries))
            self.resync()
            time.sleep(3)
            retries += 1
        else:
            raise Exception("failed to load the filesystem")
        print("Connected to filesystem!")

        self.logger.debug("entered: Fuse.main()")
        return Fuse.main(self, *args, **kwargs)

    # def _load_fs_crdt(self):
    #     key = 'updates'
    #     self.logger.debug("iroh load: " + key)
    #     entries = self.iroh_key_all_authors(key)
    #     self.crdt_doc = pycrdt.Doc()
    #     for entry in entries:
    #         self.logger.debug("Applying update.")
    #         update = entry.content_bytes(self.iroh_doc)
    #         self.crdt_doc.apply_update(update)
    #     return self.crdt_doc['fs']

    def _load_root(self):
        key = 'root_uuid'
        self.logger.debug("load: " + key)
        root_uuid = self.latest_contents(key).decode('utf-8')
        root_node = HoloFS.FSNode.load(self, root_uuid)
        if not isinstance(root_node, HoloFS.Dir):
            raise Exception("Root node must be a directory!")
        return root_node

    def iroh_latest_contents(self, key):
        return self.iroh_latest_key_one(key).content_bytes(self.iroh_doc)

    def latest_contents(self, key):
        return self.crdt_doc['fs'][key]

    def set_key(self, key, contents):
        content_length = len(contents)
        self.logger.debug(f"set key: {key} (length: {content_length})")
        self.crdt_doc['fs'][key] = contents

    def iroh_set_key(self, key, contents):
        content_length = len(contents)
        self.logger.debug(f"set key: {key} (length: {content_length})")
        return self.iroh_doc.set_bytes(self.iroh_author, key.encode('utf-8'), contents)

    def latest_prefix_many(self, prefix):
        self.logger.debug("query latest entries matching prefix: %s" % prefix)
        self.logger.debug(f"keys:")
        for k in self.crdt_doc['fs'].keys():
            self.logger.debug(f"       {k}")
        return {k: v for k, v in self.crdt_doc['fs'].items() if k.startswith(prefix)}

    def iroh_latest_prefix_many(self, prefix):
        self.logger.debug("query latest entries matching prefix: %s" % prefix)
        query = iroh.Query.single_latest_per_key_prefix(prefix.encode('utf-8'), None)
        return self.iroh_doc.get_many(query)

    def latest_prefix_one(self, prefix):
        for k, v in self.crdt_doc['fs'].items():
            if k.startswith(prefix):
                return k, v
        return None, None

    def iroh_latest_prefix_one(self, prefix):
        self.logger.debug("query latest one entry matching prefix: %s" % prefix)
        query = iroh.Query.single_latest_per_key_prefix(prefix.encode('utf-8'), None)
        return self.iroh_doc.get_one(query)

    def latest_key_one(self, key):
        v = self.crdt_doc['fs'].get(key, None)
        if not v:
            return None, None
        return key, v

    def iroh_latest_key_one(self, key):
        self.logger.debug("query latest entry for key: %s" % key)
        query = iroh.Query.single_latest_per_key_exact(key.encode('utf-8'))
        return self.iroh_doc.get_one(query)

    def iroh_key_all_authors(self, key):
        self.logger.debug(f"query latest entry for all authors for key: {key}")
        query = iroh.Query.key_exact(key.encode('utf-8'), None)
        return self.iroh_doc.get_many(query)

    def release(self, path, flags, fh):
        # @TODO: Guess this isn't quite right?
        self.logger.info(f"release: {path}")
        try:
            fh.release()
            self.logger.info(f"closed: {path}")
        except Exception as e:
            print(traceback.format_exc())
            return -errno.EIO
        return 0

    def _sync_fs(self):
        self.logger.info("syncing...")
        self.iroh_set_key('updates', self.crdt_doc.get_update())
        self.logger.info("synced!")

    def fsync(self, path, isfsyncfile, fh):
        self.logger.info(f"fsync: {path}")
        fh.flush()

    def flush(self, path, fh):
        self.logger.info(f"flush: {path}")
        fh.flush()

    def getattr(self, path):
        self.logger.debug("getattr: " + path)
        self.resync_if_stale()
        try:
            direntry = self.root_direntry.walk(path)
            if not direntry:
                self.logger.debug("getattr: " + path + ": no such file or directory")
                return -errno.ENOENT
            return direntry.node().stat
        except Exception as e:
            print(traceback.format_exc())
            self.logger.warning(f"getattr: exception loading node")
            self.logger.debug("getattr: " + path + ": no such file or directory")
            return -errno.ENOENT

    def rmdir(self, path):
        self.logger.debug(f"rmdir: {path}")
        self.resync_if_stale()
        direntry = self.root_direntry.walk(path)
        if not direntry:
            return -errno.ENOENT
        try:
            direntry.unlink()
        except Exception as e:
            print(traceback.format_exc())
            return -errno.EIO

    def readdir(self, path, offset):
        self.logger.debug("readdir: " + path)
        self.resync_if_stale()
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

    def readlink(self, symlink_path):
        self.logger.info(f"readlink: {symlink_path}")

        symlink_direntry = self.root_direntry.walk(symlink_path)
        if not symlink_direntry:
            return -errno.ENOENT
        symlink = symlink_direntry.node()

        try:
            return symlink.readlink()
        except Exception as e:
            print(traceback.format_exc())
            return -errno.EIO

    def symlink(self, target, symlink_path):
        self.logger.info(f"symlink: {target} -> {symlink_path}")

        symlink_parent_path = os.path.dirname(symlink_path)
        symlink_parent_direntry = self.root_direntry.walk(symlink_parent_path)
        if not symlink_parent_direntry:
            return -errno.ENOENT
        symlink_parent_node = symlink_parent_direntry.node()

        symlink_name = os.path.basename(symlink_path)

        try:
            symlink = HoloFS.SymLink.symlink(self, target)
            symlink_direntry = symlink_parent_node.add_child(symlink_name, symlink.uuid)
            symlink_direntry.persist()
        except Exception as e:
            print(traceback.format_exc())
            return -errno.EIO

    def link(self, from_path, to_path):
        self.logger.info(f"link: {from_path} -> {to_path}")

        from_direntry = self.root_direntry.walk(from_path)
        if not from_direntry:
            return -errno.ENOENT
        from_node = from_direntry.node()

        to_parent_path = os.path.dirname(to_path)
        to_parent_direntry = self.root_direntry.walk(to_parent_path)
        if not to_parent_direntry:
            return -errno.ENOENT
        to_parent_node = to_parent_direntry.node()

        to_name = os.path.basename(to_path)
        to_direntry = to_parent_direntry.node().child(to_name)
        if to_direntry:
            return -errno.EEXIST

        try:
            to_direntry = to_parent_node.add_child(to_name, from_node.uuid)
            to_direntry.persist()
        except Exception as e:
            print(traceback.format_exc())
            return -errno.EIO

    def rename(self, path, path1):
        self.logger.info('rename: ' + path + ' -> ' + path1)

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
            to_direntry = to_parent_node.add_child(to_name, from_node.uuid)
            to_direntry.persist()
            from_direntry.unlink()
        except Exception as e:
            print(traceback.format_exc())
            return -errno.EIO

    def mkdir(self, path, mode):
        self.logger.info("mkdir: " + path)

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

    def chmod(self, path, mode):
        self.logger.info(f"chmod: {path} to {mode}")
        direntry = self.root_direntry.walk(path)
        if not direntry:
            return -errno.ENOENT
        try:
            direntry.node().chmod(mode)
        except Exception as e:
            print(traceback.format_exc())
            return -errno.EIO

    def utime(self, path, times):
        self.logger.info(f"utime: {path} to {times}")
        direntry = self.root_direntry.walk(path)
        if not direntry:
            return -errno.ENOENT
        try:
            direntry.node().utime(times)
        except Exception as e:
            print(traceback.format_exc())
            return -errno.EIO

    def create(self, path, flags, mode):
        self.logger.info(f"create: {path} (flags: {flags}, mode: {mode})")

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
            flags = flags & ~os.O_CREAT
            flags = flags & ~os.O_EXCL
            new_direntry = parent_dir.add_child(name, new_file.uuid)
            new_direntry.persist()
            return new_file.open(flags, mode)
        except Exception as e:
            print(traceback.format_exc())
            return -errno.EIO

    def open(self, path, flags):
        self.logger.info(f"open: {path} (flags: {flags})")
        direntry = self.root_direntry.walk(path)
        if not direntry:
            return -errno.ENOENT

        return direntry.node().open(flags, 0)

    def resync_if_stale(self):
        current_time = time.monotonic()
        if current_time > self.last_resync + self.resync_interval:
            self.resync()

    def resync(self):
        # pydevd_pycharm.settrace('localhost', port=23234, stdoutToServer=True, stderrToServer=True, suspend=False)
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
        self.logger.debug("Applying updates:")
        for update_entry in self.iroh_key_all_authors('updates'):
            if update_entry:
                update = update_entry.content_bytes(self.iroh_doc)
                self.crdt_doc.apply_update(update)
                self.logger.debug(f"     update applied!")
        self.crdt_doc['fs'] = pycrdt.Map()
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
        self.logger.info("read: " + path)
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
            # print(traceback.format_exc())
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
            if type(path) is str:
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
            self._fs.set_key(self.key, b'\x00')
            self._fs.on_change()

        def node(self):
            return HoloFS.FSNode.load(self._fs, self.node_uuid)

        def to_fuse_direntry(self):
            return fuse.Direntry(self.name)

        def unlink(self):
            self._fs.iroh_doc._del(self._fs.iroh_author, self.key.encode('utf-8'))

    class Stat(fuse.Stat):
        def __init__(self, *initial_data, **kwargs):
            super().__init__()
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

    class FSNode(object):
        def __init__(self, fs, node_contents, node_uuid=None):
            self._fs = fs
            if not node_uuid:
                node_uuid = str(uuid.uuid4())
            self.uuid = node_uuid
            self.key = self.node_key(node_uuid)
            self.stat = node_contents['stat']

        @classmethod
        def node_key(cls, node_uuid):
            return f"stat/{node_uuid}.json"

        # @classmethod
        # def make(cls, fs, node_stat):
        #     if stat.S_ISDIR(node_stat.st_mode):
        #         new_node = HoloFS.Dir(fs, node_stat)
        #     elif stat.S_ISREG(node_stat.st_mode):
        #         new_node = HoloFS.File(fs, node_stat)
        #     elif stat.S_ISLNK(node_stat.st_mode):
        #         new_node = HoloFS.SymLink(fs, node_stat)
        #     else:
        #         node_type = stat.S_IFMT(node_stat.st_mode)
        #         raise Exception(f"Unknown node type: {node_type}")

        #     try:
        #         new_node.persist()
        #         return new_node
        #     except Exception as e:
        #         print(traceback.format_exc())
        #         return -errno.EIO

        @classmethod
        def load(cls, fs, node_uuid):
            node_key = cls.node_key(node_uuid)
            # @TODO: This is where we'll handle policies that tell us which version of the node entry to load
            contents = loads(fs.latest_contents(node_key))
            contents['stat'] = HoloFS.Stat(contents.get('stat'))

            if stat.S_ISDIR(contents['stat'].st_mode):
                return HoloFS.Dir(fs, contents, node_uuid)
            elif stat.S_ISREG(contents['stat'].st_mode):
                return HoloFS.File(fs, contents, node_uuid)
            elif stat.S_ISLNK(contents['stat'].st_mode):
                return HoloFS.SymLink(fs, contents, node_uuid)
            else:
                node_type = stat.S_IFMT(contents['stat'].st_mode)
                raise Exception(f"Unknown node type: {node_type}")

        def persist(self):
            to_save = {
                'stat': self.stat.to_dict()
            }
            self._fs.set_key(self.key, dumps(to_save).encode('utf-8'))
            self._fs.on_change()

        def chmod(self, mode):
            self.stat.st_mode = mode
            self.persist()

        def utime(self, times):
            self.stat.st_atime = times[0]
            self.stat.st_mtime = times[1]
            self.persist()

    class File(FSNode):
        def __init__(self, fs, node_contents, node_uuid=None):
            super().__init__(fs, node_contents, node_uuid)
            self._data_uuid = str(uuid.uuid4())
            self._data_entry = None

        @property
        def _data_key(self):
            return f"data/{self._data_uuid}"

        @property
        def _real_path(self):
            return os.path.join(self._fs.state_dir, f"data/{self.uuid}")

        def _refresh_if_stale(self):
            should_refresh = True
            try:
                contents = loads(self._fs.latest_contents(self.key))
                self.stat = HoloFS.Stat(contents.get('stat'))
                real_stat = os.stat(self._real_path)
                if real_stat.st_mtime >= self.stat.st_mtime:
                    should_refresh = False
            except Exception as e:
                pass
            if should_refresh:
                return self._refresh()

        def _refresh(self):
            self._fs.logger.debug(f"_refresh: {self.key} (data: {self._data_key})")
            self._fs.resync_if_stale()
            try:
                os.mknod(self._real_path, mode=0o600 | stat.S_IFREG)
            except FileExistsError:
                pass

            if self.stat.st_size == 0:
                os.truncate(self._real_path, 0)
            else:
                self._data_entry = self._fs.latest_key_one(self._data_key)
                self._fs.iroh_doc.export_file(self._data_entry, self._real_path, None)
            os.utime(self._real_path, (self.stat.st_atime, self.stat.st_mtime))

        def persist(self):
            to_save = {
                'data_uuid': self._data_uuid,
                'stat': self.stat.to_dict()
            }
            self._fs.set_key(self.key, dumps(to_save).encode('utf-8'))
            self._fs.on_change()

        def progress(self, add_progress: iroh.AddProgress):
            event_type = add_progress.type()
            if event_type == iroh.AddProgressType.FOUND:
                p = add_progress.as_found()
                self._fs.logger.debug(f"add blob found: id: {p.id} name: {p.name} size: {p.size}")
            elif event_type == iroh.AddProgressType.PROGRESS:
                p = add_progress.as_progress()
                self._fs.logger.debug(f"add blog progress: id: {p.id} offset: {p.offset}")
            elif event_type == iroh.AddProgressType.DONE:
                p = add_progress.as_done()
                self._fs.logger.debug(f"add blob done: id: {p.id} hash: {p.hash}")
            elif event_type == iroh.AddProgressType.ALL_DONE:
                p = add_progress.as_all_done()
                self._fs.logger.debug(f"add blob all done: hash: {p.hash} format: {p.format} tag: {p.tag}")
            elif event_type == iroh.AddProgressType.ABORT:
                p = add_progress.as_abort()
                self._fs.logger.debug(f"add blob: error: {p.error}")
            else:
                self._fs.logger.warning(f"Unknown event adding blob: {event_type}")

        def commit(self):
            real_stat = os.stat(self._real_path)
            real_size = real_stat.st_size
            self._data_uuid = str(uuid.uuid4())
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
            node_contents = {
                'stat': HoloFS.Stat({
                    'st_mode': mode,
                    'st_dev': dev,
                    'st_nlink': 1,
                    'st_uid': os.getuid(),
                    'st_gid': os.getgid(),
                    'st_size': 0
                })
            }
            new_file = cls(fs, node_contents)
            new_file.persist()
            return new_file

        def open(self, flags, mode):
            self._refresh_if_stale()
            return HoloFS.FileHandle(self, flags, mode)

        def truncate(self, length):
            if length > 0:
                self._refresh_if_stale()
            os.truncate(self._real_path, length)
            self.commit()

        def getattr(self):
            return self.stat

    class SymLink(FSNode):
        def __init__(self, fs, node_contents, node_uuid=None):
            super().__init__(fs, node_contents, node_uuid)
            # self._data_key = f"data/{self.uuid}"
            self._data_entry = None
            self._target = node_contents.get('target', None)

        @classmethod
        def symlink(cls, fs, target):
            node_contents = {
                'stat': HoloFS.Stat({
                    'st_mode': 0o0777 | stat.S_IFLNK,
                    'st_dev': 0,
                    'st_nlink': 1,
                    'st_uid': os.getuid(),
                    'st_gid': os.getgid(),
                    'st_size': len(target)
                }),
                'target': target,
            }
            new_symlink = cls(fs, node_contents)
            new_symlink.persist()
            return new_symlink

        def persist(self):
            to_save = {
                'stat': self.stat.to_dict(),
                'target': self._target.encode('utf-8')
            }
            self._fs.set_key(self.key, dumps(to_save).encode('utf-8'))
            self._fs.on_change()

        @property
        def target(self):
            return self._target

        def readlink(self):
            return self.target

    class Dir(FSNode):
        def __init__(self, fs, node_contents, node_uuid=None):
            super().__init__(fs, node_contents, node_uuid)

        def _child_prefix(self):
            return f"fs/{self.uuid}/"

        def _child_search_key(self, name):
            return f"fs/{self.uuid}/{name}/"

        def _child_direntry_key(self, name, node_uuid):
            return f"fs/{self.uuid}/{name}/{node_uuid}"

        @classmethod
        def mkdir(cls, fs, mode):
            contents = {
                'stat': HoloFS.Stat({
                    'st_mode': stat.S_IFDIR | mode ,
                    'st_dev': 0,
                    'st_nlink': 2,
                    'st_uid': os.getuid(),
                    'st_gid': os.getgid(),
                    'st_size': 0
                })
            }
            new_dir = cls(fs, contents)
            new_dir.persist()
            return new_dir

        def add_child(self, name: str, node_uuid: str):
            return HoloFS.DirEntry(self._fs, self._child_direntry_key(name, node_uuid))

        def child(self, name):
            key, _ = self._fs.latest_prefix_one(self._child_search_key(name))
            if not key:
                return None
            return HoloFS.DirEntry(self._fs, key)

        def children(self):
            entries = self._fs.latest_prefix_many(self._child_prefix()).items()
            dir_entries = []
            for key, _ in entries:
                dir_entries.append(HoloFS.DirEntry(self._fs, key))
            return dir_entries

    class FileHandle(object):
        def __init__(self, fsnode, flags, mode):
            self.node = fsnode
            self.file = os.fdopen(os.open(fsnode._real_path, flags, mode), flag2mode(flags))
            self.fd = self.file.fileno()

        def read(self, length, offset):
            self.node._refresh_if_stale()
            return os.pread(self.fd, length, offset)

        def ftruncate(self, length):
            self.node._refresh_if_stale()
            os.truncate(self.node._real_path, length)
            self.node.commit()

        def write(self, buf, offset):
            return os.pwrite(self.fd, buf, offset)

        def release(self):
            self.file.close()
            self.node.commit()

        def fsync(self, issyncfile):
            os.fsync(self.fd)
            self.node.commit()

        def flush(self):
            os.fsync(self.fd)
            self.node.commit()

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
