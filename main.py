import errno, stat
import io

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

class IrohDocFS:
    write_buffer = {}

    chunk_size = 262144

    def __init__(self, *args, **kwargs):
        super(IrohDocFS, self).__init__(*args, **kwargs)
        self.iroh_doc =kwargs.pop('iroh_doc')
        self.iroh_author =kwargs.pop('iroh_author')
        # Debug logging
        log_level = logging.DEBUG
        self.logger = self._setup_logging(log_level)

    def _setup_logging(self, log_level):
        logger = logging.getLogger('mfs')
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

    def mkdir(self, path, mode):
        self.logger.debug("mkdir: " + path)
        try:
            self.iroh_doc.set_bytes(self.iroh_author, path + '.stat', dumps({'Type': 'directory'}))
            self.iroh_doc.set_bytes(self.iroh_author, path + '.dir', dumps({'Entries': []}))
            self._on_change()
        except Exception as e:
            return -errno.EIO

    def read(self, path, size, offset):
        self.logger.debug("read: " + path)
        try:
            self._sync(path)
            # some_bytes = self.ipfs_conn.files.read(self.root_path + path, offset, size)
            some_bytes = self.iroh_doc.get_exact(self.iroh_author, path + '.data')
            # @TODO: deal with offset and size
        except Exception as e:
            return -errno.ENOENT
        return some_bytes


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
    server = IrohDocFS(version="%prog " + fuse.__version__,
                    usage=usage,
                    dash_s_do='setsingle',
                    iroh_author=author,
                    iroh_doc=doc
                    )

    server.parse(errex=1)
    server.main()


if __name__ == '__main__':
    main()
