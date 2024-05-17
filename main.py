import iroh
import fuse
from fuse import Fuse

IROH_DATA_DIR = "./iroh_data_dir"

class IrohFS:
    pass

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
    server = IrohFS(version="%prog " + fuse.__version__,
                    usage=usage,
                    dash_s_do='setsingle',
                    iroh_doc=doc
                    )

    server.parse(errex=1)
    server.main()


if __name__ == '__main__':
    main()
