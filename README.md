# HoloFS
## Your personal distributed filesystem

### Status
⚠️️⚠️️⚠️️ HoloFS is pre-alpha software released as a proof of concept ⚠️️⚠️️⚠️️

In its current iteration HoloFS is just about starting to look like a prototype: Error checking is ... present, corner cases are
somewhat understood and even the that are understood starting to be handled. That said, it does work to some extent
and I'm very interested in feedback about the direction, the features people are interested in and the use-cases that 
come to mind. I'm also interested in bug reports, but please realize this is very much a side project for me and 
progress may be quite sporadic. PRs are always welcome!

### Concept
HoloFS is a distributed filesystem based on the [Iroh toolkit](https://iroh.computer/). When mounted it stores files in
the local Iroh data store and replicates them to any online peers you grant access to. Files can be written to offline 
and synchrnized when network access is available. Some effort has been made to design a filesystem structure that is 
resistant to conflicts in trivial cases, but if two offline peers both write changes to a single file the result will be
that the "last write wins" (i.e., the write with the most recent modified time is the one presented by the filesystem). 
I have some ideas about how conflicts could be handled more sanely and Iroh provides some functionality there (entry 
versions, authors, etc) that could be built on. An explicit non-goal is for this to be fully compatible with POSIX 
Filesystem semantics. That means this will likely never be capable of hosting something like Postgres that relies on specific 
behavior guaranteed by the POSIX FS operations. That said, this could be an interesting tool for keeping a directory of 
pictures or media files in sync across several personal computers.

### Requirements
Clone the repository and install the Python dependencies. I recommend creating a virtual environment. Something like 
this should work:
- `python3 -m venv env`
- `./env/bin/activate`
- `pip3 install -r requirements.txt`

### Usage

To create a new filesystem:

```python3 holofs.py -s -f --create --share ./mnt```

💡 Note the Doc ID printed when you create the filesystem. You'll need this to mount it again later.

To open a previously created filesystem:

```python3 holofs.py -s -f --doc=jd67twwwyqcog3ijv4lbnifnex23so4vj4v5amh36nwk627owclq ./mnt```

To join a remotely shared filesystem:

```python3 holofs.py -s -f --join=docaaacbe2jkgdyajvcqgrczaeeceihuk5re4brmz5tcdrwks2ccbsmrcofagxpy3f22yds26zxw2fpwi2uxcehmkjj6fko2e5urzj6iob3x7rf4ajdnb2hi4dthixs65ltmuys2mjoojswyylzfzuxe33ifzxgk5dxn5zgwlrpauaaufaaahcfo8dc663rl3akacwbeaabyrlqblc6vqc4ivybeyaqmaqhp7fduj2hqhatxvx4elcvo ./mnt```

To see help (and an example of how poorly errors are handled right now):

```python3 holofs.py -h```
