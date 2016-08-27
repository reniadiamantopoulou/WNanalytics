from time import time

from download import auto_download
from insert_to_db import insert_to_db

if __name__ == '__main__':
    t = time()
    auto_download()
    insert_to_db()
    print "Download data and insertion finished in %f seconds." % (time() - t)
