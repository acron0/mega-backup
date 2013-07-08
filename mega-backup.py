from __future__ import with_statement
from contextlib import closing
from zipfile import ZipFile, ZIP_DEFLATED
from mega import Mega
import os, sys, datetime

USERNAME = "<email>"
PASSWORD = "<password>"
DIR = "<directory_to_put_backups>"
CLEAN_OLDER_THAN = datetime.timedelta(28) # 28 days

def sizeof_fmt(num):
    for x in ['bytes','KB','MB','GB']:
        if num < 1024.0 and num > -1024.0:
            return "%3.1f%s" % (num, x)
        num /= 1024.0
    return "%3.1f%s" % (num, 'TB')

def zipdir(basedir, archivename):
    assert os.path.isdir(basedir)
    with closing(ZipFile(archivename, "w", ZIP_DEFLATED)) as z:
        for root, dirs, files in os.walk(basedir):
            for fn in files:
                absfn = os.path.join(root, fn)
                zfn = absfn[len(basedir)+len(os.sep):]
                z.write(absfn, zfn)

def get_dir_id(m, directory):
        try:
                return m.find(directory)[1]['h']
        except Exception:
                return None

def upload(m, file):
	backup_id = get_dir_id(m, DIR)
	
	if backup_id is None:
		print "Couldn't find %s directory" % DIR
		exit()
	
	print "Uploading file..."
	file = m.upload(file, backup_id)
	print file

def cleanup(m):
	backup_id = get_dir_id(m, DIR)
	
	if backup_id is None:
		print "Couldn't find %s directory" % DIR
		exit()

	print "Cleaning up files older than %s..." % str(CLEAN_OLDER_THAN)
	all_files = m.get_files_in_node(1)
	file_keys = filter(lambda w: all_files[w]['p'] == backup_id, all_files.keys())
	count = 0
	for fk in file_keys:
		md = datetime.datetime.fromtimestamp(all_files[fk]['ts'])
		if md + CLEAN_OLDER_THAN < datetime.datetime.now():
			m.delete(fk)
			count += 1

	print "Deleted %d file(s)" % count

if __name__ == '__main__':
	basedir = sys.argv[1]
	archivename = "/tmp/" + os.path.basename(basedir.strip('/')) + "_" + str(datetime.datetime.now().isoformat()) + ".zip"
	print "Archiving %s " % archivename
	zipdir(basedir, archivename)
	size = os.stat(archivename).st_size
	print sizeof_fmt(size)

	mega = Mega()
	m = mega.login(USERNAME,PASSWORD)

	upload(m, archivename)
	cleanup(m)
	os.system("rm %s" % archivename)
