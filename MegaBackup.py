"""
mega-backup.py
zip a directory in one or more zips and upload to mega.co.nz
"""
from __future__ import with_statement
from contextlib import closing
from zipfile import ZipFile, ZIP_DEFLATED
from mega import Mega
from FileSplitter import FileSplitter
from math import ceil
import os, sys, datetime

USERNAME = "<email>"
PASSWORD = "<password>"
DIR = "<directory_to_put_backups>"
CLEAN_OLDER_THAN = datetime.timedelta(28) # 28 days
MAX_FILE_SIZE = 536870912 # 512MB

def sizeof_fmt(num):
    """
    friendly size of file
    """
    for suffix in ['bytes', 'KB', 'MB', 'GB']:
        if num < 1024.0 and num > -1024.0:
            return "%3.1f%s" % (num, suffix)
        num /= 1024.0
    return "%3.1f%s" % (num, 'TB')

def zipdir(basedir, archivename, max_size=None):
    """
    zip up a directory
    """
    assert os.path.isdir(basedir)
    with closing(ZipFile(archivename, "w", ZIP_DEFLATED, True)) as zip_file:
        for root, _, files in os.walk(basedir):
            for filename in files:
                absfn = os.path.join(root, filename)
                zfn = absfn[len(basedir)+len(os.sep):]
                zip_file.write(absfn, zfn)

def splitfile(file_name, file_size):
    num_files = ceil(float(file_size)/float(MAX_FILE_SIZE))
    print file_size, MAX_FILE_SIZE, num_files
    
    fsp = FileSplitter()
    fsp.parseOptions(["-i", file_name, "-n", num_files, "-s"])
    fsp.do_work()
    return num_files

def get_dir_id(mega_inst, directory):
    """
    find the id of a directory from the mega response
    """
    try:
        return mega_inst.find(directory)[1]['h']
    except Exception:
        return None

def upload(mega_inst, file_to_upload):
    """
    upload a file
    """
    print "Looking for", DIR, "directory"
    backup_id = get_dir_id(mega_inst, DIR)
    
    if backup_id is None:
        print "Couldn't find %s directory" % DIR
        exit()
    else:
        print backup_id
    
    print "Uploading file %s..." % file_to_upload
    file_response = mega_inst.upload(file_to_upload, backup_id)
    print file_response

def cleanup(mega_inst):
    """
    removes files older than a certain date
    """
    backup_id = get_dir_id(mega_inst, DIR)
    
    if backup_id is None:
        print "Couldn't find %s directory" % DIR
        return

    print "Cleaning up files older than %s..." % str(CLEAN_OLDER_THAN)
    all_files = mega_inst.get_files_in_node(1)
    file_keys = filter(lambda w: all_files[w]['p'] == backup_id, all_files.keys())
    count = 0
    for file_key in file_keys:
        file_date = datetime.datetime.fromtimestamp(all_files[file_key]['ts'])
        if file_date + CLEAN_OLDER_THAN < datetime.datetime.now():
            mega_inst.delete(file_key)
            count += 1

    print "Deleted %d file(s)" % count

if __name__ == '__main__':
    BASE_DIR = sys.argv[1]
    ARCHIVE_NAME = "/tmp/" + os.path.basename(BASE_DIR.strip('/')) + "_" + str(
        datetime.datetime.now().isoformat()) + ".zip"

    print "Archiving %s " % ARCHIVE_NAME
    zipdir(BASE_DIR, ARCHIVE_NAME)
    ARCHIVE_SIZE = os.stat(ARCHIVE_NAME).st_size
    print sizeof_fmt(ARCHIVE_SIZE)

    num_files = 1
    if ARCHIVE_SIZE > MAX_FILE_SIZE:
        print 'Splitting file into chunks...'
        num_files = splitfile(ARCHIVE_NAME, ARCHIVE_SIZE)

    print "Logging into Mega..."
    MEGA_INST = Mega({'verbose': True}).login(USERNAME, PASSWORD)

    try: 
        if num_files == 1:
            upload(MEGA_INST, ARCHIVE_NAME)
        else:
            for file_index in range(int(num_files)):
                upload(MEGA_INST, ARCHIVE_NAME+"-"+str(file_index+1))
        cleanup(MEGA_INST)
    except Exception as e:
        print e
    print 'Removing temporary files...'
    os.system("rm %s*" % ARCHIVE_NAME)
