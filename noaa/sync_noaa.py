from pathlib import Path
from ftplib import FTP
import tempfile
import shelve
import tarfile
import gzip
import hdfs

START_YEAR = 1929
END_YEAR = 2020

HDFS_URL = 'http://had03.hadoop.test:50070'
HDFS_USER = 'hdfs'
DATALAKE_DIR = '/data/gsod'

FTP_URL = 'ftp.ncdc.noaa.gov'
FTP_DIR = '/pub/data/gsod'

print('Trying to connect to', HDFS_URL, 'as', HDFS_USER, '...', end=' ')
hdfs_client = hdfs.InsecureClient(HDFS_URL, user=HDFS_USER)
print('success')
base_directory_status = hdfs_client.status(DATALAKE_DIR, strict=False)
if base_directory_status is None:
    print('Creating', DATALAKE_DIR, '...', end=' ')
    hdfs_client.makedirs(DATALAKE_DIR)
    print('success')

for year in range(START_YEAR, END_YEAR+1):
    year_directory = DATALAKE_DIR + '/' + str(year)
    year_directory_status = hdfs_client.status(year_directory,
                                                   strict=False)
    if year_directory_status is None:
        print('Creating', year_directory, '...', end=' ')
        hdfs_client.makedirs(year_directory)
        print('success')
    with tempfile.TemporaryDirectory() as tmpdirname, FTP(FTP_URL) as ftp:
        current_file = 'gsod_' + str(year) + '.tar'
        print('Trying to connect to', FTP_URL, '...', end=' ')
        ftp.login()
        ftp.cwd(FTP_DIR + '/' + str(year))
        print('success')
        print('Retrieving', current_file, '...', end = ' ')
        with open(Path(tmpdirname).joinpath(current_file), 'wb') as fp:
            ftp.retrbinary('RETR ' + current_file, fp.write)
        print('success')
        ftp.quit()
        print('untar', current_file, '...', end = ' ')
        with tarfile.open(Path(tmpdirname).joinpath(current_file), 'r') as tar:
            tar.extractall(tmpdirname)
            print('success')
            zipfiles = Path(tmpdirname).glob('*.gz')
            for zipfile in zipfiles:
                print('unzip', zipfile.stem, '...', end=' ')
                outfile = zipfile.parent.joinpath(zipfile.stem)
                with gzip.open(zipfile) as zf, open(outfile, 'wb') as of:
                    content = zf.read()
                    of.write(content)
                print('success')
                print('upload', zipfile.stem, '...', end=' ')
                hdfs_client.upload(year_directory, outfile, overwrite=True)
                print('success')
