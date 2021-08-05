"""
This function checks to see if the file exists under a different ext.
eg.
filename = filename1.txt
extension = 'jpg'

if filename1.jpg exists then True
if filename1.jpg does not exist then False

"""
import logging
import sys
sys.path.append("src")
from pipelines.integration.aws.s3 import S3Service
from pipelines.lang.filename_investigator import FilenameInvestigator

s3_service = S3Service()
filename_investigator = FilenameInvestigator()


def run(bucket_name, filename, extension):
    stripped_filename = filename_investigator.determine_base_filename(filename)

    new_filename = f'{stripped_filename}.{extension}'
    logging.debug(f'Checking for existence of {bucket_name}/{new_filename}, corresponding to {filename}')
    exists = s3_service.file_exists(bucket_name, new_filename)

    return {
        'bucket_name': bucket_name,
        'key': new_filename,
        'exists': exists
    }
