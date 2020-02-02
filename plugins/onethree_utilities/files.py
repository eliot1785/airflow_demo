'''
These functions provide file-related functionality.
Currently it assumes we're only working with GCS, but
if we had a hybrid cloud structure we'd obviously
add AWS support as well.
'''

import os
import errno
import subprocess
import logging

log = logging.getLogger(__name__)

def create_enclosing_dirs_local(file):
    '''
    Works on a local filesystem only. Creates any directories
    that might be needed to create a file at the specified
    location.

    :param file: Path to the file whose containing directories
    need to be created if they don't exist.
    '''

    # Suppress any exceptions caused by race conditions
    # in between the first and second command, since
    # two tasks may be trying to create the same directory
    # at the same time.
    if not os.path.exists(os.path.dirname(file)):
        try:
            os.makedirs(os.path.dirname(file))
        except OSError as exc:
            if exc.errno != errno.EEXIST:
                raise

def copy_local_to_gcs(local_file, remote_file):
    '''
    Copies a local file to Google Cloud Storage. This is
    the appropriate method to call even when running in the
    cloud already (i.e. local just means local to the machine).

    "Directories" are automatically created because to Cloud
    Storage, "a file in a directory" is just "a file with /
    characters in the name."

    :param local_file: The local file to be copied.
    :param remote_file: The remote file to be created.
    '''

    subprocess.call(['gsutil', 'cp', local_file, remote_file])

def move_to_output_loc(env, source_file, target_file):
    '''
    Moves a produced file from its local location to its
    output location. When env is local this means local-to-local,
    otherwise this means local-to-GCS. target_file should be
    the corresponding path.

    :param env: The environment where the code is running
    ('local', 'prod', etc)
    :param source_file: Absolute path to the file to be moved
    :param target_file: Absolute path to the destination,
    as a local or Cloud Storage path depending on the environment
    '''

    if env != 'local':
        copy_local_to_gcs(source_file, target_file)
        os.remove(source_file)
    else:
        create_enclosing_dirs_local(target_file)
        os.rename(source_file, target_file)

def append_to_local(env, source_path, target_path):
    '''
    Appends the contents of a file (in GCS or local)
    to a local file. Separate downloading isn't necessary.

    :param env: The environment where the code is running
    ('local', 'prod', etc)
    :param source_path: Absolute path to the source file,
    as a local or Cloud Storage path depending on the
    environment.
    :param target_path: Absolute path to the target file
    '''
    if env != 'local':
        command = 'gsutil cat {} >> {}'.format(source_path, target_path)
    else:
        command = 'cat {} >> {}'.format(source_path, target_path)
    log.info(command)
    os.system(command)
