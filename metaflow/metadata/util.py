from io import BytesIO
import os
import tarfile

from distutils.dir_util import copy_tree

from metaflow import util
from metaflow.datastore.local_backend import LocalBackend
from metaflow.metaflow_config import DATASTORE_LOCAL_DIR


def sync_local_metadata_to_datastore(task_ds):
    # Sync metadata from local metadatastore to datastore, so that scheduler
    # can pick it up and copy to its own local metadata store.
    with util.TempDir() as td:
        tar_file_path = os.path.join(td, 'metadata.tgz')
        with tarfile.open(tar_file_path, 'w:gz') as tar:
            # The local metadata is stored in the local datastore
            # which, for batch jobs, is always the DATASTORE_LOCAL_DIR
            tar.add(DATASTORE_LOCAL_DIR)
        # At this point we upload what need to s3
        with open(tar_file_path, 'rb') as f:
            _, key = task_ds.parent_datastore.save_data([f], len_hint=1)[0]
        task_ds.save_metadata({'local_metadata': key})


def sync_local_metadata_from_datastore(metadata, task_ds):
    # Do nothing if metadata is not local
    if metadata.TYPE != 'local':
        return

    # Otherwise, copy metadata from task datastore to local metadata store
    def echo_none(*args, **kwargs):
        pass
    key_to_load = task_ds.load_metadata(['local_metadata'])['local_metadata']
    _, tarball = next(task_ds.parent_datastore.load_data([key_to_load]))
    with util.TempDir() as td:
        with tarfile.open(fileobj=BytesIO(tarball), mode='r:gz') as tar:
            tar.extractall(td)
        copy_tree(
            os.path.join(td, DATASTORE_LOCAL_DIR),
            LocalBackend.get_datastore_root_from_config(echo_none),
            update=True)