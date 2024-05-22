from azure.storage.blob import BlobServiceClient, BlobLeaseClient
from env import (
    AZURE_ACCOUNT_CONTAINER,
    AZURE_ACCOUNT_KEY,
    AZURE_ACCOUNT_NAME,
    LOG_LEVEL_AZURE,
    BLOB_ARCHIVE_DIR,
    BLOB_INPUT_DIR,
    BLOB_QUEUE_DIR,
)
from time import sleep
from modules.sduk.common import set_logger
from azure.core import exceptions as azure_exceptions
from datetime import datetime, timedelta

logger = set_logger("azure.storage")
logger.setLevel(LOG_LEVEL_AZURE)


class BlobError(Exception):
    def __init__(self, message):
        super().__init__(message)


class AzureBlob:
    def __init__(
        self,
        azure_account_name=AZURE_ACCOUNT_NAME,
        azure_account_key=AZURE_ACCOUNT_KEY,
        azure_account_container=AZURE_ACCOUNT_CONTAINER,
    ):
        self.lease_breaker = {}
        try:
            self.service = BlobServiceClient(
                account_url="https://segdemoaks01sftp.blob.core.windows.net/",
                credential={
                    "account_name": azure_account_name,
                    "account_key": azure_account_key,
                },
            )

            self.container = self.service.get_container_client(azure_account_container)
            assert self.container.exists()

        except Exception as ex:
            print("Exception:")
            print(ex)

    def list_blobs(self, name_starts_with=""):
        return self.container.list_blob_names(name_starts_with=name_starts_with)

    def get_blob(self, blob=None):
        assert blob is not None
        return self.container.get_blob_client(blob=blob)

    def get_blob_with_lease(self, blob_name=None, lease_duration=-1):
        assert blob_name is not None
        blob = self.container.get_blob_client(blob=blob_name)
        now = datetime.now()
        if blob.get_blob_properties().size == 0:
            raise BlobError(f"{blob_name} - size=0")
        try:
            blob_lease = blob.acquire_lease(lease_duration=lease_duration)
        except azure_exceptions.ResourceExistsError as e:
            if blob_name not in self.lease_breaker:
                self.lease_breaker[blob_name] = now
            else:
                lease_delta = now - self.lease_breaker[blob_name]
                logger.error(f"Stale lease for {blob_name} found {lease_delta} old")
                if lease_delta > timedelta(minutes=5):
                    logger.error(f"Stale Lease older than 5 minutes -> Breaking")
                    BlobLeaseClient(blob).break_lease()
            raise e
        if blob_name in self.lease_breaker:
            self.lease_breaker.pop(blob_name)
        return blob, blob_lease

    def copy_blob(self, src_name=None, tgt_name=None):
        assert src_name is not None
        assert tgt_name is not None
        src_blob = self.get_blob(src_name)
        tgt_blob = self.get_blob(tgt_name)
        logger.debug(f"Copying {src_name} -> {tgt_blob}")
        tgt_blob.start_copy_from_url(src_blob.url)
        status = None
        for _ in range(20):
            sleep(1.23)
            status = tgt_blob.get_blob_properties().copy.status
            if status == "success":
                break
        if status != "success":
            logger.error(f"Copying {src_name} -> {tgt_blob} FAILED")
            tgt_blob.abort_copy()
            return False
        else:
            return True

    def move_blob(self, src_name=None, tgt_name=None, src_lease=None):
        if self.copy_blob(src_name, tgt_name):
            logger.debug(f"Deleting {src_name}")
            self.get_blob(src_name).delete_blob(lease=src_lease)


if __name__ == "__main__":
    AB = AzureBlob()
    from sys import argv

    if len(argv) > 1 and argv[1] == "reset":
        print("Moving archive back into input")
        for blob in AB.list_blobs(name_starts_with=f"{BLOB_ARCHIVE_DIR}/"):
            logger.info(blob)
            AB.copy_blob(
                blob, blob.replace(f"{BLOB_ARCHIVE_DIR}/", f"{BLOB_INPUT_DIR}/")
            )
    if len(argv) > 1 and argv[1] == "list":
        for key, val in AB.get_blob("input/test.zip").get_blob_properties().items():
            if val is not None:
                print(f"{key}: {val}")

    if len(argv) > 2 and argv[1] == "upload":
        filename = argv[2]
        print("Uploading TEST {filename} into input ")
        blob = AB.get_blob(f"{BLOB_INPUT_DIR}/{filename.split('/')[-1]}")
        with open(filename, "rb") as f:
            try:
                blob.upload_blob(f.read())
            except azure_exceptions.ResourceExistsError:
                logger.error("Tried to queue identical data.")

    if len(argv) > 2 and argv[1] == "download":
        import os

        print(AZURE_ACCOUNT_CONTAINER)
        dirname = argv[2]
        print("Downloading all from {dirname} into /mnt/cephfs/users/dh/all_sduk_data")
        for blob in AB.list_blobs(name_starts_with=f"{dirname}/"):
            logger.info(blob)
            blob_client = AB.get_blob(blob)
            blob_size = blob_client.get_blob_properties().size
            file_name = os.path.join(r"/mnt/cephfs/users/dh/all_sduk_data", blob)
            try:
                file_size = os.stat(file_name).st_size
            except:
                file_size = 0
            if blob_size == file_size:
                continue
            print(f"Blob_size: {blob_size}, File_size: {file_size}")
            with open(
                file=file_name,
                mode="wb",
            ) as sample_blob:
                download_stream = blob_client.download_blob()
                sample_blob.write(download_stream.readall())

    # for blob in AB.list_blobs(name_starts_with="input/SD"):
    #     logger.info(blob)
    #     AB.move_blob(blob, blob.replace("input", "archive"))
