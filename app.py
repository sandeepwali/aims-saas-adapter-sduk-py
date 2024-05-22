from zipfile import ZipFile, BadZipFile
from io import TextIOWrapper
from tempfile import TemporaryFile
from hashlib import blake2b
from time import sleep
from datetime import timedelta
from json import dumps

from azure.core import exceptions as azure_exceptions

from modules.aims_saas.aims_saas_api_client import *
from modules.sduk.sduk import *
from modules.sduk.blob import AzureBlob, BlobError
from modules.sduk.common import *
from env import (
    BLOB_ARCHIVE_DIR,
    BLOB_INPUT_DIR,
    BLOB_QUEUE_DIR,
    BLOB_REJECT_DIR,
    BLOB_ACTIVE_DIR,
)

AB = AzureBlob()
SaasClient = AIMSSaaSAPIClient()


def main(logger):
    # with open(
    #     "tests/PE/zip_contents/e1/PE0033_PR_TST_Small_POS(Event_No_12500)_1912230333short.csv",
    #     "rb",
    # ) as pe33_file:
    #     process_pe0033_csv(
    #         pe33_file,
    #         pe_filename="PE0033_PR_TST_Small_POS(Event_No_12500)_1912230333.csv",
    #     )

    # todo
    # multi-promotion from csv-lines
    # logging level ausgabe ueberarbeiten
    #   zip und csv namen immer mit ausgeben
    while True:
        # Process Active / End-PE
        process_active_pe()

        # Process Queued Data
        process_queued()

        # Process Input
        process_input()

        # Process queued items
        # do_queud
        from sys import argv

        if len(argv) > 1 and argv[1] == "once":
            return
        logger.debug("####################### Sleeping #######################")
        sleep(90)


def extract_file_name_from_path(file_path):
    file_name = os.path.basename(file_path)
    return file_name


def get_sd_zip_file_path(start_dir):
    for root, dirs, files in os.walk(start_dir):
        for file in files:
            if match_file(file, starts="sd", ends=".zip"):
                zip_path = os.path.join(root, file)
                return zip_path
    return


def is_zip(path=""):
    return path.lower().endswith(".zip")


def is_csv(path=""):
    return path.lower().endswith(".csv")


def match_file(filename, starts=None, ends=None):
    does_start = filename.lower().startswith(starts.lower())
    does_end = filename.lower().endswith(ends.lower())
    return does_start and does_end


def process_input(
    input_dir=BLOB_INPUT_DIR,
    archive_dir=BLOB_ARCHIVE_DIR,
    queue_dir=BLOB_QUEUE_DIR,
    reject_dir=BLOB_REJECT_DIR,
):
    input_files = AB.list_blobs(name_starts_with=f"{input_dir}/")

    input_files = [input_file for input_file in input_files]
    input_files.sort()

    if not input_files:
        logger.debug("Nothing to process")
        return

    for input_file in input_files:
        _was_processed = False
        try:
            blob, blob_lease = AB.get_blob_with_lease(input_file)
        except azure_exceptions.ResourceExistsError:
            logger.error(
                f"Could not get lease for {input_file}, will wait so that nothing is processed out of order"
            )
            break
        except BlobError as e:
            logger.debug(e)
            continue

        if is_csv(input_file):
            _was_processed = True

        elif is_zip(input_file):
            tmp_file = TemporaryFile()
            blob.download_blob().readinto(tmp_file)

            try:
                zip = ZipFile(tmp_file)
            except BadZipFile:
                logger.error(f"{input_file} is not a zip file")
                tgt_blob = input_file.replace(f"{input_dir}/", f"{reject_dir}/")
                AB.move_blob(
                    src_name=input_file, tgt_name=tgt_blob, src_lease=blob_lease
                )
                logger.error(f"Moved {input_file} to {tgt_blob}")
                continue
            zip_test = zip.testzip()

            if zip_test:
                logger.error(f"Error in {input_file} - {zip_test}")
                # maybe abort here

            # Process SD Files
            if match_file(input_file, starts=f"{input_dir}/SD", ends=".zip"):
                zipped_files = [
                    name
                    for name in zip.namelist()
                    if match_file(name, starts="plu", ends=".csv")
                ]

                # Search for specific CSV files in extracted directory and print the first line
                for zipped_csv_file in zipped_files:
                    logger.debug(f"Processing PLU file {zipped_csv_file}")
                    with zip.open(zipped_csv_file, "r") as f:
                        process_plu_csv(f, zipped_csv_file, queue_dir)
                _was_processed = True

            # Process PE0033 Files
            elif match_file(input_file, starts=f"{input_dir}/PE", ends=".zip"):
                zipped_files = [
                    name
                    for name in zip.namelist()
                    if match_file(name, starts="pe0033", ends=".csv")
                ]

                # Search for specific CSV files in extracted directory and print the first line
                for zipped_csv_file in zipped_files:
                    logger.debug(f"Processing PE file {zipped_csv_file}")
                    with zip.open(zipped_csv_file) as f:
                        process_pe0033_csv(
                            f=f,
                            pe_filename=zipped_csv_file,
                            queue_dir=queue_dir,
                            extra_data={"pe0033_csv_filename": zipped_csv_file},
                        )
                _was_processed = True

        if _was_processed:
            _move_to = f"{archive_dir}/"
        else:
            _move_to = f"{reject_dir}/"
        AB.move_blob(
            src_name=input_file,
            tgt_name=input_file.replace(f"{input_dir}/", _move_to),
            src_lease=blob_lease,
        )


def process_pe0033_csv(
    f=None,
    pe_filename=None,
    queue_dir=BLOB_QUEUE_DIR,
    active_dir=BLOB_ACTIVE_DIR,
    article_filter=False,
    extra_data={},
):
    assert f is not None
    assert pe_filename is not None

    header_line = f.readline().strip()
    f.seek(0)
    csv_data = f.read()
    csv_hash = blake2b(csv_data, digest_size=4)
    f.seek(0)

    start_date, end_date, promo_type = sduk_csv_pe_parse_header(header_line)

    # if True:
    #     from datetime import datetime
    #     from random import randrange

    #     start_date = datetime.now(tz=pytz.UTC)
    #     start_date += timedelta(days=randrange(-8, 2))
    #     start_date += timedelta(hours=randrange(-11, 11))
    #     end_date = start_date + timedelta(
    #         days=randrange(1, 14), hours=randrange(-13, 13)
    #     )

    start_date_iso, end_date_iso = start_date.isoformat(), end_date.isoformat()

    pe_duration = end_date - start_date
    if pe_duration.seconds > 20 * 60 * 60:
        pe_duration = timedelta(days=pe_duration.days + 1)

    extra_data["pe_start_date"] = (
        f"{str(start_date.day)}.{str(start_date.month)}.{str(start_date.year)}"
    )
    extra_data["pe_end_date"] = (
        f"{str(end_date.day)}.{str(end_date.month)}.{str(end_date.year)}"
    )
    extra_data["pe_duration_days"] = str(pe_duration.days)
    extra_data["pe_promo_type"] = promo_type
    # Check if the store id is listed

    if is_in_future(start_date):
        # Queue the Articles
        blob_name = f"{queue_dir}/{start_date_iso}|{csv_hash.hexdigest()}|{pe_filename}"
        logger.debug(f"Storing {pe_filename} articles in {blob_name}")
        try:
            AB.get_blob(blob_name).upload_blob(csv_data)
        except azure_exceptions.ResourceExistsError:
            logger.error("Tried to queue identical data.")
    elif is_in_future(end_date):
        # Send the Articles
        for store in get_existing_store_ids():
            send_csv_pe0033_items_to_aims(
                store,
                csv_file=f,
                csv_filename=pe_filename,
                article_filter=article_filter,
                extra_data=extra_data,
            )
        blob_name = f"{active_dir}/{end_date_iso}|{csv_hash.hexdigest()}|{pe_filename}"
        for _ in range(10):
            try:
                AB.get_blob(blob_name).upload_blob(csv_data)
                break
            except azure_exceptions.ResourceExistsError:
                logger.error(
                    f"Tried to 'activate' identical data. ({blob_name} already present)"
                )
                break
            except azure_exceptions.ServiceResponseError:
                logger.error(f"Timeout uploading {blob_name}, retrying.")
            logger.error(f"was not able to store {blob_name}")

    else:
        # Ignore the Articles
        logger.debug("Do nothing")


def reprocess_pe0033_csv(
    f=None,
    pe_filename=None,
    article_filter=False,
    extra_data={},
):
    assert f is not None
    assert pe_filename is not None

    header_line = f.readline().strip()
    f.seek(0)

    start_date, end_date, promo_type = sduk_csv_pe_parse_header(header_line)

    pe_duration = end_date - start_date
    if pe_duration.seconds > 20 * 60 * 60:
        pe_duration = timedelta(days=pe_duration.days + 1)

    extra_data["pe_start_date"] = (
        f"{str(start_date.day)}.{str(start_date.month)}.{str(start_date.year)}"
    )
    extra_data["pe_end_date"] = (
        f"{str(end_date.day)}.{str(end_date.month)}.{str(end_date.year)}"
    )
    extra_data["pe_duration_days"] = str(pe_duration.days)
    extra_data["pe_promo_type"] = promo_type
    # Check if the store id is listed

    # Send the Articles
    for store in get_existing_store_ids():
        send_csv_pe0033_items_to_aims(
            store,
            csv_file=f,
            csv_filename=pe_filename,
            article_filter=article_filter,
            extra_data=extra_data,
        )


def process_plu_csv(f: bytes = None, plu_filename=None, queue_dir=BLOB_QUEUE_DIR):
    assert f is not None
    assert plu_filename is not None

    header_line = f.readline().strip()
    store_id_str, activation_datetime, plu_extra_data = sduk_csv_sd_parse_header(
        header_line
    )

    # Check if the store id is listed
    if not is_store_id_existing(store_id=store_id_str):
        logger.debug(
            f"Skipping file PLU file {plu_filename}, store {store_id_str} does not exist in resources/store_ids.csv"
        )
        return None

    if is_in_future(activation_datetime):
        f.seek(0)
        csv_data = f.read()
        csv_hash = blake2b(csv_data, digest_size=4)
        activation_iso = activation_datetime.isoformat()
        blob_name = (
            f"{queue_dir}/{activation_iso}|{csv_hash.hexdigest()}|{plu_filename}"
        )
        logger.debug(f"Storing {plu_filename} articles in {blob_name}")
        try:
            AB.get_blob(blob_name).upload_blob(csv_data)
        except azure_exceptions.ResourceExistsError:
            logger.error("Tried to queue identical data.")
    else:
        plu_extra_data.update({"plu_csv_filename": plu_filename})
        send_csv_plu_items_to_aims(
            store_id_str,
            csv_without_header=f,
            csv_filename=plu_filename,
            extra_data=plu_extra_data,
        )


def strip_empty_fields(dict_to_clean):
    cleaned_dict = {}
    for key in dict_to_clean.keys():
        if isinstance(dict_to_clean[key], dict):
            cleaned_dict[key] = strip_empty_fields(dict_to_clean[key])
        elif not isinstance(dict_to_clean[key], str):
            cleaned_dict[key] = dict_to_clean[key]
        elif dict_to_clean[key]:
            cleaned_dict[key] = dict_to_clean[key]
    return cleaned_dict


def strip_some_empty_fields(dict_to_clean, filter=[]):
    cleaned_dict = {}
    for key in dict_to_clean.keys():
        if isinstance(dict_to_clean[key], dict):
            cleaned_dict[key] = strip_some_empty_fields(dict_to_clean[key], filter)
        elif key not in filter:
            cleaned_dict[key] = dict_to_clean[key]
        elif not isinstance(dict_to_clean[key], str):
            cleaned_dict[key] = dict_to_clean[key]
        elif dict_to_clean[key]:
            cleaned_dict[key] = dict_to_clean[key]
    return cleaned_dict


def send_csv_plu_items_to_aims(
    store_id_str=None, csv_without_header=None, csv_filename=None, extra_data={}
):
    assert type(store_id_str) is str
    assert csv_without_header is not None
    assert csv_filename is not None
    csv_articles = sduk_csv_sd_parse_items_into_articles(csv_without_header)
    logger.info(
        f"Processed {len(csv_articles)} PLU items for {store_id_str} / {csv_filename}"
    )

    articles = []

    for csv_article in csv_articles:
        op_code = csv_article.get("data", {}).get("OP_CODE", False)
        if not op_code:
            logger.error("OP_CODE does not exists, this should never happen")
            assert op_code is not False
        if op_code == "0":  # Filter out Ignore record
            continue
        elif op_code == "4":  # Filter out Delete
            continue
        elif op_code == "3":  # Delete empty fields for Price Updates
            csv_article = strip_empty_fields(csv_article)
        elif op_code == "2":  # Article Updates may not contain prices
            csv_article = strip_some_empty_fields(
                csv_article, ["RTL_PRC", "RTL_PRC_DATE"]
            )
        elif op_code == "5":
            logger.error(
                f"Unhandled PLU OP_CODE 5 (Sale) {dumps(csv_article)}, stripping empty fields"
            )
            csv_article = strip_empty_fields(csv_article)
        csv_article["data"].update(extra_data)
        articles.append(csv_article)

    SaasClient.get_access_token()
    SaasClient.add_articles(store_code=store_id_str, articles=articles)
    logger.debug(
        f"Sent {len(articles)} articles for store {store_id_str} / {csv_filename}"
    )


def send_csv_pe0033_items_to_aims(
    store_id_str="MASTER",
    csv_file=None,
    csv_filename=None,
    article_filter=False,
    extra_data={},
):
    assert csv_file is not None
    assert csv_filename is not None

    if extra_data.get("pe_promo_type", False) == "LOYALTY":
        csv_articles = sduk_csv_pe0033_parse_items_into_articles(
            # csv_file, header_postfix="__loyalty"
            csv_file
        )
    else:
        csv_articles = sduk_csv_pe0033_parse_items_into_articles(csv_file)

    if article_filter:
        articles = [
            article
            for article in csv_articles
            if article["articleId"] in article_filter
        ]
    else:
        articles = csv_articles

    # from json import dumps
    # len(dumps(articles))
    # for article in articles:
    #     for key in article.keys():
    #         if key == "data":
    #             for data_key in article[key]:
    #                 if not article[key][data_key]:
    #                     article[key].pop(data_key)
    #         if not article[key]:
    #             article.pop(key)
    # len(dumps(articles))

    for article in articles:
        article["data"].update(extra_data)

    logger.info(
        f"Processed {len(csv_articles)} (filtered: {len(articles)}) PE0033 items for {store_id_str}"
    )

    if len(csv_articles) == 0:
        logger.error(f"PE csv without content processed {csv_filename}")

    SaasClient.get_access_token()
    SaasClient.add_articles(store_code=store_id_str, articles=articles)
    logger.debug(f"Sent {len(articles)} articles for store {store_id_str}")


def process_queued(queue_dir=BLOB_QUEUE_DIR):
    current = datetime.now(tz=pytz.UTC)
    logger.debug(f"Processing Queue for NOW: {current.isoformat()}")

    queue_files = AB.list_blobs(name_starts_with=f"{queue_dir}/")

    queue_files = [queue_file for queue_file in queue_files]
    queue_files.sort()

    for file in queue_files:
        blob_filename = file.replace(f"{queue_dir}/", "")
        try:
            file_activation_str, file_hash, file_name = blob_filename.split("|")
        except Exception as e:
            logger.error(e)
        file_activation = datetime.fromisoformat(file_activation_str)

        if current > file_activation:
            try:
                blob, blob_lease = AB.get_blob_with_lease(file)
            except azure_exceptions.ResourceExistsError:
                logger.error(
                    f"Could not get lease for {file}, will wait so that nothing is processed out of order"
                )
                break

            if blob.get_blob_properties().size == 0:
                logger.error(f"{file} - size=0, skipping")
                continue

            was_processed = False
            if match_file(file_name, starts="plu", ends=".csv"):
                with TemporaryFile() as tmp_file:
                    blob.download_blob().readinto(tmp_file)
                    tmp_file.seek(0)
                    store_id_str, _, plu_extra_data = sduk_csv_sd_parse_header(
                        tmp_file.readline().strip()
                    )
                    plu_extra_data.update({"plu_csv_filename": file_name})
                    send_csv_plu_items_to_aims(
                        store_id_str,
                        csv_without_header=tmp_file,
                        csv_filename=file_name,
                        extra_data=plu_extra_data,
                    )
                    was_processed = True
            elif match_file(file_name, starts="pe0033", ends=".csv"):
                with TemporaryFile() as tmp_file:
                    blob.download_blob().readinto(tmp_file)
                    tmp_file.seek(0)
                    process_pe0033_csv(
                        tmp_file,
                        pe_filename=file_name,
                        extra_data={"pe0033_csv_filename": file_name},
                    )
                    was_processed = True
            if was_processed:
                blob.delete_blob(lease=blob_lease)
                logger.debug(f"Deleted {file}")
            else:
                blob_lease.release()
                logger.error(f"Processing of {file_name} failed")
                # ToDo, maybe move blob into queue_error folder or something.

        else:
            logger.debug(f"Skipped sending {file}, the time has not yet come.")


def get_articles_in_pe0033(blob_name=None):
    assert blob_name is not None

    articles = []

    try:
        blob = AB.get_blob(blob_name)
    except azure_exceptions.ResourceExistsError:
        logger.error(f"Could not get lease for {blob_name}")
    with TemporaryFile() as tmp_file:
        blob.download_blob().readinto(tmp_file)
        tmp_file.seek(0)
        _, _, promo_type = sduk_csv_pe_parse_header(tmp_file.readline().strip())
        header_line = tmp_file.readline().strip()

        for line in tmp_file.readlines():
            article_id = decode_text(line.split(b",", maxsplit=1)[0])
            if article_id == "=ROW()":
                continue
            if article_id not in articles:
                articles.append(article_id)

    return articles, blob, promo_type


def process_active_pe(active_dir=BLOB_ACTIVE_DIR):
    current = datetime.now(tz=pytz.UTC)
    logger.debug(f"Processing Queue for NOW: {current.isoformat()}")
    active_files = AB.list_blobs(name_starts_with=f"{active_dir}/")

    def group_files_by_time():
        files_to_deactivate = []
        files_still_active = []

        for file in active_files:
            blob_filename = file.replace(f"{active_dir}/", "")
            file_enddate_str, file_hash, file_name = blob_filename.split("|")
            file_enddate = datetime.fromisoformat(file_enddate_str)
            if match_file(file_name, starts="pe0033", ends=".csv"):
                if current > file_enddate:
                    files_to_deactivate.append(file)
                else:
                    files_still_active.append(file)

        files_to_deactivate.sort()
        files_still_active.sort()
        return files_to_deactivate, files_still_active

    files_to_deactivate, files_still_active = group_files_by_time()

    logger.debug(f"PE files to deactivate: {files_to_deactivate}")
    logger.debug(f"still active PE files:  {files_still_active}")

    for file in files_to_deactivate:
        try:
            blob, blob_lease = AB.get_blob_with_lease(file)
        except azure_exceptions.ResourceExistsError:
            logger.error(
                f"Could not get lease for {file}, will wait so that nothing is processed out of order"
            )
            break

        if blob.get_blob_properties().size == 0:
            logger.error(f"{file} - size=0, skipping")
            continue

        articles_to_check, _, promo_type = get_articles_in_pe0033(blob_name=file)

        fileblobs_to_reprocess = set()
        found_articles = set()
        for active_file in files_still_active:
            active_file_articles, active_file_blob, _ = get_articles_in_pe0033(
                blob_name=active_file
            )
            for article in articles_to_check:
                if article in active_file_articles:
                    fileblobs_to_reprocess.add((active_file, active_file_blob))
                    found_articles.add(article)

        logger.debug(
            f"PE files to reprocess (having one of the articles from the file that ended): {list(map(lambda x: x[0], fileblobs_to_reprocess))}"
        )

        articles_without_active_promotion = set(articles_to_check) - found_articles

        if promo_type == "LOYALTY":
            articles_no_promo = [
                # {"articleId": article, "data": {"offer_type__loyalty": ""}}
                {"articleId": article, "data": {"offer_type": ""}}
                for article in articles_without_active_promotion
            ]
        else:
            articles_no_promo = [
                {"articleId": article, "data": {"offer_type": ""}}
                for article in articles_without_active_promotion
            ]

        if articles_no_promo:
            SaasClient.get_access_token()
            [
                SaasClient.add_articles(store_code=store, articles=articles_no_promo)
                for store in get_existing_store_ids()
            ]

        def reprocess_blobs():
            for blob_path, blob in fileblobs_to_reprocess:
                blob_filename = blob_path.replace(f"{active_dir}/", "")
                _, _, file_name = blob_filename.split("|")
                with TemporaryFile() as tmp_file:
                    blob.download_blob().readinto(tmp_file)
                    tmp_file.seek(0)
                    reprocess_pe0033_csv(
                        tmp_file,
                        pe_filename=file_name,
                        article_filter=found_articles,
                        extra_data={"pe0033_csv_filename": file_name},
                    )
                    logger.debug(f"Re-Processed {blob_path}")

        reprocess_blobs()
        logger.info(f"Deleting successfully deactivated blob: {file}")
        blob.delete_blob(lease=blob_lease)
        # ToDo, maybe move blob into queue_error folder or something.


if __name__ == "__main__":
    # Set logger
    logger = set_logger(logger_name="app", log_file="./logs/main.log")
    main(logger)
