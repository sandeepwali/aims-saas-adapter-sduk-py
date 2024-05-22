import pytz
import chardet
from datetime import datetime, timedelta
from env import CSV_TZ, CSV_TIMEFORMAT
from modules.sduk.common import set_logger

logger = set_logger("sduk")


def decode_text(byte_array: bytes = None) -> str:
    assert byte_array is not None

    if len(byte_array) == 0:
        return ""
    try:
        decoded_str = byte_array.decode(encoding="ISO-8859-1")
    except:
        detection = chardet.detect(byte_array)
        logger.error(
            f"Decoding Failed: {detection['encoding']} with {detection['confidence']}"
        )
    return decoded_str


def sduk_csv_parse_timestamp(
    csv_time: str, time_format=CSV_TIMEFORMAT, timezone_str=CSV_TZ
):
    timezone = pytz.timezone(timezone_str)
    try:
        parsed_datetime = datetime.strptime(csv_time, time_format)
    except ValueError:
        parsed_datetime = datetime.strptime(csv_time, "%d/%m/%Y %H:%M")
    parsed_datetime = timezone.localize(parsed_datetime).astimezone(pytz.UTC)
    return parsed_datetime


def is_in_future(timestamp):
    return datetime.now(tz=pytz.UTC) < timestamp


def sduk_csv_sd_parse_header(plu_file_header_line: bytes = None):
    assert plu_file_header_line is not None

    plu_file_header_dict = convert_line_to_dict(
        read_file_to_list(filename="resources/plu_file_headers.csv"),
        plu_file_header_line,
    )

    activation_date_time_str = plu_file_header_dict["PLU_BTCH_ACTIVATION_DATE_TIME"]
    activation_datetime = sduk_csv_parse_timestamp(activation_date_time_str)

    store_id_str = plu_file_header_dict["PLU_BTCH_ACTIVATION_STORE_ID"]
    logger.debug(
        f"Processed PLU header: StoreId: {store_id_str}, Activation-Datetime: {activation_datetime.isoformat()}"
    )

    return (
        store_id_str,
        activation_datetime,
        {k.lower(): v for (k, v) in plu_file_header_dict.items()},
    )


def sduk_csv_pe_parse_header(pe_file_header_line=None):
    assert pe_file_header_line is not None

    pe_file_header_dict = convert_line_to_dict(
        # company,eventNo,comment,region,startDate,endDate
        read_file_to_list(filename="resources/pe_file_headers.csv"),
        pe_file_header_line,
        b",",
    )

    promoType = pe_file_header_dict["comment"].split(" ", maxsplit=1)[1]
    startDate_str = pe_file_header_dict["startDate"]
    endDate_str = pe_file_header_dict["endDate"]
    startDate = sduk_csv_parse_timestamp(startDate_str, time_format="%d.%m.%Y")
    endDate = sduk_csv_parse_timestamp(endDate_str, time_format="%d.%m.%Y") + timedelta(
        hours=23, minutes=59, seconds=59
    )
    logger.debug(
        f"Processed PE header: Activation-Datetime: {startDate.isoformat()} - {endDate.isoformat()}"
    )

    return (startDate, endDate, promoType)


def sduk_csv_sd_parse_items_into_articles(f=None):
    assert f is not None

    plu_item_header_dict = read_file_to_list("resources/plu_item_headers.csv")
    plu_item_dicts = []
    for line in f:
        item_dict = convert_line_to_dict(plu_item_header_dict, line)
        plu_item_dicts.append(item_dict)

    articles = convert_plu_items_to_articles(plu_item_dicts)

    return articles


def sduk_csv_pe0033_parse_items_into_articles(csv_file=None, header_postfix=""):
    assert csv_file is not None

    sduk_header = csv_file.readline()
    item_headers = [
        decode_text(header.strip().replace(b" ", b"_").replace(b"(xx_prom_desc)", b""))
        for header in csv_file.readline().split(b",")
    ]

    if "0" in item_headers:
        item_headers[item_headers.index("0")] = "zero"

    item_headers = [f"{header}{header_postfix}" for header in item_headers]

    pe_articles = {}
    for line in csv_file:
        item_dict = convert_line_to_dict(item_headers, line, seperator=b",")
        item_id = item_dict.pop(f"item{header_postfix}")
        if item_id == "=ROW()":
            continue
        if item_id not in pe_articles:
            pe_articles[item_id] = item_dict
            pe_articles[item_id][f"pe0033_line_count{header_postfix}"] = str(1)
        else:
            if item_dict[f"xx_prom_type{header_postfix}"] in [
                v
                for k, v in pe_articles[item_id].items()
                if k.startswith(f"xx_prom_type{header_postfix}")
            ]:
                continue

            # Update article with additional datafield
            addition_num = (
                int(pe_articles[item_id][f"pe0033_line_count{header_postfix}"]) + 1
            )

            pe_articles[item_id][f"pe0033_line_count{header_postfix}"] = str(
                addition_num
            )
            additional_item_dict = {
                f"{k}__{addition_num}": v for (k, v) in item_dict.items()
            }
            pe_articles[item_id].update(additional_item_dict)
            logger.debug(f"MultiPromo Article {item_id} Variant {addition_num}")
    articles = convert_pe0033_items_to_articles(pe_articles)
    if header_postfix:
        logger.info(f"Debug_Extra: {header_postfix}: {pe_articles.keys()}")
    return articles


if __name__ == "__main__":
    assert not is_in_future(sduk_csv_parse_timestamp("10/04/2022 00:00:21"))
    assert is_in_future(sduk_csv_parse_timestamp("10/04/3022 00:00:21"))

    for dt in [
        "10/04/2024 00:00:21",
        "10/03/2024 00:00:21",
        "10/02/2024 00:00:21",
        "10/03/2024 00:00:21",
        "10/05/2024 00:00:21",
    ]:
        print(sduk_csv_parse_timestamp(dt))
        if is_in_future(sduk_csv_parse_timestamp(dt)):
            print(f"{dt} is in future")
        else:
            print(f"{dt} is in past or present")


def convert_line_to_dict(key_list, line, seperator=b"|"):
    items = line.strip().split(seperator)
    items = [decode_text(item.strip()) for item in items]
    item_dict = dict(zip(key_list, items))
    return item_dict


def read_file_to_list(filename=None):
    """Read the header file and return a list of headers."""
    assert filename is not None
    with open(filename, "r") as file:
        headers = [line.strip() for line in file]
    return headers


def is_store_id_existing(filename="resources/store_ids.csv", store_id=None):
    assert type(store_id) == str
    return store_id in get_existing_store_ids(filename=filename)


def get_existing_store_ids(filename="resources/store_ids.csv"):
    with open(filename, "r") as file:
        return [line.strip() for line in file]


def ean_padding(ean_to_pad):
    source_len = len(ean_to_pad)
    eans = [ean_to_pad]
    if source_len == 8 or source_len == 13:
        return eans
    if source_len < 8:
        eans.append(ean_to_pad.zfill(8))
    if source_len > 8 and source_len < 13:
        eans.append(ean_to_pad.zfill(13))
    return eans


def convert_plu_items_to_articles(plu_item_dicts):
    articles = []
    for plu_item_dict in plu_item_dicts:

        article_id = remove_leading_zeros(plu_item_dict["INTRNL_ID"])
        article_name = plu_item_dict["DSPL_DESCR"]
        article_nfc = f"https://www.superdrug.com/p/{article_id}"
        article_eans = ean_padding(remove_leading_zeros(plu_item_dict["ITM_ID"]))
        article = {
            "articleId": article_id,
            "articleName": article_name,
            "nfcUrl": article_nfc,
            "eans": article_eans,
            "data": plu_item_dict,
        }

        articles.append(article)

    return articles


def convert_pe0033_items_to_articles(pe_articles):
    articles = []
    for article_id in pe_articles:
        # article_name = pe_articles[article_id]["item_desc"]
        article = {
            "articleId": article_id,
            # "articleName": article_name,
            "data": pe_articles[article_id],
        }
        # if "bul_two" in article["data"]:
        #     logger.debug(
        #         f"article_id,o_type,o_desc,bul_two: {article_id},{article['data']['offer_type']},{article['data']['offer_desc']},{article['data']['bul_two']}"
        #     )
        # else:
        #     print(f"NOBUL: {article}")
        articles.append(article)

    return articles


def remove_leading_zeros(text):
    if type(text) == bytes:
        return text.lstrip(b"0")
    else:
        return text.lstrip("0")
