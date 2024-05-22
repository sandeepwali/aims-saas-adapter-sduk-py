from os import getenv
from dotenv import load_dotenv


def strtobool(val):
    """Convert a string representation of truth to true (1) or false (0).
    True values are 'y', 'yes', 't', 'true', 'on', and '1'; false values
    are 'n', 'no', 'f', 'false', 'off', and '0'.  Raises ValueError if
    'val' is anything else.
    """
    val = val.lower()
    if val in ("y", "yes", "t", "true", "on", "1"):
        return True
    elif val in ("n", "no", "f", "false", "off", "0"):
        return False
    else:
        raise ValueError("invalid truth value %r" % (val,))


load_dotenv(override=False)

LOG_LEVEL = getenv(key="LOG_LEVEL", default="WARN").upper()
LOG_LEVEL_AZURE = getenv(key="LOG_LEVEL_AZURE", default="WARN").upper()

AZURE_ACCOUNT_NAME = getenv(key="AZURE_ACCOUNT_NAME")
AZURE_ACCOUNT_KEY = getenv(key="AZURE_ACCOUNT_KEY")
AZURE_ACCOUNT_CONTAINER = getenv(
    key="AZURE_ACCOUNT_CONTAINER", default="aims-saas-adapter-superdrug-uk-test"
)

AIMS_SAAS_URL = getenv(
    key="AIMS_SAAS_URL", default="https://stage00.common.solumesl.com/common"
)
AIMS_SAAS_USERNAME = getenv(key="AIMS_SAAS_USERNAME")
AIMS_SAAS_PASSWORD = getenv(key="AIMS_SAAS_PASSWORD")
AIMS_SAAS_COMPANY = getenv(key="AIMS_SAAS_COMPANY", default="SPD")


# Fail if Azure Env is does not exist
assert AZURE_ACCOUNT_NAME is not None
assert AZURE_ACCOUNT_KEY is not None
# Fail if AIMS SaaS Credentials are not given
assert AIMS_SAAS_USERNAME is not None
assert AIMS_SAAS_PASSWORD is not None


CSV_TZ = getenv(key="CSV_TZ", default=getenv(key="TZ", default="Europe/London"))
CSV_TIMEFORMAT = getenv(
    key="CSV_TIMEFORMAT", default=getenv(key="TIMEFORMAT", default="%d/%m/%Y %H:%M:%S")
)

BLOB_QUEUE_DIR = getenv(key="BLOB_QUEUE_DIR", default="queue")
BLOB_INPUT_DIR = getenv(key="BLOB_INPUT_DIR", default="input")
BLOB_ARCHIVE_DIR = getenv(key="BLOB_ARCHIVE_DIR", default="archive")
BLOB_REJECT_DIR = getenv(key="BLOB_REJECT_DIR", default="reject")
BLOB_ACTIVE_DIR = getenv(key="BLOB_ACTIVE_DIR", default="active")

TIMEOUT = 30

VERIFY_SSL = strtobool(getenv(key="VERIFY_SSL", default="true"))
