"""
Application configuration.

Inputs: Environment variables (with defaults for development).
Outputs: Settings singleton with paths, DB credentials, compiled regexes.

Loads settings from environment variables with sensible defaults for
running on ibss-central. Compiles the CASIZ number regexes once at import time.
"""

import regex
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """
    All configuration for the IZ Ingest Status tool.
    Override any field via environment variable (e.g. IZ_SPECIFY_DB_PASSWORD).
    """

    # -- Scan folder --
    scan_root: str = (
        "/letter_drives/n_drive/izg/"
        "iz images_curated for cas sci computing ingest"
    )

    # -- Specify database (READ-ONLY) --
    specify_db_host: str = "10.1.10.196"
    specify_db_port: int = 3306
    specify_db_name: str = "casiz"
    specify_db_user: str = "jfong"
    iz_specify_db_password: str = "J0nd@vid"

    # -- SQLite cache --
    sqlite_path: str = "/data/iz_status.db"

    # -- Server --
    host: str = "0.0.0.0"
    port: int = 8000

    model_config = {"env_prefix": ""}


# -- Compiled regexes (ported from iz_config.py) --

# Extension filter: matches valid image file extensions
IMAGE_EXTENSION = r"(\.(jpg|jpeg|tiff|tif|png|dng|pdf))$"
IMAGE_SUFFIX = rf"[a-z\-\(\)0-9 ©_,.]*{IMAGE_EXTENSION}"
IMAGE_PATH_REGEX = regex.compile(rf"^.*{IMAGE_SUFFIX}", regex.IGNORECASE)

# CASIZ number extraction constants
MINIMUM_ID_DIGITS_WITH_PREFIX = 3
MAXIMUM_ID_DIGITS = 12
MINIMUM_ID_DIGITS = 5

# Main CASIZ number regex (from iz_config.py CASIZ_NUMBER_REGEX).
# Uses the `regex` library for conditional patterns.
# Handles prefixes CASIZ/CAS, excludes camera serials (DSC/P),
# excludes date patterns (YYYYMMDD), suppresses IZACC matches.
CASIZ_NUMBER_REGEX = regex.compile(
    r"""
    (?ix)                           # Ignore case, allow comments
    (?<!\w)                         # No word character before
    (
      (?:
        (?!IZACC[\s_#-]?)             # Not IZACC prefix (negative lookahead)
        (?P<prefix>CASIZ|CAS)        # CASIZ or CAS (named group 'prefix')
        (?:[\s_#-]*)                  # Spaces, underscores, dashes (zero or more)
      )?
      (?P<number>                    # --- Capture only the number ---
        (?!
          (?:DSC|P)\d{{{min_digits_with_prefix},}}     # Not camera serials
        )
        (?!
          (?<!CASIZ[\s_#-]*|CAS[\s_#-]*)
          (?:(?:19|20)\d{{2}}(?:0[1-9]|1[0-2])(?:0[1-9]|[12]\d|3[01]))
        )
        \d{{{min_digits_with_prefix},{max_digits}}}
      )
    )
    (?(prefix)
        (?=\D|$)
    |
        (?=\b|[_\s#-]|$)
    )
    """.format(
        min_digits_with_prefix=MINIMUM_ID_DIGITS_WITH_PREFIX,
        max_digits=MAXIMUM_ID_DIGITS,
    ),
    regex.VERBOSE,
)

# Fallback regex for simpler CASIZ/CAS patterns
CASIZ_FALLBACK_REGEX = regex.compile(
    r"(?i)(?:CASIZ|CAS)[\s_#-]*(\d{{{min_digits},{max_digits}}})(?!\d)".format(
        min_digits=MINIMUM_ID_DIGITS_WITH_PREFIX,
        max_digits=MAXIMUM_ID_DIGITS,
    )
)

# key.csv column mappings (lowercase CSV key -> normalized dict key)
KEY_CSV_COLUMN_MAPPINGS = {
    "copyrightdate": "CopyrightDate",
    "copyrightholder": "CopyrightHolder",
    "credit": "Credit",
    "license": "License",
    "remarks": "Remarks",
    "ispublic": "IsPublic",
    "subtype": "subType",
    "createdbyagent": "createdByAgent",
    "metadatatext": "creator",
    "remove": "remove",
    "erase_exif_fields": "erase_exif_fields",
}

# Truthy values for the "remove" field in key.csv
TRUTHY_VALUES = {"true", "1", "yes"}


def get_settings() -> Settings:
    """
    Return a Settings instance. Call once at startup; cache the result.

    Inputs: None (reads from environment).
    Outputs: Settings object with all configuration values.
    """
    return Settings()
