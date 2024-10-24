from typing import TypedDict

from airflow.models import Variable

# ---------------------------------------------------------------------------- #
#                                     Names                                    #
# ---------------------------------------------------------------------------- #

LECTORIUM_DATABASE_CONNECTION_STRING = "lectorium::database::connection-string"
LECTORIUM_DATABASE_COLLECTIONS = "lectorium::database::collections"

# ---------------------------------------------------------------------------- #
#                                    Models                                    #
# ---------------------------------------------------------------------------- #

class LectoriumDatabaseCollections(TypedDict):
    transcripts: str

# ---------------------------------------------------------------------------- #
#                                    Default                                   #
# ---------------------------------------------------------------------------- #

Variable.setdefault(
    LECTORIUM_DATABASE_CONNECTION_STRING,
    "",
    "Lectorium database connection string"
)

Variable.setdefault(
   LECTORIUM_DATABASE_COLLECTIONS,
   LectoriumDatabaseCollections(
       transcripts="library-transcripts-v0001"
   ),
   "Database collection names",
   deserialize_json=True
)
