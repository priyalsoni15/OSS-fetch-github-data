# This is for taking a back-up of the MongoDB data

#!/bin/bash

# Configuration
DATABASE="decal-db"                # Replace with your database name
USERNAME="oss-nav"                 # Replace with your MongoDB username
PASSWORD="navuser@98"              # Replace with your MongoDB password (special characters will be encoded)
AUTH_DB="admin"                    # Replace with the authentication database (usually 'admin')
OUTPUT_DIR="./mongo_exports"       # Directory to save exported JSON files
MONGOEXPORT_PATH="/opt/homebrew/bin/mongoexport"  # Path to the mongoexport tool
MONGOSH_PATH="/opt/homebrew/bin/mongosh"          # Path to the mongosh tool

# Encode the password for URL safety
ENCODED_PASSWORD=$(python3 -c "import urllib.parse; print(urllib.parse.quote('$PASSWORD'))")

# Create output directory
mkdir -p $OUTPUT_DIR

# Fetch the list of collections using mongosh
collections=$($MONGOSH_PATH --quiet "mongodb://$USERNAME:$ENCODED_PASSWORD@localhost:27017/$DATABASE?authSource=$DATABASE" \
    --eval "db.getCollectionNames().join(' ')")

# Check if collections were fetched
if [[ -z "$collections" ]]; then
    echo "Error: Unable to fetch collections. Check your credentials and database name."
    exit 1
fi

# Export each collection
for collection in $collections; do
    echo "Exporting collection: $collection"
    $MONGOEXPORT_PATH --uri="mongodb://$USERNAME:$ENCODED_PASSWORD@localhost:27017/$DATABASE?authSource=$DATABASE" \
        --collection=$collection --out=$OUTPUT_DIR/$collection.json --jsonArray
done

echo "All collections exported to $OUTPUT_DIR"

