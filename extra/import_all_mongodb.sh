# This is for importing your collection to MongoDB

# Configuration
DATABASE="decal-db"          # Replace with your target database name
USERNAME="priyalsoniwritings"               # Replace with your MongoDB username
PASSWORD="FL3YyVGCr79xlPT0"               # Replace with your MongoDB password
AUTH_DB="decal-db"                     # Replace with the authentication database (e.g., 'admin')
DIRECTORY="/root/mongo_exports"  # Directory containing the exported JSON files # Adjust if necessary

# Encode username and password for URL safety
ENCODED_USERNAME=$(python3 -c "import urllib.parse; print(urllib.parse.quote('$USERNAME'))")
ENCODED_PASSWORD=$(python3 -c "import urllib.parse; print(urllib.parse.quote('$PASSWORD'))")

# Navigate to the directory
cd "$DIRECTORY" || { echo "Directory $DIRECTORY not found!"; exit 1; }

# Import all JSON files
for file in *.json; do
    collection_name=$(basename "$file" .json)
    echo "Importing $file into collection $collection_name..."
    mongoimport --uri="mongodb://$ENCODED_USERNAME:$ENCODED_PASSWORD@localhost:27017/$DATABASE?authSource=$AUTH_DB" \
        --collection="$collection_name" --file="$file" --jsonArray
done

echo "All collections imported successfully!"