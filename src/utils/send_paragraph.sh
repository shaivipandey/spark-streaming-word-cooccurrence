#!/bin/bash

# File: send_paragraph.sh

# Check if the required arguments are provided
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <port>"
    exit 1
fi

port=$1

# Create a temporary file to store the modified paragraph
temp_file=$(mktemp)

# Split the paragraph into sentences and store them in the temporary file
cat /Users/kritikkumar/Downloads/shaivipa_kritikku_assignment_4/src/setup/paragraph.txt | sed 's/\. /\.\'$'\n/g' > "$temp_file"

# Append a whitespace to the temporary file
echo " " >> "$temp_file"

# Send each sentence with a 1-second delay through netcat
cat "$temp_file" | while read -r sentence; do
    echo "$sentence"
    sleep 1
done | ncat -lk -p "$port"

# Clean up the temporary file
rm "$temp_file"
