# s5table
read only S3 backed sstable implementation


# Run 
gcloud run deploy --image IMAGE --update-env-vars=^~^"GOOGLE_APPLICATION_CREDENTIALS_JSON=$GOOGLE_APPLICATION_CREDENTIALS_JSON" --update-env-vars=SSTABLE_FILE=gs://bucket/file
