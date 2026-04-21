#!/bin/bash
# Deploy backend ke Cloud Run
# Jalankan dari folder backend/

PROJECT=markaswalet-dashboard
REGION=asia-southeast2
SERVICE=markaswalet-api
IMAGE=gcr.io/$PROJECT/$SERVICE

echo "Building image..."
gcloud builds submit --tag $IMAGE .

echo "Deploying to Cloud Run..."
gcloud run deploy $SERVICE \
  --image $IMAGE \
  --region $REGION \
  --platform managed \
  --allow-unauthenticated \
  --add-cloudsql-instances $PROJECT:$REGION:markaswalet-db \
  --set-env-vars DB_USER=markaswalet_app \
  --set-env-vars DB_PASS=Markas2026 \
  --set-env-vars DB_NAME=markaswalet_crm \
  --set-env-vars DB_SOCKET=/cloudsql/$PROJECT:$REGION:markaswalet-db \
  --memory 512Mi \
  --project $PROJECT

echo "Done! API URL:"
gcloud run services describe $SERVICE --region $REGION \
  --format "value(status.url)"
