#!/bin/bash
echo "Triggering QuickBooks -> AppSheet Sync..."
echo "This will sync both Projects and Transactions."
echo "Please wait ~1-2 minutes for completion..."
curl http://localhost:3000/webhook/fetch-projects
echo ""
echo "Sync triggered."
