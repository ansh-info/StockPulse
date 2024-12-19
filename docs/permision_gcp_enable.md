The `PERMISSION_DENIED` error indicates that the service account `stock-pipeline-sa@stock-data-pipeline-444011.iam.gserviceaccount.com` lacks the necessary permissions to enable services like `pubsub.googleapis.com`, `storage-component.googleapis.com`, and `bigquery.googleapis.com`.

### Steps to Resolve

#### 1. **Verify Your IAM Permissions**

The service account needs the `Service Usage Admin` or `Owner` role to enable services. You can grant these roles through the Google Cloud Console or `gcloud` CLI.

**Using Google Cloud Console**:

- Navigate to **IAM & Admin** > **IAM** in the [Google Cloud Console](https://console.cloud.google.com/).
- Locate the service account `stock-pipeline-sa@stock-data-pipeline-444011.iam.gserviceaccount.com`.
- Click **Edit** and add the `Service Usage Admin` role.

**Using `gcloud` CLI**:
If you have sufficient permissions, run:

```bash
gcloud projects add-iam-policy-binding stock-data-pipeline-444011 \
    --member="serviceAccount:stock-pipeline-sa@stock-data-pipeline-444011.iam.gserviceaccount.com" \
    --role="roles/serviceusage.serviceUsageAdmin"
```

#### 2. **Authenticate with a User Account (Temporarily)**

If you cannot modify the service account's permissions, switch to a user account with sufficient rights.

Run the following to authenticate with your user credentials:

```bash
gcloud auth login
gcloud auth application-default login
```

Retry enabling the services:

```bash
gcloud services enable pubsub.googleapis.com
gcloud services enable storage-component.googleapis.com
gcloud services enable bigquery.googleapis.com
```

#### 3. **Enable APIs Manually via Google Cloud Console**

- Go to the [Google Cloud Console API Library](https://console.cloud.google.com/apis/library).
- Search for the APIs (`Pub/Sub API`, `Cloud Storage`, `BigQuery API`).
- Click **Enable** for each service.

#### 4. **Ensure the Correct Service Account Key**

Verify that the `service_account.json` file is correct and corresponds to the intended service account. Re-download it from the Google Cloud Console if necessary:

- Navigate to **IAM & Admin** > **Service Accounts**.
- Locate the service account and click **Manage Keys**.
- Download a new JSON key.

#### 5. **Test Permissions**

After updating the permissions, confirm that the service account can enable services:

```bash
gcloud auth activate-service-account --key-file=/workspace/keys/service_account.json
gcloud services enable pubsub.googleapis.com
```

#### 6. **Check Billing**

Ensure that billing is enabled for the project. Service enablement may fail if billing is not configured.

By following these steps, the issue should be resolved, and you’ll be able to enable the required APIs. Let me know if you encounter further problems!
