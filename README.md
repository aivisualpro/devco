# qbfetchall - QuickBooks to AppSheet Sync

This service synchronizes Project and Transaction data from QuickBooks Online to AppSheet tables.

## Features
- **Project Sync**: Fetches projects (Customers) from QBO and syncs to `Projects` table.
- **Transaction Sync**: Fetches aggregated transactions (Invoices, Bills, Expenses) from `ProfitAndLossDetail` report and syncs to `Project Transactions` table.
- **Smart Aggregation**: Groups split lines into single transaction rows.
- **Auto-Refresh**: Handles OAuth2 token refreshing automatically.

## Setup

1.  **Clone the repository**.
2.  **Install dependencies**:
    ```bash
    npm install
    ```
3.  **Configure Environment**:
    -   Copy `.env.example` to `.env`.
    -   Fill in your QuickBooks and AppSheet credentials in `.env`.
    ```bash
    cp .env.example .env
    # Edit .env
    ```

## Usage

**Start the Server**:
```bash
npm start
```

**Trigger Sync**:
Navigate to OR trigger via webhook:
`http://localhost:3000/webhook/fetch-projects`

## Deployment (Render)

1.  Push to GitHub.
2.  Create a new **Web Service** on Render connected to your repo.
3.  Set **Build Command**: `npm install`
4.  Set **Start Command**: `node server.js`
5.  **Environment Variables**: Add all variables from your `.env` file to the Render dashboard.
    -   `QBO_CLIENT_ID`
    -   `QBO_CLIENT_SECRET`
    -   `QBO_REFRESH_TOKEN`
    -   `QBO_REALM_ID`
    -   `APPSHEET_APP_ID`
    -   `APPSHEET_ACCESS_KEY`

## License
Private
