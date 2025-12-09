require('dotenv').config();
const express = require('express');
const axios = require('axios');
const app = express();
const port = process.env.PORT || 3000;

// QuickBooks Credentials
const clientId = process.env.QBO_CLIENT_ID;
const clientSecret = process.env.QBO_CLIENT_SECRET;
const redirectUri = 'https://developer.intuit.com/v2/OAuth2Playground/RedirectUrl'; // Keep default or env if needed, usually ignored for refresh token flow
// Initial Tokens - In a real app these should be in DB, here used for initial boot if needed, but we rely on refresh token from Env
let accessToken = ''; // Will be fetched via refresh
let refreshToken = process.env.QBO_REFRESH_TOKEN;

const realmId = process.env.QBO_REALM_ID;
const baseUrl = `https://quickbooks.api.intuit.com/v3/company/${realmId}`;

// AppSheet Credentials
const appId = process.env.APPSHEET_APP_ID;
const accessKey = process.env.APPSHEET_ACCESS_KEY;
const tableName = process.env.APPSHEET_TABLE_NAME || 'Projects';
const transactionsTableName = process.env.APPSHEET_TX_TABLE_NAME || 'Project Transactions';

// Helper: Get Basic Auth Header
const getAuthHeader = () => {
    const apiKey = `${clientId}:${clientSecret}`;
    return `Basic ${Buffer.from(apiKey).toString('base64')}`;
};

// Helper: Refresh Access Token
const refreshAccessToken = async () => {
    try {
        console.log('Refreshing Access Token...');
        const response = await axios.post('https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer',
            `grant_type=refresh_token&refresh_token=${refreshToken}`,
            {
                headers: {
                    'Accept': 'application/json',
                    'Content-Type': 'application/x-www-form-urlencoded',
                    'Authorization': getAuthHeader()
                }
            }
        );

        accessToken = response.data.access_token;
        refreshToken = response.data.refresh_token; // Rotate refresh token
        console.log('Token Refreshed Successfully!');
        return accessToken;
    } catch (error) {
        console.error('Error refreshing token:', error.response ? error.response.data : error.message);
        throw error;
    }
};

// Helper: Authenticated Request Wrapper
const makeAuthenticatedRequest = async (requestFn) => {
    try {
        return await requestFn();
    } catch (error) {
        if (error.response && error.response.status === 401) {
            console.log('Encountered 401 Error. Attempting to refresh token...');
            await refreshAccessToken();
            // Retry with new token (the caller must use the updated 'accessToken' var)
            console.log('Retrying request with new token...');
            return await requestFn();
        }
        throw error;
    }
};


// Helper: Process array in batches (Rate Limit protection)
async function processInBatches(items, batchSize, fn) {
    let result = [];
    for (let i = 0; i < items.length; i += batchSize) {
        const batch = items.slice(i, i + batchSize);
        console.log(`Processing batch ${Math.floor(i / batchSize) + 1}/${Math.ceil(items.length / batchSize)}...`);
        const batchResults = await Promise.all(batch.map(fn));
        result = result.concat(batchResults);
    }
    return result;
}

// Endpoint to handle the webhook
app.get('/webhook/fetch-projects', async (req, res) => {
    try {
        console.log('Starting sync...');

        // Ensure we have a token (or refresh it)
        if (!accessToken) {
            console.log('No access token, refreshing...');
            await refreshAccessToken();
        }

        // 1. Fetch ALL Projects
        const fetchProjects = () => axios.get(`${baseUrl}/query?query=${encodeURIComponent("SELECT *, ProjectStatus FROM Customer WHERE Job = true AND Active IN (true, false) MAXRESULTS 1000")}&minorversion=69`, {
            headers: { 'Authorization': `Bearer ${accessToken}`, 'Accept': 'application/json' }
        });

        // 2. Fetch P&L Report
        const fetchFinancials = () => axios.get(`${baseUrl}/reports/ProfitAndLoss?minorversion=69&date_macro=All&summarize_column_by=Customers`, {
            headers: { 'Authorization': `Bearer ${accessToken}`, 'Accept': 'application/json' }
        });

        const [projectsResponse, financialsResponse] = await Promise.all([
            makeAuthenticatedRequest(fetchProjects),
            makeAuthenticatedRequest(fetchFinancials)
        ]);

        const allJobs = projectsResponse.data.QueryResponse.Customer || [];
        const projects = allJobs.filter(job => job.IsProject === true);

        const financialCols = financialsResponse.data.Columns.Column;
        const financialRows = financialsResponse.data.Rows.Row;

        console.log(`Fetched ${projects.length} projects.`);

        // --- PART A: SYNC PROJECTS ---
        const getProjectFinancials = (projectName) => {
            let income = 0;
            let cost = 0;
            const colIndex = financialCols.findIndex(col => col.ColTitle === projectName);

            if (colIndex > -1) {
                const findValueInRows = (rows, label) => {
                    for (const row of rows) {
                        if (row.type === 'Section' && row.group === label) {
                            if (row.Summary && row.Summary.ColData && row.Summary.ColData[colIndex]) {
                                return parseFloat(row.Summary.ColData[colIndex].value) || 0;
                            }
                        }
                        if (row.Rows && row.Rows.Row) {
                            const val = findValueInRows(row.Rows.Row, label);
                            if (val !== null) return val;
                        }
                    }
                    return null;
                };
                income = findValueInRows(financialRows, 'Income') || 0;

                const cogs = findValueInRows(financialRows, 'COGS') || 0;
                const expenses = findValueInRows(financialRows, 'Expenses') || 0;
                cost = cogs + expenses;
            }
            return { income, cost };
        };

        const projectRows = projects.map(proj => {
            const financials = getProjectFinancials(proj.DisplayName);

            let customerName = "";
            if (proj.FullyQualifiedName && proj.FullyQualifiedName.includes(':')) {
                customerName = proj.FullyQualifiedName.split(':')[0];
            } else {
                customerName = proj.CompanyName || proj.DisplayName;
            }

            let status = "In Progress";
            if (proj.ProjectStatus) {
                status = proj.ProjectStatus;
            } else {
                status = proj.Active ? "In Progress" : "Completed";
            }

            return {
                "Project": proj.DisplayName,
                "Customer": customerName,
                "Income": financials.income,
                "Cost": financials.cost,
                "Start Date": proj.JobStartDate || proj.MetaData.CreateTime.split('T')[0],
                "End Date": "",
                "Status": status
            };
        });

        // --- PART B: SYNC TRANSACTIONS ---
        console.log("Fetching transactions for all projects (Batched)...");

        // Function to fetch and map transactions for a single project
        const fetchProjectTransactions = async (project) => {
            try {
                // Use ProfitAndLossDetail to get ALL transactions (Invoices, Bills, Expenses, Payroll)
                const reportReq = () => axios.get(`${baseUrl}/reports/ProfitAndLossDetail?minorversion=69&customer=${project.Id}&date_macro=All`, {
                    headers: { 'Authorization': `Bearer ${accessToken}`, 'Accept': 'application/json' }
                });

                const response = await makeAuthenticatedRequest(reportReq);
                const reportData = response.data;

                if (!reportData.Rows || !reportData.Rows.Row) {
                    return [];
                }

                let transactionsMap = new Map();

                // Helper to clean Amount string (remove commas/currency symbols if any, though usually plain number in API)
                const parseAmount = (val) => parseFloat(String(val).replace(/[^0-9.-]/g, '')) || 0;

                // P&L Detail returns nested rows. Helper to traverse and aggregate.
                const traverseRows = (rows) => {
                    for (const row of rows) {
                        if (row.type === 'Data' && row.ColData) {
                            // Map Columns
                            // 0: Date, 1: Type (with ID), 2: Num, 3: Name, 4: Memo, ... Amount
                            const getValue = (idx) => (row.ColData[idx] && row.ColData[idx].value) ? row.ColData[idx].value : "";
                            const getId = (idx) => (row.ColData[idx] && row.ColData[idx].id) ? row.ColData[idx].id : null;

                            const date = getValue(0);
                            const type = getValue(1);
                            const txnIdRaw = getId(1); // The QBO Transaction ID
                            const num = getValue(2);
                            const name = getValue(3);
                            const memo = getValue(4);
                            // Amount is usually last or 2nd to last.
                            // Safest is to find the column that looks like amount? 
                            // Based on debug: Index 6 or 7.
                            // Debug output showed 8 columns. Index 6 and 7 were amounts.
                            // Usually last column is 'Balance', 2nd to last is 'Amount'.
                            // Let's grab value at col 6 (0-indexed).
                            const amountVal = getValue(6) || getValue(5); // Fallback
                            const amount = parseAmount(amountVal);

                            // Generate a grouping key. Prefer QBO ID. Fallback to properties.
                            const groupKey = txnIdRaw || `${date}_${type}_${num}_${amount}`; // Fallback key

                            if (!transactionsMap.has(groupKey)) {
                                transactionsMap.set(groupKey, {
                                    id: txnIdRaw || `GEN_${groupKey}`, // Store QBO ID or Generated
                                    date,
                                    type,
                                    num,
                                    name,
                                    memo,
                                    amount: 0,
                                    project: project.DisplayName
                                });
                            }

                            const tx = transactionsMap.get(groupKey);
                            tx.amount += amount;

                            // Keep the longest memo (assumed to be the header memo vs line item memo like "1% App")
                            if (memo && memo.length > tx.memo.length) {
                                tx.memo = memo;
                            }
                            // Update Num/Name if missing in first row found (rare)
                            if (!tx.num && num) tx.num = num;
                            if (!tx.name && name) tx.name = name;
                        } else if (row.Rows && row.Rows.Row) {
                            traverseRows(row.Rows.Row);
                        }
                    }
                };

                traverseRows(reportData.Rows.Row);

                // Convert Map to Array
                return Array.from(transactionsMap.values()).map((tx, index) => ({
                    "Transaction ID": `${project.Id}_${tx.id}`, // AppsSheet Key: ProjectID_TxnID
                    "Project Name": tx.project,
                    "Date": tx.date,
                    "Transaction Type": tx.type,
                    "Num": tx.num,
                    "Name": tx.name,
                    "Memo": tx.memo,
                    "Amount": parseFloat(tx.amount.toFixed(2)) // Round to 2 decimals
                }));

            } catch (e) {
                console.error(`Failed to fetch transactions for ${project.DisplayName}:`, e.message);
                return [];
            }
        };

        // Run batch processing (5 concurrent requests at a time, with delay)
        async function processInBatchesWithDelay(items, batchSize, fn, delayMs) {
            let result = [];
            for (let i = 0; i < items.length; i += batchSize) {
                const batch = items.slice(i, i + batchSize);
                console.log(`Processing batch ${Math.floor(i / batchSize) + 1}/${Math.ceil(items.length / batchSize)}...`);
                const batchResults = await Promise.all(batch.map(fn));
                result = result.concat(batchResults);
                // Delay to respect rate limits
                if (i + batchSize < items.length) {
                    await new Promise(resolve => setTimeout(resolve, delayMs));
                }
            }
            return result;
        }

        const allTransactions = (await processInBatchesWithDelay(projects, 5, fetchProjectTransactions, 1000)).flat();
        console.log(`Fetched ${allTransactions.length} total transactions.`);


        // --- PART C: PUSH TO APPSHEET ---

        // 1. Push Projects
        console.log(`Syncing ${projectRows.length} projects to AppSheet...`);
        await axios.post(`https://api.appsheet.com/api/v2/apps/${appId}/tables/${tableName}/Action`, {
            "Action": "Add",
            "Properties": { "Locale": "en-US", "Timezone": "UTC" },
            "Rows": projectRows
        }, { headers: { 'ApplicationAccessKey': accessKey, 'Content-Type': 'application/json' } });

        // 2. Push Transactions (Batch push if array is huge, AppSheet limits payload size)
        // Let's push in chunks of 500 rows
        if (allTransactions.length > 0) {
            console.log(`Syncing ${allTransactions.length} transactions to AppSheet...`);
            const txBatchSize = 500;
            for (let i = 0; i < allTransactions.length; i += txBatchSize) {
                const txBatch = allTransactions.slice(i, i + txBatchSize);
                console.log(`Pushing Transaction Batch ${Math.floor(i / txBatchSize) + 1}...`);
                await axios.post(`https://api.appsheet.com/api/v2/apps/${appId}/tables/${transactionsTableName}/Action`, {
                    "Action": "Add",
                    "Properties": { "Locale": "en-US", "Timezone": "UTC" },
                    "Rows": txBatch
                }, { headers: { 'ApplicationAccessKey': accessKey, 'Content-Type': 'application/json' } });
            }
        } else {
            console.log("No transactions fetched (or all failed).");
        }

        res.json({
            message: "Full Sync Complete",
        });

    } catch (error) {
        console.error('Error in sync:', JSON.stringify(error.response ? error.response.data : error.message, null, 2));
        res.status(500).send('Error syncing data');
    }
});

// Start the server
app.listen(port, () => {
    console.log(`Server running on port ${port}`);
});
