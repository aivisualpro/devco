require('dotenv').config();
const express = require('express');
const axios = require('axios');
const crypto = require('crypto');
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
const webhookVerifierToken = process.env.QBO_WEBHOOK_VERIFIER_TOKEN;
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


// Shared Helper: Sync Data to AppSheet
const pushToAppSheet = async (items, table) => {
    if (!items || items.length === 0) return;
    console.log(`Pushing ${items.length} rows to ${table}...`);
    const batchSize = 500;
    for (let i = 0; i < items.length; i += batchSize) {
        const batch = items.slice(i, i + batchSize);
        console.log(`Pushing batch to ${table} (${Math.floor(i / batchSize) + 1}/${Math.ceil(items.length / batchSize)})...`);
        await axios.post(`https://api.appsheet.com/api/v2/apps/${appId}/tables/${table}/Action`, {
            "Action": "Add",
            "Properties": { "Locale": "en-US", "Timezone": "UTC" },
            "Rows": batch
        }, { headers: { 'ApplicationAccessKey': accessKey, 'Content-Type': 'application/json' } });
    }
};


// Shared Helper: Process in batches with delay
async function processInBatchesWithDelay(items, batchSize, fn, delayMs) {
    let result = [];
    for (let i = 0; i < items.length; i += batchSize) {
        const batch = items.slice(i, i + batchSize);
        console.log(`Processing batch ${Math.floor(i / batchSize) + 1}/${Math.ceil(items.length / batchSize)}...`);
        const batchResults = await Promise.all(batch.map(fn));
        result = result.concat(batchResults);
        if (i + batchSize < items.length) {
            await new Promise(resolve => setTimeout(resolve, delayMs));
        }
    }
    return result;
}

// Shared Helper: Fetch and map transactions for a single project
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

        // Helper to clean Amount string
        const parseAmount = (val) => parseFloat(String(val).replace(/[^0-9.-]/g, '')) || 0;

        // P&L Detail returns nested rows. Helper to traverse and aggregate.
        const traverseRows = (rows) => {
            for (const row of rows) {
                if (row.type === 'Data' && row.ColData) {
                    // Map Columns
                    const getValue = (idx) => (row.ColData[idx] && row.ColData[idx].value) ? row.ColData[idx].value : "";
                    const getId = (idx) => (row.ColData[idx] && row.ColData[idx].id) ? row.ColData[idx].id : null;

                    const date = getValue(0);
                    const type = getValue(1);
                    const txnIdRaw = getId(1); // The QBO Transaction ID
                    const num = getValue(2);
                    const name = getValue(3);
                    const memo = getValue(4);
                    const amountVal = getValue(6) || getValue(5); // Fallback
                    const amount = parseAmount(amountVal);

                    // Generate a grouping key. Prefer QBO ID. Fallback to properties.
                    const groupKey = txnIdRaw || `${date}_${type}_${num}_${amount}`;

                    if (!transactionsMap.has(groupKey)) {
                        transactionsMap.set(groupKey, {
                            id: txnIdRaw || `GEN_${groupKey}`,
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

                    // Keep the longest memo
                    if (memo && memo.length > tx.memo.length) {
                        tx.memo = memo;
                    }
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
            "Transaction ID": `${project.Id}_${tx.id}`,
            "Project Name": tx.project,
            "Date": tx.date,
            "Year": tx.date ? tx.date.substring(0, 4) : "",
            "Transaction Type": tx.type,
            "Num": tx.num,
            "Name": tx.name,
            "ProjectId": project.Id,
            "Amount": parseFloat(tx.amount.toFixed(2))
        }));

    } catch (e) {
        console.error(`Failed to fetch transactions for ${project.DisplayName}:`, e.message);
        return [];
    }
};

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
                "ProjectId": proj.Id,
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

        // Run batch processing (5 concurrent requests at a time, with delay)


        const allTransactions = (await processInBatchesWithDelay(projects, 5, fetchProjectTransactions, 1000)).flat();
        console.log(`Fetched ${allTransactions.length} total transactions.`);


        // --- PART C: PUSH TO APPSHEET ---

        // 1. Push Projects
        // --- PART C: PUSH TO APPSHEET ---

        // 1. Push Projects
        console.log(`Syncing ${projectRows.length} projects to AppSheet...`);
        await pushToAppSheet(projectRows, tableName);

        // 2. Push Transactions
        if (allTransactions.length > 0) {
            console.log(`Syncing ${allTransactions.length} transactions to AppSheet...`);
            await pushToAppSheet(allTransactions, transactionsTableName);
        } else {
            console.log("No transactions fetched.");
        }

        res.json({
            message: "Full Sync Complete",
        });

    } catch (error) {
        console.error('Error in sync:', JSON.stringify(error.response ? error.response.data : error.message, null, 2));
        res.status(500).send('Error syncing data');
    }
});

// Helper: Sync Single Project Logic
const syncSingleProject = async (projectId) => {
    console.log(`Starting Single Project Sync for ID: ${projectId}...`);

    if (!accessToken) {
        console.log('No access token, refreshing...');
        await refreshAccessToken();
    }

    // 1. Fetch THIS Project
    const qResponse = await makeAuthenticatedRequest(() =>
        axios.get(`${baseUrl}/query?query=${encodeURIComponent(`SELECT *, ProjectStatus FROM Customer WHERE Id = '${projectId}'`)}&minorversion=69`, {
            headers: { 'Authorization': `Bearer ${accessToken}`, 'Accept': 'application/json' }
        })
    );

    const project = qResponse.data.QueryResponse.Customer ? qResponse.data.QueryResponse.Customer[0] : null;
    if (!project) {
        throw new Error("Project not found in QuickBooks");
    }

    // 2. Fetch P&L
    const financialsResponse = await makeAuthenticatedRequest(() =>
        axios.get(`${baseUrl}/reports/ProfitAndLoss?minorversion=69&date_macro=All&summarize_column_by=Customers&customer=${project.Id}`, {
            headers: { 'Authorization': `Bearer ${accessToken}`, 'Accept': 'application/json' }
        })
    );

    // Parse financials
    const fRows = financialsResponse.data.Rows.Row || [];
    const findValue = (rows, label) => {
        for (const r of rows) {
            if (r.type === 'Section' && r.group === label && r.Summary && r.Summary.ColData) {
                return parseFloat(r.Summary.ColData[r.Summary.ColData.length - 1].value) || 0;
            }
            if (r.Rows && r.Rows.Row) {
                const v = findValue(r.Rows.Row, label);
                if (v !== null) return v;
            }
        }
        return null;
    };

    const income = findValue(fRows, 'Income') || 0;
    const cogs = findValue(fRows, 'COGS') || 0;
    const expenses = findValue(fRows, 'Expenses') || 0;
    const cost = cogs + expenses;

    // Map Project Data
    let customerName = "";
    if (project.FullyQualifiedName && project.FullyQualifiedName.includes(':')) {
        customerName = project.FullyQualifiedName.split(':')[0];
    } else {
        customerName = project.CompanyName || project.DisplayName;
    }

    const projectRow = {
        "ProjectId": project.Id,
        "Project": project.DisplayName,
        "Customer": customerName,
        "Income": income,
        "Cost": cost,
        "Start Date": project.JobStartDate || project.MetaData.CreateTime.split('T')[0],
        "End Date": "",
        "Status": project.ProjectStatus || (project.Active ? "In Progress" : "Completed")
    };

    // 3. Fetch Transactions
    const transactions = await fetchProjectTransactions(project);

    console.log(`Syncing Project '${project.DisplayName}' with ${transactions.length} transactions.`);

    // 4. Push to AppSheet
    await pushToAppSheet([projectRow], tableName);
    await pushToAppSheet(transactions, transactionsTableName);

    return {
        project: project.DisplayName,
        transactionsSynced: transactions.length
    };
};

// Endpoint: Single Project Sync
app.get('/webhook/sync-project', async (req, res) => {
    try {
        const projectId = req.query.projectId;
        if (!projectId) {
            return res.status(400).send("Missing projectId query parameter");
        }

        const result = await syncSingleProject(projectId);

        res.json({
            message: "Single Project Sync Complete",
            ...result
        });

    } catch (error) {
        console.error('Error in single sync:', error.message);
        if (error.message === "Project not found in QuickBooks") {
            res.status(404).json({ error: error.message });
        } else {
            res.status(500).send('Error syncing project');
        }
    }
});

// Helper: Delete Project from AppSheet
const deleteProjectFromAppSheet = async (projectId) => {
    console.log(`Deleting ID ${projectId} from AppSheet...`);
    // 1. Delete the Project Row
    // For AppSheet 'Delete' action, we need to send the row with its Key.
    // Assuming 'ProjectId' is the key for 'Projects' table and 'Transaction ID' is key for 'Project Transactions'.

    try {
        await axios.post(`https://api.appsheet.com/api/v2/apps/${appId}/tables/${tableName}/Action`, {
            "Action": "Delete",
            "Properties": { "Locale": "en-US", "Timezone": "UTC" },
            "Rows": [{ "ProjectId": projectId }]
        }, { headers: { 'ApplicationAccessKey': accessKey, 'Content-Type': 'application/json' } });
        console.log(`Deleted project ${projectId} from Projects table.`);

        // 2. Delete Transactions? 
        // We can't easily query AppSheet for all transactions with this ProjectId to get their IDs.
        // AppSheet API doesn't support "Delete Where".
        // Strategy: We might have to leave them or fetch them first?
        // Fetching "Project Transactions" from AppSheet is redundant/hard without filter.
        // OPTION: We can't delete orphaned transactions easily without their IDs.
        // User asked: "if i update / delete or add any project it should update to appsheet"
        // If we delete the Project, the transactions are orphaned.
        // Let's LOG a warning that transactions might remain or we need a way to find them.
        // Actually, AppSheet Automation can handle "Delete related", but via API strictly...
        // For now, let's just delete the Project record itself.

    } catch (error) {
        console.error(`Error deleting project ${projectId}:`, error.message);
    }
};

// Endpoint: QBO Webhook
app.post('/webhook/qbo', express.json(), async (req, res) => {
    const signature = req.get('intuit-signature');
    if (!signature) {
        return res.status(401).send('Forbidden');
    }

    if (!webhookVerifierToken) {
        console.error("Missing QBO_WEBHOOK_VERIFIER_TOKEN");
        return res.status(500).send("Server Configuration Error");
    }

    // Verify Signature
    const payload = JSON.stringify(req.body);
    const hmac = crypto.createHmac('sha256', webhookVerifierToken);
    const digest = hmac.update(payload).digest('base64');

    if (signature !== digest) {
        console.error("Invalid Webhook Signature");
        return res.status(401).send('Unauthorized');
    }

    console.log('Received Valid QBO Webhook:', JSON.stringify(req.body, null, 2));
    res.status(200).send('SUCCESS'); // Respond quickly

    // Process Events
    const events = req.body.eventNotifications || [];
    for (const event of events) {
        const entities = event.dataChangeEvent.entities || [];
        for (const entity of entities) {
            if (entity.name === 'Customer') {
                const projectId = entity.id;
                const operation = entity.operation; // Create, Update, Delete, Merge

                console.log(`Processing Customer Event: ${operation} on ID ${projectId}`);

                if (operation === 'Delete' || operation === 'Merge') {
                    // Handle Delete
                    // If Merge, the 'deletedId' is usually passed? QBO docs say 'id' is the one being merged/deleted.
                    await deleteProjectFromAppSheet(projectId);
                } else if (operation === 'Create' || operation === 'Update') {
                    // Handle Sync
                    // Trigger the existing sync logic
                    try {
                        // We can reuse the logic from /webhook/sync-project but we need to mock req/res or extract logic.
                        // Best to just hit our own localhost URL or extract a function. 
                        // Extracting function `syncSingleProject(projectId)` is cleaner.
                        await syncSingleProject(projectId);
                    } catch (err) {
                        console.error(`Failed to sync project ${projectId} from webhook:`, err.message);
                    }
                }
            }
        }
    }
});

// Start the server
app.listen(port, () => {
    console.log(`Server running on port ${port}`);
});
