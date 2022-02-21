const parse = require("pg-connection-string").parse;
const { Pool } = require("pg");
const prompt = require("prompt");
const { v4: uuidv4 } = require("uuid");

var accountValues = Array(3);

// Wrapper for a transaction.  This automatically re-calls the operation with
// the client as an argument as long as the database server asks for
// the transaction to be retried.
async function retryTxn(n, max, client, operation, callback) {
  await client.query("BEGIN;");
  while (true) {
    n++;
    if (n === max) {
      throw new Error("Max retry count reached.");
    }
    try {
      await operation(client, callback);
      await client.query("COMMIT;");
      return;
    } catch (err) {
      if (err.code !== "40001") {
        return callback(err);
      } else {
        console.log("Transaction failed. Retrying transaction.");
        console.log(err.message);
        await client.query("ROLLBACK;", () => {
          console.log("Rolling back transaction.");
        });
        await new Promise((r) => setTimeout(r, 2 ** n * 1000));
      }
    }
  }
}

// This function is called within the first transaction. It inserts some initial values into the "accounts" table.
async function initTable(client, callback) {
  let i = 0;
  while (i < accountValues.length) {
    accountValues[i] = await uuidv4();
    i++;
  }

  const insertStatement =
    "INSERT INTO accounts (id, balance) VALUES ($1, 1000), ($2, 250), ($3, 0);";
  await client.query(insertStatement, accountValues, callback);

  const selectBalanceStatement = "SELECT id, balance FROM accounts;";
  await client.query(selectBalanceStatement, callback);
}

// This function updates the values of two rows, simulating a "transfer" of funds.
async function transferFunds(client, callback) {
  const from = accountValues[0];
  const to = accountValues[1];
  const amount = 100;
  const selectFromBalanceStatement =
    "SELECT balance FROM accounts WHERE id = $1;";
  const selectFromValues = [from];
  await client.query(
    selectFromBalanceStatement,
    selectFromValues,
    (err, res) => {
      if (err) {
        return callback(err);
      } else if (res.rows.length === 0) {
        console.log("account not found in table");
        return callback(err);
      }
      var acctBal = res.rows[0].balance;
      if (acctBal < amount) {
        return callback(new Error("insufficient funds"));
      }
    }
  );

  const updateFromBalanceStatement =
    "UPDATE accounts SET balance = balance - $1 WHERE id = $2;";
  const updateFromValues = [amount, from];
  await client.query(updateFromBalanceStatement, updateFromValues, callback);

  const updateToBalanceStatement =
    "UPDATE accounts SET balance = balance + $1 WHERE id = $2;";
  const updateToValues = [amount, to];
  await client.query(updateToBalanceStatement, updateToValues, callback);

  const selectBalanceStatement = "SELECT id, balance FROM accounts;";
  await client.query(selectBalanceStatement, callback);
}

// This function deletes the third row in the accounts table.
async function deleteAccounts(client, callback) {
  const deleteStatement = "DELETE FROM accounts WHERE id = $1;";
  await client.query(deleteStatement, [accountValues[2]], callback);

  const selectBalanceStatement = "SELECT id, balance FROM accounts;";
  await client.query(selectBalanceStatement, callback);
}

// Run the transactions in the connection pool
(async () => {
  prompt.start();
  const URI = await prompt.get("connectionString");
  const connectionString = URI.connectionString.replace(
    // Expand $env:appdata environment variable in Windows connection string
    "$env:appdata",
    process.env.APPDATA
  ).replace(
    // Expand $HOME environment variable in UNIX connection string
    "$HOME",
    process.env.HOME
  );
  var config = parse(connectionString);
  config.port = 26257;
  config.database = "bank";
  const pool = new Pool(config);

  // Connect to database
  const client = await pool.connect();

  // Callback
  function cb(err, res) {
    if (err) throw err;

    if (res.rows.length > 0) {
      console.log("New account balances:");
      res.rows.forEach((row) => {
        console.log(row);
      });
    }
  }

  // Initialize table in transaction retry wrapper
  console.log("Initializing accounts table...");
  await retryTxn(0, 15, client, initTable, cb);

  // Transfer funds in transaction retry wrapper
  console.log("Transferring funds...");
  await retryTxn(0, 15, client, transferFunds, cb);

  // Delete a row in transaction retry wrapper
  console.log("Deleting a row...");
  await retryTxn(0, 15, client, deleteAccounts, cb);

  // Exit program
  process.exit();
})().catch((err) => console.log(err.stack));
