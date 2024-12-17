using MongoDB.Driver;
using System;

const string ConnectionString = "mongodb://localhost:27017";
var client = new MongoClient(ConnectionString);
var database = client.GetDatabase("testdb");
var accounts = database.GetCollection<Account>("accounts");

Console.WriteLine("Preparing test environment...");
await SetupTestEnvironment();

Console.WriteLine("Testing transaction isolation level...");
await TestTransactionIsolationLevel();

Console.WriteLine("Test completed.");

async Task SetupTestEnvironment()
{
    // Clear the collection and insert initial test data
    await accounts.DeleteManyAsync(FilterDefinition<Account>.Empty);
    await accounts.InsertManyAsync(new[]
    {
        new Account { Id = 1, Balance = 1000 },
        new Account { Id = 2, Balance = 500 }
    });
    Console.WriteLine("Test environment prepared.");
}

async Task TestTransactionIsolationLevel()
{
    using var session = await client.StartSessionAsync();
    try
    {
        // Start a transaction
        session.StartTransaction();
        Console.WriteLine("Transaction started...");

        // Update Account 1's balance within the transaction
        var updateFilter = Builders<Account>.Filter.Eq(a => a.Id, 1);
        var update = Builders<Account>.Update.Inc(a => a.Balance, -100);
        await accounts.UpdateOneAsync(session, updateFilter, update);
        Console.WriteLine("Updated Account 1 balance within transaction.");

        // Simulate another session attempting to read the data
        using var otherSession = await client.StartSessionAsync();
        var otherAccountData = await accounts
            .Find(otherSession, Builders<Account>.Filter.Eq(a => a.Id, 1))
            .FirstOrDefaultAsync();
        Console.WriteLine($"Other session read (before commit): AccountId=1, Balance={otherAccountData?.Balance}");

        // Commit the transaction
        await session.CommitTransactionAsync();
        Console.WriteLine("Transaction committed.");

        // Read the updated data after the transaction commit
        var committedData = await accounts
            .Find(Builders<Account>.Filter.Eq(a => a.Id, 1))
            .FirstOrDefaultAsync();
        Console.WriteLine($"After commit read: AccountId=1, Balance={committedData?.Balance}");
    }
    catch (Exception ex)
    {
        // Rollback the transaction in case of an error
        Console.WriteLine($"Error during transaction: {ex.Message}");
        await session.AbortTransactionAsync();
    }
}

public class Account
{
    public int Id { get; set; } // Account ID
    public int Balance { get; set; } // Account balance
}