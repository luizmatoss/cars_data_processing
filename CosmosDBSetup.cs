using Azure.Cosmos;
using System;
using System.Threading.Tasks;

public class CosmosDBSetup
{
    private static readonly string cosmosDbConnectionString = "CosmosDBConnectionString";
    private static readonly string databaseName = "LeadDatabase";
    private static readonly string containerName = "Leads";
    private static readonly string partitionKeyPath = "/Brand"; // Define partition key path

    private static readonly CosmosClient cosmosClient = new CosmosClient(cosmosDbConnectionString);

    public static async Task CreateDatabaseAndContainerAsync()
    {
        // Create a new Cosmos DB database if it doesn't exist
        CosmosDatabase database = await cosmosClient.CreateDatabaseIfNotExistsAsync(databaseName);
        Console.WriteLine($"Database '{databaseName}' created or already exists.");

        // Create a new container within the database if it doesn't exist
        CosmosContainer container = await database.CreateContainerIfNotExistsAsync(containerName, partitionKeyPath);
        Console.WriteLine($"Container '{containerName}' with partition key '{partitionKeyPath}' created or already exists.");
    }

    public static async Task Main(string[] args)
    {
        try
        {
            await CreateDatabaseAndContainerAsync();
        }
        catch (CosmosException ex)
        {
            Console.WriteLine($"Cosmos DB error: {ex.Message}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error: {ex.Message}");
        }
    }
}
