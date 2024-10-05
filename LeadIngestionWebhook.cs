using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Azure.Cosmos;

public static class IngestLeadsFunction
{
    private static readonly string cosmosDbConnectionString = "YourCosmosDBConnectionString";
    private static readonly string databaseName = "LeadDatabase";
    private static readonly string containerName = "Leads";

    // Initialize Cosmos client
    private static readonly CosmosClient cosmosClient = new CosmosClient(cosmosDbConnectionString);
    private static readonly CosmosContainer container = cosmosClient.GetDatabase(databaseName).GetContainer(containerName);

    [FunctionName("IngestLeads")]
    public static async Task<IActionResult> Run(
        [HttpTrigger(AuthorizationLevel.Function, "post", Route = null)] HttpRequest req,
        ILogger log)
    {
        log.LogInformation("Webhook received a request.");

        // Read the incoming JSON data from the request body
        string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
        dynamic leadData = JsonConvert.DeserializeObject(requestBody);

        // Check if the data is in the correct format
        if (leadData == null)
        {
            return new BadRequestObjectResult("Invalid JSON format");
        }

        try
        {
            // Store data in Cosmos DB
            await StoreLeadDataInCosmosDB(leadData);
            return new OkObjectResult("Lead data received and stored successfully");
        }
        catch (Exception ex)
        {
            log.LogError($"Error storing data in Cosmos DB: {ex.Message}");
            return new StatusCodeResult(StatusCodes.Status500InternalServerError);
        }
    }

    // Method to store the lead data in Cosmos DB
    private static async Task StoreLeadDataInCosmosDB(dynamic leadData)
    {
        // Iterate over each brand in the lead data
        foreach (var brand in leadData)
        {
            // Create a document for each brand in Cosmos DB
            await container.CreateItemAsync(brand, new PartitionKey(brand.Brand.ToString()));
        }
    }
}
