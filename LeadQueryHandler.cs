using Azure.Cosmos;
using System;
using System.Threading.Tasks;

public class CosmosDBQueryExample
{
    private static readonly string cosmosDbConnectionString = "YourCosmosDBConnectionString";
    private static readonly string databaseName = "LeadDatabase";
    private static readonly string containerName = "Leads";

    private static readonly CosmosClient cosmosClient = new CosmosClient(cosmosDbConnectionString);
    private static readonly CosmosContainer container = cosmosClient.GetDatabase(databaseName).GetContainer(containerName);

    public static async Task QueryLeadsAsync()
    {
        // Query 1: Retrieve All Leads for a Specific Brand
        await GetLeadsByBrand("BMW");

        // Query 2: Retrieve Models Introduced After a Specific Date
        await GetModelsIntroducedAfterDate("2020-01-01");

        // Query 3: Retrieve All Currently Available Models (Not Discontinued)
        await GetCurrentAvailableModels();

        // Query 4: Retrieve All Derivatives of a Specific Model
        await GetDerivativesByModel("Series 3");

        // Query 5: Count Leads by Brand
        await CountLeadsByBrand();

        // Query 6: Retrieve Leads by Luxury Brands Only
        await GetLeadsByLuxuryBrands(new string[] { "BMW", "Mercedes-Benz", "Porsche", "Audi" });
    }

    // Query 1: Retrieve All Leads for a Specific Brand
    private static async Task GetLeadsByBrand(string brand)
    {
        string queryText = "SELECT * FROM c WHERE c.Brand = @brand";
        var queryDefinition = new QueryDefinition(queryText).WithParameter("@brand", brand);
        
        var queryIterator = container.GetItemQueryIterator<dynamic>(queryDefinition);
        
        Console.WriteLine($"Leads for Brand: {brand}");
        while (queryIterator.HasMoreResults)
        {
            foreach (var lead in await queryIterator.ReadNextAsync())
            {
                Console.WriteLine(lead);
            }
        }
    }

    // Query 2: Retrieve Models Introduced After a Specific Date
    private static async Task GetModelsIntroducedAfterDate(string date)
    {
        string queryText = "SELECT c.Brand, m.Model, d.Derivative, d.VehicleInfo.Introduced " +
                           "FROM c JOIN m IN c.Models JOIN d IN m.Derivatives " +
                           "WHERE d.VehicleInfo.Introduced > @introducedDate";
        var queryDefinition = new QueryDefinition(queryText).WithParameter("@introducedDate", date);
        
        var queryIterator = container.GetItemQueryIterator<dynamic>(queryDefinition);
        
        Console.WriteLine($"Models Introduced After {date}");
        while (queryIterator.HasMoreResults)
        {
            foreach (var lead in await queryIterator.ReadNextAsync())
            {
                Console.WriteLine(lead);
            }
        }
    }

    // Query 3: Retrieve All Currently Available Models (Not Discontinued)
    private static async Task GetCurrentAvailableModels()
    {
        string queryText = "SELECT c.Brand, m.Model, d.Derivative " +
                           "FROM c JOIN m IN c.Models JOIN d IN m.Derivatives " +
                           "WHERE d.VehicleInfo.Discontinued = null";
        var queryDefinition = new QueryDefinition(queryText);
        
        var queryIterator = container.GetItemQueryIterator<dynamic>(queryDefinition);
        
        Console.WriteLine("Currently Available Models (Not Discontinued)");
        while (queryIterator.HasMoreResults)
        {
            foreach (var lead in await queryIterator.ReadNextAsync())
            {
                Console.WriteLine(lead);
            }
        }
    }

    // Query 4: Retrieve All Derivatives of a Specific Model
    private static async Task GetDerivativesByModel(string model)
    {
        string queryText = "SELECT c.Brand, m.Model, d.Derivative " +
                           "FROM c JOIN m IN c.Models JOIN d IN m.Derivatives " +
                           "WHERE m.Model = @model";
        var queryDefinition = new QueryDefinition(queryText).WithParameter("@model", model);
        
        var queryIterator = container.GetItemQueryIterator<dynamic>(queryDefinition);
        
        Console.WriteLine($"Derivatives for Model: {model}");
        while (queryIterator.HasMoreResults)
        {
            foreach (var lead in await queryIterator.ReadNextAsync())
            {
                Console.WriteLine(lead);
            }
        }
    }

    // Query 5: Count Leads by Brand
    private static async Task CountLeadsByBrand()
    {
        string queryText = "SELECT c.Brand, COUNT(1) AS LeadCount " +
                           "FROM c GROUP BY c.Brand";
        var queryDefinition = new QueryDefinition(queryText);
        
        var queryIterator = container.GetItemQueryIterator<dynamic>(queryDefinition);
        
        Console.WriteLine("Count of Leads by Brand");
        while (queryIterator.HasMoreResults)
        {
            foreach (var lead in await queryIterator.ReadNextAsync())
            {
                Console.WriteLine($"Brand: {lead.Brand}, Count: {lead.LeadCount}");
            }
        }
    }

    // Query 6: Retrieve Leads by Luxury Brands Only
    private static async Task GetLeadsByLuxuryBrands(string[] luxuryBrands)
    {
        string brandsFilter = string.Join(",", luxuryBrands.Select(b => $"'{b}'"));
        string queryText = $"SELECT * FROM c WHERE c.Brand IN ({brandsFilter})";
        var queryDefinition = new QueryDefinition(queryText);
        
        var queryIterator = container.GetItemQueryIterator<dynamic>(queryDefinition);
        
        Console.WriteLine("Leads for Luxury Brands");
        while (queryIterator.HasMoreResults)
        {
            foreach (var lead in await queryIterator.ReadNextAsync())
            {
                Console.WriteLine(lead);
            }
        }
    }
}
