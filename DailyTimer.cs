using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Azure.Data.Tables;
using Azure.Identity;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Microsoft.Graph.Beta;
using Microsoft.Graph.Beta.Security.MicrosoftGraphSecurityRunHuntingQuery;
using TableHunt.BatchTableHelper;
using TableHunt.Models;

namespace TableHunt;

public static class DailyTimer
{
    
    [FunctionName("DailyTimer")]
    public static async Task RunAsync([TimerTrigger("0 3 * * *")] TimerInfo myTimer, ILogger log)
    {
        await RunQueries(log);
    }

    /// <summary>
    /// Run batch queries
    /// </summary>
    /// <returns></returns>
    private static async Task<Boolean> RunQueries(ILogger log)
    {
        
        // Construct the graph client
        GraphServiceClient _graphClient = GetGraphServicesClient();
        
        // Loop each query
        foreach (var query in Data.Queries)
        {
            try
            {
                await RunQuery(_graphClient, query, log);
            }
            catch (Exception e)
            {
                log.LogError($"Failed to run query {query.Name}: {e}");
            }
        }
        
        return true;
    }

    /// <summary>
    /// Run individual query
    /// </summary>
    /// <param name="_graphClient">Graph Client for AH query</param>
    /// <param name="query">DataQuery Object</param>
    /// <param name="log">Logger</param>
    /// <returns></returns>
    private static async Task<Boolean> RunQuery(GraphServiceClient _graphClient, DataQuery query, ILogger log)
    {
        Stopwatch sw = Stopwatch.StartNew();
        log.LogInformation($"Running query {query.Name}");
            
        // Generate a batch table uploader for this query
        BatchTable batchTable = new BatchTable(GetStorageConnection(), query.Name, log);
            
        // Run query and get results
        var requestBody = new RunHuntingQueryPostRequestBody
        {
            Query = query.Query,
        };

        var results = await _graphClient
            .Security
            .MicrosoftGraphSecurityRunHuntingQuery
            .PostAsync(requestBody);

        int RowCounter = 0;
        foreach(var result in results.Results)
        {
            RowCounter++;
                
            // Find the index value
            if (result.AdditionalData.ContainsKey(query.Index))
            {
                
                // Get index
                var Index = result.AdditionalData[query.Index];
                
                // If index is a datetime, then change it to a timestamp
                if (Index is DateTime dtIndex)
                {
                    Index = dtIndex.ToFileTimeUtc();
                }

                TableEntity rowTableEntity =
                    new TableEntity(new TableEntity(query.Name, Index.ToString()));

                // Add each key value pair, excluding odata types
                foreach (var key in result.AdditionalData.Keys)
                {
                    if (!key.EndsWith("@odata.type"))
                    {
                        var value = result.AdditionalData[key];

                        // Azure table forces UTC, convert
                        if (value is DateTime dtValue)
                        {
                            value = DateTime.SpecifyKind(dtValue, DateTimeKind.Utc);
                        }
                    
                        rowTableEntity.Add(key, value);
                    }
                }
                    
                // Add the batch table entry
                await batchTable.EnqueueUploadAsync(new TableTransactionAction(TableTransactionActionType.UpdateReplace,
                    rowTableEntity));

            }
            else
            {
                // Row result does not contain the index value
                log.LogError($"Row {RowCounter} result in query {query.Name} does not contain index column {query.Index}");
            }
                
        }
            
        // Send up lost batch results and disepose;
        await batchTable.DisposeAsync();
            
        log.LogInformation($"Query {query.Name} complete in {sw.Elapsed}");

        return true;

    }
    
    /// <summary>
    /// Get the Graph Client for Tenant
    /// </summary>
    /// <returns></returns>
    private static GraphServiceClient GetGraphServicesClient()
    {
        // Use default azure credential
        var tokenCredential = new DefaultAzureCredential();
        
        // Default graph scope
        var scopes = new[] { "https://graph.microsoft.com/.default" };

        // Return graph services client
        return new GraphServiceClient(tokenCredential, scopes);
    }
    
    /// <summary>
    /// Get Storage Connection from App settings
    /// </summary>
    /// <returns></returns>
    private static string GetStorageConnection() => Environment.GetEnvironmentVariable("AzureWebJobsStorage", EnvironmentVariableTarget.Process);

    
}