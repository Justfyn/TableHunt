using System;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using TableHunt.BatchTableHelper;

namespace TableHunt;

public static class DailyTimer
{
    [FunctionName("DailyTimer")]
    public static async Task RunAsync([TimerTrigger("0 */5 * * * *")] TimerInfo myTimer, ILogger log)
    {
        await RunQueries(log);
    }

    /// <summary>
    /// Run batch queries
    /// </summary>
    /// <returns></returns>
    public static async Task<Boolean> RunQueries(ILogger log)
    {
        // Loop each query
        foreach (var query in Data.Queries)
        {
            // Generate a batch table uploader for this query
            BatchTable batchTable = new BatchTable(GetStorageConnection(), query.Name, log);
        }
        
        return true;
    }
    
    /// <summary>
    /// Get Storage Connection from App settings
    /// </summary>
    /// <returns></returns>
    public static string GetStorageConnection() => Environment.GetEnvironmentVariable("AzureWebJobsStorage", EnvironmentVariableTarget.Process);

    
}