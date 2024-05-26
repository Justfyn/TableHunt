using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Azure.Data.Tables;
using Microsoft.Extensions.Logging;

namespace TableHunt.BatchTableHelper;

public class BatchTable : IAsyncDisposable
{

    /// <summary>
    /// Max batch size, default 100
    /// </summary>
    private int _BatchSize = 100;
    
    /// <summary>
    /// Table services client
    /// </summary>
    private TableServiceClient _tableServiceClient { get; set; }

    /// <summary>
    /// Queue for batch operations 
    /// </summary>
    /// <returns></returns>
    private ConcurrentQueue<TableTransactionAction> _batchActions = new ConcurrentQueue<TableTransactionAction>();

    /// <summary>
    /// Logger
    /// </summary>
    private ILogger _log { get; set; }
    
    /// <summary>
    /// Table client
    /// </summary>
    private TableClient _tableClient { get; set; }
    
    /// <summary>
    /// Batch Table
    /// </summary>
    public BatchTable(string ConnectionString, string TableName, ILogger logger)
    {
        
        // Establish service client
        var serviceClient = new TableServiceClient(ConnectionString);
        
        // Create required table
        serviceClient.CreateTableIfNotExists("TableName");
        
        // Set vars
        _log = logger;

        _tableClient = new TableClient(ConnectionString, TableName);
    }
    
    /// <summary>
    /// Perform batch upload
    /// </summary>
    /// <returns></returns>
    private async Task<bool> BatchUploadAsync()
    {
        List<TableTransactionAction> BatchTransactions = new List<TableTransactionAction>();
        
        // Used to re-queue transactions that cannot be put in this batch
        // Such as transactions with a row key that is already present in the batch (cannot perform within the same batch)
        
        List<TableTransactionAction> RequeueTransactions = new List<TableTransactionAction>();

        // Take items out of the queue until it's empty or the max batch size hit
        while (!_batchActions.IsEmpty && BatchTransactions.Count < _BatchSize)
        {
            TableTransactionAction dequeued;

            if (_batchActions.TryDequeue(out dequeued))
            {
                // Validate row key is not already in batch transactions
                // Batches cannot contain two transactions for the same partition key and row.
                
                if (BatchTransactions.Any(x =>
                        x.Entity.PartitionKey == dequeued.Entity.PartitionKey &&
                        x.Entity.RowKey == dequeued.Entity.RowKey))
                {
                    // Requeue the transaction for next batch as it is already existing in this batch
                    RequeueTransactions.Add(dequeued);
                }
                else
                {
                    BatchTransactions.Add(dequeued);
                }
                
            }
                
        }

        if (BatchTransactions.Any())
        {
            // Submit the transactions
            _log.LogInformation($"Uploading batch to {_tableClient.Name} of size {BatchTransactions.Count}");
            
            try
            {
                await _tableClient.SubmitTransactionAsync(BatchTransactions);
            }
            catch (TableTransactionFailedException e)
            {
                List<TableTransactionAction> failedBatch = BatchTransactions.ToList();
                
                _log.LogError($"Failed to insert batch transaction in {_tableClient.Name} with partition key {failedBatch[e.FailedTransactionActionIndex.Value].Entity.PartitionKey} row key {failedBatch[e.FailedTransactionActionIndex.Value].Entity.RowKey} {e.Message}");
                
                // Remove the failing item from the batch and requeue rest
                failedBatch.RemoveAt(e.FailedTransactionActionIndex.Value);
                foreach (TableTransactionAction action in failedBatch)
                {
                    _batchActions.Enqueue(action);
                }
            }
        }
        
        // Requeue transactions that need to be moved to another batch
        if (RequeueTransactions.Any())
        {
            foreach(var transaction in RequeueTransactions)
                _batchActions.Enqueue(transaction);
        }

        return true;
    }

    /// <summary>
    /// Enqueue and upload when hits max size
    /// </summary>
    /// <returns></returns>
    public async Task<bool> EnqueueUploadAsync(TableTransactionAction action)
    {
        // Enqueue
        _batchActions.Enqueue(action);
        
        // Run upload if > batch size
        if (_batchActions.Count >= _BatchSize)
            await BatchUploadAsync();

        return true;
    }
    
    /// <summary>
    /// Batch upload and dispose
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (!_batchActions.IsEmpty)
            await BatchUploadAsync();
        
    }
}