using Microsoft.Azure.Functions.Worker;
using Microsoft.Data.SqlClient;
using Azure.Messaging.EventHubs.Producer;
using System.Text.Json;
using Azure.Messaging.EventHubs;
using Microsoft.Azure.WebJobs;

namespace CdcPublisher
{
    public class PublishAdhCdcViaAzureFunction
    {
        /*
        Instead of a polling-based publisher, 
        use this Azure Function with a timer trigger or SQL trigger (via CDC integration) to push changes to Event Hubs. 
        This reduces latency and simplifies the publisher.
        */
        private readonly EventHubProducerClient _producer;

        public PublishAdhCdcViaAzureFunction()
        {
            _producer = new EventHubProducerClient(
                Environment.GetEnvironmentVariable("EventHubConnectionString"),
                "StudentChangeEvents");
        }

        [Function("PublishAdhCdc")]
        public async Task Run([TimerTrigger("0 */5 * * * *")] TimerInfo timer, CancellationToken cancellationToken)
        {
            var sqlConnectionString = Environment.GetEnvironmentVariable("SqlConnectionString");
            using var conn = new SqlConnection(sqlConnectionString);
            await conn.OpenAsync(cancellationToken);

            try
            {
                string tableName = "sif.Student";
                string cdcTableName = tableName.Replace(".", "_");

                // Get last processed Log Sequence Number (LSN)
                var lastLogSequenceNumber = await GetLastProcessedLsn(conn, cdcTableName);
                // Get min and max Log Sequence Number (LSN) for the capture instance
                byte[] fromLsn;
                if (lastLogSequenceNumber == null)
                {
                    // Query sys.fn_cdc_get_min_lsn
                    using var minLsnCmd = new SqlCommand(
                        $"SELECT sys.fn_cdc_get_min_lsn('{cdcTableName}')",
                        conn);
                    var minLsnResult = await minLsnCmd.ExecuteScalarAsync(cancellationToken);
                    fromLsn = minLsnResult as byte[] ?? throw new InvalidOperationException($"Failed to retrieve min LSN from {cdcTableName}");
                }
                else
                {
                    fromLsn = lastLogSequenceNumber;
                }

                // Get max Log Sequence Number (LSN)
                using var maxLsnCmd = new SqlCommand(
                    "SELECT sys.fn_cdc_get_max_lsn()",
                    conn);
                var toLsn = await maxLsnCmd.ExecuteScalarAsync(cancellationToken) as byte[]
                    ?? throw new InvalidOperationException("Failed to retrieve max LSN");

                var changes = new List<ChangeEvent>();
                using var cmd = new SqlCommand(
                    $"SELECT * FROM cdc.fn_cdc_get_all_changes_{cdcTableName}(@from_lsn, @to_lsn, 'all')",
                    conn);

                cmd.Parameters.AddWithValue("@from_lsn", fromLsn);
                cmd.Parameters.AddWithValue("@to_lsn", toLsn);
                using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                {
                    var operation = reader.GetInt32(reader.GetOrdinal("__$operation")) switch
                    {
                        1 => "Delete",
                        2 => "Insert",
                        3 => "UpdateBefore",
                        4 => "UpdateAfter",
                        _ => "Unknown"
                    };
                    changes.Add(new ChangeEvent
                    {
                        TableName = "Student",
                        Operation = operation,
                        EntityId = reader.GetInt32(reader.GetOrdinal("StudentId")).ToString(),
                        ChangeData = JsonSerializer.Serialize(new
                        {
                            StudentId = reader.GetInt32(reader.GetOrdinal("StudentId")),
                            Name = reader.GetString(reader.GetOrdinal("Name")),
                            Email = reader.GetString(reader.GetOrdinal("Email"))
                        })
                    });
                }

                if (changes.Any())
                {
                    var eventDataBatch = await _producer.CreateBatchAsync(cancellationToken);
                    foreach (var change in changes)
                    {
                        var eventData = new EventData(JsonSerializer.SerializeToUtf8Bytes(change));
                        eventData.MessageId = change.EventId.ToString();
                        eventDataBatch.TryAdd(eventData);
                    }
                    await _producer.SendAsync(eventDataBatch, cancellationToken);
                    await UpdateLastProcessedLsn(conn, cdcTableName, toLsn);
                }
            }
            catch (InvalidOperationException ex) when (ex.Message.Contains("Invalid LastLSN"))
            {
                // Log and alert on invalid LSN
                Console.WriteLine($"Error: {ex.Message}");
                // Optionally notify admins (e.g., via Azure Monitor or email)
                // Stop processing to avoid data loss
                throw;
            }
            catch (Exception ex)
            {
                // Handle other errors (e.g., transient failures)
                Console.WriteLine($"Error: {ex.Message}");
                // Consider retry logic with Polly
            }
        }

        //common to both Azure Function and without Azure Function methods
        private async Task<byte[]?> GetLastProcessedLsn(
            SqlConnection conn,
            string tableName,
            bool validate = false,
            CancellationToken cancellationToken = default)
        {
            // Retrieve the last processed LSN
            using var cmd = new SqlCommand(
                "SELECT LastLSN FROM cdc.LastProcessedLSN WHERE TableName = @tableName",
                conn);
            cmd.Parameters.AddWithValue("@tableName", tableName);
            var result = await cmd.ExecuteScalarAsync(cancellationToken);
            var lastLsn = result as byte[];

            // If validation is requested and an LSN was retrieved, validate it
            if (validate && lastLsn != null)
            {
                // Construct the CDC capture instance name (e.g., 'dbo_Student')
                var captureInstance = tableName.Replace(".", "_"); // Converts 'sif.Student' to 'sif_Student'

                // Check if the LSN is within the valid range
                using var validLsnCmd = new SqlCommand(
                    "SELECT CASE WHEN @fromLsn >= sys.fn_cdc_get_min_lsn(@captureInstance) " +
                    "AND @fromLsn <= sys.fn_cdc_get_max_lsn() THEN 1 ELSE 0 END",
                    conn);
                validLsnCmd.Parameters.AddWithValue("@fromLsn", lastLsn);
                validLsnCmd.Parameters.AddWithValue("@captureInstance", captureInstance);
                var isValid = (int)await validLsnCmd.ExecuteScalarAsync(cancellationToken) == 1;

                if (!isValid)
                {
                    throw new InvalidOperationException(
                        $"Invalid LastLSN for table {tableName}; potential data loss detected. " +
                        $"Last saved LSN was {lastLsn} and the LSN from {captureInstance} may have been purged or is outside the valid range.");
                }
            }

            return lastLsn;
        }

        //common to both Azure Function and without Azure Function methods
        private async Task UpdateLastProcessedLsn(SqlConnection conn, string tableName, byte[] lsn)
        {
            using var cmd = new SqlCommand(
                "MERGE INTO cdc.LastProcessedLSN AS target " +
                "USING (SELECT @tableName AS TableName, @lsn AS LastLSN, GETUTCDATE() AS LastProcessedTime) AS source " +
                "ON target.TableName = source.TableName " +
                "WHEN MATCHED THEN UPDATE SET LastLSN = source.LastLSN, LastProcessedTime = source.LastProcessedTime " +
                "WHEN NOT MATCHED THEN INSERT (TableName, LastLSN, LastProcessedTime) " +
                "VALUES (source.TableName, source.LastLSN, source.LastProcessedTime);",
                conn);
            cmd.Parameters.AddWithValue("@tableName", tableName);
            cmd.Parameters.AddWithValue("@lsn", lsn);
            await cmd.ExecuteNonQueryAsync();
        }


    }

}