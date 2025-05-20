using System.Text.Json;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using Microsoft.ApplicationInsights;
using Microsoft.Data.SqlClient;

namespace CdcPublisher
{
    public class PublishAdhCdc
    {
        private readonly TelemetryClient _telemetryClient;
        private readonly ApplicationInsightsLogger _logger;
        public PublishAdhCdc()
        {
            // Initialize TelemetryClient with Application Insights connection string
            var configuration = new Microsoft.ApplicationInsights.Extensibility.TelemetryConfiguration
            {
                ConnectionString = Environment.GetEnvironmentVariable("ApplicationInsightsConnectionString")
            };
            _telemetryClient = new TelemetryClient(configuration);
            _logger = new ApplicationInsightsLogger(_telemetryClient);
        }

        public async Task PublishCdcChanges(string sqlConnectionString, string eventHubConnectionString, CancellationToken cancellationToken)
        {
            await using var producer = new EventHubProducerClient(eventHubConnectionString, "StudentChangeEvents");

            while (!cancellationToken.IsCancellationRequested)
            {
                using var conn = new SqlConnection(sqlConnectionString);
                await conn.OpenAsync(cancellationToken);
                try
                {
                    string tableName = "sif.Student";
                    string cdcTableName = tableName.Replace('.', '_') + "_CT";
                    // Get last processed LSN with validation
                    var lastLsn = await GetLastProcessedLsn(conn, tableName, validate: true, cancellationToken);

                    // Get min and max LSN
                    byte[] fromLsn;
                    if (lastLsn == null)
                    {
                        using var minLsnCmd = new SqlCommand(
                            $"SELECT sys.fn_cdc_get_min_lsn('{cdcTableName}')",
                            conn);
                        var minLsnResult = await minLsnCmd.ExecuteScalarAsync(cancellationToken);
                        fromLsn = minLsnResult as byte[]
                            ?? throw new InvalidOperationException($"Failed to retrieve min LSN from {cdcTableName}");
                    }
                    else
                    {
                        fromLsn = lastLsn;
                    }

                    using var maxLsnCmd = new SqlCommand(
                        $"SELECT sys.fn_cdc_get_max_lsn('{cdcTableName}')",
                        conn);
                    var toLsn = await maxLsnCmd.ExecuteScalarAsync(cancellationToken) as byte[]
                        ?? throw new InvalidOperationException($"Failed to retrieve max LSN from {cdcTableName}");

                    // Query CDC changes
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

                    // Publish changes to Event Hubs
                    if (changes.Any())
                    {
                        var eventDataBatch = await producer.CreateBatchAsync(cancellationToken);
                        foreach (var change in changes)
                        {
                            var eventData = new EventData(JsonSerializer.SerializeToUtf8Bytes(change));
                            eventData.MessageId = change.EventId.ToString();
                            eventDataBatch.TryAdd(eventData);
                        }
                        await producer.SendAsync(eventDataBatch, cancellationToken);
                        await UpdateLastProcessedLsn(conn, cdcTableName, toLsn);
                    }
                }
                catch (InvalidOperationException ex) when (ex.Message.Contains("Invalid LastLSN"))
                {
                    // Log and alert on invalid LSN
                    Console.WriteLine($"Error: {ex.Message}");
                    // Log to Application Insights with table name and additional context
                    await _logger.LogToApplicationInsights(
                        message: ex.Message,
                        tableName: "dbo.Student",
                        additionalProperties: new Dictionary<string, string>
                        {
                        { "ErrorType", "InvalidLSN" },
                        { "Timestamp", DateTime.UtcNow.ToString("o") }
                        });

                    // Optionally notify admins (e.g., via Azure Logic Apps or email)
                    throw; // Re-throw to halt processing
                }
                catch (Exception ex)
                {
                    // Handle other errors (e.g., transient failures)
                    Console.WriteLine($"Error: {ex.Message}");
                    // Log other errors
                    await _logger.LogToApplicationInsights(
                        message: $"Unexpected error: {ex.Message}",
                        tableName: "dbo.Student",
                        additionalProperties: new Dictionary<string, string>
                        {
                        { "ErrorType", "General" },
                        { "StackTrace", ex.StackTrace?.ToString() ?? "N/A" }
                        });

                    // Consider retry logic with Polly
                }

                await Task.Delay(5000, cancellationToken); // Poll every 5 seconds
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
                // Construct the CDC capture instance name (e.g., 'sif_Student')
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