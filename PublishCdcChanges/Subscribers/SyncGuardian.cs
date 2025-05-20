using System.Text.Json;
using Azure.Messaging.EventHubs;
using Azure.Storage.Blobs;
using CdcPublisher.Pocos;
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.DataContracts;
using Oracle.ManagedDataAccess.Client;

namespace CdcPublisher.Subscribers
{
    public class SyncGuardian
    {
        private readonly TelemetryClient _telemetryClient;
        private readonly string _oracleConnectionString;
        private readonly string _eventHubConnectionString;
        private readonly string _blobConnectionString;
        private readonly string _consumerGroup = "GuardianSyncConsumer";

        public SyncGuardian(
            string oracleConnectionString,
            string eventHubConnectionString,
            string blobConnectionString,
            TelemetryClient telemetryClient)
        {
            _oracleConnectionString = oracleConnectionString ?? throw new ArgumentNullException(nameof(oracleConnectionString));
            _eventHubConnectionString = eventHubConnectionString ?? throw new ArgumentNullException(nameof(eventHubConnectionString));
            _blobConnectionString = blobConnectionString ?? throw new ArgumentNullException(nameof(blobConnectionString));
            _telemetryClient = telemetryClient ?? throw new ArgumentNullException(nameof(telemetryClient));
        }

        public async Task ConsumeEventsAsync(CancellationToken cancellationToken)
        {
            var blobContainerClient = new BlobContainerClient(_blobConnectionString, "checkpoints");
            await blobContainerClient.CreateIfNotExistsAsync(cancellationToken: cancellationToken);

            var processor = new EventProcessorClient(
                blobContainerClient,
                _consumerGroup,
                _eventHubConnectionString,
                "StudentChangeEvents");

            // Track processed EventIds to ensure idempotency
            var processedEventIds = new HashSet<Guid>(); // In-memory cache; consider persistent storage for production

            // Handle events
            processor.ProcessEventAsync += async args =>
            {
                var eventData = args.Data;
                try
                {
                    var change = JsonSerializer.Deserialize<ChangeEvent>(eventData.Body.ToArray());
                    if (change == null || processedEventIds.Contains(change.EventId))
                    {
                        return; // Skip null or duplicate events
                    }

                    // Process the event
                    await ProcessChangeEventAsync(change, args.CancellationToken);

                    // Mark event as processed
                    processedEventIds.Add(change.EventId);

                    // Checkpoint progress
                    await args.UpdateCheckpointAsync(args.CancellationToken);

                    // Log success
                    _telemetryClient.TrackEvent(new EventTelemetry("EventProcessed")
                    {
                        Properties = { { "EventId", change.EventId.ToString() }, { "Operation", change.Operation } }
                    });
                }
                catch (Exception ex)
                {
                    // Log error to Application Insights
                    _telemetryClient.TrackException(new ExceptionTelemetry(ex)
                    {
                        SeverityLevel = SeverityLevel.Error,
                        Properties = { { "EventId", eventData.MessageId }, { "Partition", args.Partition.PartitionId } }
                    });
                    _telemetryClient.Flush();

                    // Send to dead-letter queue
                    await SendToDeadLetterQueue(eventData, ex.Message, args.CancellationToken);

                    // Continue processing; consider halting for critical errors
                }
            };

            // Handle errors
            processor.ProcessErrorAsync += args =>
            {
                _telemetryClient.TrackException(new ExceptionTelemetry(args.Exception)
                {
                    SeverityLevel = SeverityLevel.Error,
                    Properties = { { "Partition", args.PartitionId }, { "Operation", args.Operation } }

//                    Properties = { { "Partition", args.Partition.PartitionId }, { "Operation", args.Operation } }
                });
                _telemetryClient.Flush();
                return Task.CompletedTask;
            };

            // Start processing
            await processor.StartProcessingAsync(cancellationToken);

            try
            {
                // Keep running until cancellation
                await Task.Delay(Timeout.Infinite, cancellationToken);
            }
            finally
            {
                // Stop processing gracefully
                await processor.StopProcessingAsync(cancellationToken);
            }
        }

        private async Task ProcessChangeEventAsync(ChangeEvent change, CancellationToken cancellationToken)
        {
            var studentData = JsonSerializer.Deserialize<StudentData>(change.ChangeData);
            if (studentData == null)
            {
                throw new InvalidOperationException("Failed to deserialize ChangeData");
            }

            using var conn = new OracleConnection(_oracleConnectionString);
            await conn.OpenAsync(cancellationToken);

            switch (change.Operation)
            {
                case "Insert":
                case "UpdateAfter":
                    await UpsertPersonInfoAsync(conn, studentData, change, cancellationToken);
                    break;

                case "Delete":
                    await DeletePersonInfoAsync(conn, studentData.StudentId, cancellationToken);
                    break;

                case "UpdateBefore":
                    // Skip UpdateBefore; handle only UpdateAfter
                    break;

                default:
                    throw new InvalidOperationException($"Unknown operation: {change.Operation}");
            }
        }

        private async Task UpsertPersonInfoAsync(
            OracleConnection conn,
            StudentData studentData,
            ChangeEvent change,
            CancellationToken cancellationToken)
        {
            if (change.Operation == "UpdateAfter")
            {
                var updateMask = GetUpdateMask(change);
                var updatedColumns = ParseUpdateMask(updateMask, new[] { "StudentId", "Name", "Email" });

                if (updatedColumns.Any())
                {
                    var setClause = string.Join(", ", updatedColumns.Select(col =>
                    {
                        return col switch
                        {
                            "StudentId" => "StudentId = :StudentId",
                            "Name" => "Name = :Name",
                            "Email" => "Email = :Email",
                            _ => throw new InvalidOperationException($"Unknown column: {col}")
                        };
                    }));

                    var sql = $"UPDATE PersonInfo SET {setClause} WHERE StudentId = :StudentId";
                    using var cmd = new OracleCommand(sql, conn);
                    cmd.Parameters.Add(new OracleParameter("StudentId", studentData.StudentId));
                    foreach (var col in updatedColumns)
                    {
                        if (col == "Name") cmd.Parameters.Add(new OracleParameter("Name", studentData.Name));
                        if (col == "Email") cmd.Parameters.Add(new OracleParameter("Email", studentData.Email));
                    }

                    var rowsAffected = await cmd.ExecuteNonQueryAsync(cancellationToken);
                    if (rowsAffected == 0)
                    {
                        await InsertPersonInfoAsync(conn, studentData, cancellationToken);
                    }
                    return;
                }
            }

            await InsertPersonInfoAsync(conn, studentData, cancellationToken);
        }

        private async Task InsertPersonInfoAsync(OracleConnection conn, StudentData studentData, CancellationToken cancellationToken)
        {
            const string sql = @"
            MERGE INTO PersonInfo dst
            USING (SELECT :StudentId AS StudentId, :Name AS Name, :Email AS Email FROM dual) src
            ON (dst.StudentId = src.StudentId)
            WHEN MATCHED THEN
                UPDATE SET Name = src.Name, Email = src.Email
            WHEN NOT MATCHED THEN
                INSERT (StudentId, Name, Email)
                VALUES (src.StudentId, src.Name, src.Email)";

            using var cmd = new OracleCommand(sql, conn);
            cmd.Parameters.Add(new OracleParameter("StudentId", studentData.StudentId));
            cmd.Parameters.Add(new OracleParameter("Name", studentData.Name));
            cmd.Parameters.Add(new OracleParameter("Email", studentData.Email));

            await cmd.ExecuteNonQueryAsync(cancellationToken);
        }

        private async Task DeletePersonInfoAsync(OracleConnection conn, int studentId, CancellationToken cancellationToken)
        {
            const string sql = "DELETE FROM PersonInfo WHERE StudentId = :StudentId";
            using var cmd = new OracleCommand(sql, conn);
            cmd.Parameters.Add(new OracleParameter("StudentId", studentId));
            await cmd.ExecuteNonQueryAsync(cancellationToken);
        }

        private byte[] GetUpdateMask(ChangeEvent change)
        {
            var changeDataDict = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(change.ChangeData);
            if (changeDataDict.TryGetValue("__$update_mask", out var maskElement))
            {
                return Convert.FromHexString(maskElement.GetString().Replace("0x", ""));
            }
            return Array.Empty<byte>();
        }

        private List<string> ParseUpdateMask(byte[] updateMask, string[] columnNames)
        {
            var updatedColumns = new List<string>();
            if (updateMask.Length == 0) return updatedColumns;

            var bits = new bool[updateMask.Length * 8];
            for (int i = 0; i < updateMask.Length; i++)
            {
                for (int j = 0; j < 8; j++)
                {
                    bits[i * 8 + j] = (updateMask[i] & (1 << j)) != 0;
                }
            }

            for (int i = 0; i < columnNames.Length && i < bits.Length; i++)
            {
                if (bits[i])
                {
                    updatedColumns.Add(columnNames[i]);
                }
            }

            return updatedColumns;
        }

        private async Task SendToDeadLetterQueue(EventData eventData, string errorMessage, CancellationToken cancellationToken)
        {
            var blobClient = new BlobClient(_blobConnectionString, "deadletter", $"{Guid.NewGuid()}.json");
            var errorData = new
            {
                EventId = eventData.MessageId,
                ErrorMessage = errorMessage,
                EventBody = Convert.ToBase64String(eventData.Body.ToArray())
            };
            await blobClient.UploadAsync(
                new BinaryData(JsonSerializer.Serialize(errorData)),
                overwrite: true,
                cancellationToken: cancellationToken);
        }
    }
}


