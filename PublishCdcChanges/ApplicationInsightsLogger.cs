using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.DataContracts;

namespace CdcPublisher
{


    public class ApplicationInsightsLogger
    {
        private readonly TelemetryClient _telemetryClient;

        public ApplicationInsightsLogger(TelemetryClient telemetryClient)
        {
            _telemetryClient = telemetryClient ?? throw new ArgumentNullException(nameof(telemetryClient));
        }

        public async Task LogToApplicationInsights(
            string message,
            string tableName,
            Dictionary<string, string>? additionalProperties = null)
        {
            try
            {
                // Create a telemetry event
                var telemetry = new EventTelemetry("PublisherError")
                {
                    Timestamp = DateTimeOffset.UtcNow
                };

                // Set the message as a property
                telemetry.Properties["Message"] = message;

                // Add table name if provided
                if (!string.IsNullOrEmpty(tableName))
                {
                    telemetry.Properties["TableName"] = tableName;
                }

                // Add severity as a custom property (since EventTelemetry doesn't support SeverityLevel)
                telemetry.Properties["Severity"] = "Error";

                // Add additional properties if provided
                if (additionalProperties != null)
                {
                    foreach (var prop in additionalProperties)
                    {
                        telemetry.Properties[prop.Key] = prop.Value;
                    }
                }

                // Track the event
                _telemetryClient.TrackEvent(telemetry);

                // Log as an exception with SeverityLevel for more detailed tracking
                var exception = new Exception(message);
                var exceptionTelemetry = new ExceptionTelemetry(exception)
                {
                    SeverityLevel = SeverityLevel.Error
                };

                // Copy properties to exception telemetry
                exceptionTelemetry.Properties["TableName"] = tableName;
                exceptionTelemetry.Properties["Severity"] = "Error";
                if (additionalProperties != null)
                {
                    foreach (var prop in additionalProperties)
                    {
                        exceptionTelemetry.Properties[prop.Key] = prop.Value;
                    }
                }

                _telemetryClient.TrackException(exceptionTelemetry);

                // Flush to ensure telemetry is sent immediately (optional, for critical errors)
                _telemetryClient.Flush();

                // Allow async flush to complete
                await Task.Delay(100); // Small delay to ensure flush completes
            }
            catch (Exception ex)
            {
                // Handle telemetry logging failure (e.g., log to console or fallback)
                Console.WriteLine($"Failed to log to Application Insights: {ex.Message}");
            }
        }
    }
}