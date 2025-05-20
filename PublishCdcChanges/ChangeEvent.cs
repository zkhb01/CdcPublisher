namespace CdcPublisher
{
    public class ChangeEvent
    {
        public Guid EventId { get; set; } = Guid.NewGuid();
        public required string TableName { get; set; }
        public required string Operation { get; set; }
        public required string EntityId { get; set; }
        public required string ChangeData { get; set; }
        public DateTime Timestamp { get; set; } = DateTime.UtcNow;
    }
}
