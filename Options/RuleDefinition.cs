using System.Collections.Generic;

namespace Otel.Kafka.Interceptors.Options;

public sealed class RuleDefinition
{
    public string? Type { get; set; }
    public IList<string>? Methods { get; set; }
    public TelemetrySpanKind Kind { get; set; } = TelemetrySpanKind.Internal;
    public IDictionary<string, string>? Tags { get; set; }
}
