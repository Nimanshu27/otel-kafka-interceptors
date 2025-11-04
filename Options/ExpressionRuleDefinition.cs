using System.Collections.Generic;

namespace Otel.Kafka.Interceptors.Options;

public sealed class ExpressionRuleDefinition
{
    public string? TypePattern { get; set; }
    public string? MethodPattern { get; set; }
    public bool UseRegex { get; set; }
    public TelemetrySpanKind Kind { get; set; } = TelemetrySpanKind.Internal;
    public IDictionary<string, string>? Tags { get; set; }
}
