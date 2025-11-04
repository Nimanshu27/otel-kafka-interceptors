using System.Collections.Generic;

namespace Otel.Kafka.Interceptors.Options;

/// <summary>
/// Root configuration options for the OpenTelemetry interceptor library.
/// </summary>
public sealed class OtelKafkaInterceptorOptions
{
    /// <summary>
    /// Name supplied to <see cref="System.Diagnostics.ActivitySource"/>.
    /// </summary>
    public string ActivitySourceName { get; set; } = "Otel.Kafka.Interceptors";

    /// <summary>
    /// Kafka specific configuration.
    /// </summary>
    public KafkaOptions Kafka { get; set; } = new();

    /// <summary>
    /// Exact type/method rules applied first.
    /// </summary>
    public IList<RuleDefinition> Rules { get; set; } = new List<RuleDefinition>();

    /// <summary>
    /// Pattern based rules evaluated after exact matches.
    /// </summary>
    public IList<ExpressionRuleDefinition> Expressions { get; set; } = new List<ExpressionRuleDefinition>();


}

/// <summary>
/// Options controlling Kafka specific propagation behaviour.
/// </summary>
public sealed class KafkaOptions
{
    public bool InjectOnProducer { get; set; } = true;
    public bool ExtractOnConsumer { get; set; } = true;
    public string? EnvelopeArgumentType { get; set; }

    public string? ConsumeResultArgumentType { get; set; }
}

