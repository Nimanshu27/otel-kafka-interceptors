using System.Collections.Generic;
using System.Diagnostics;

namespace Otel.Kafka.Interceptors.Rules;

public sealed class RuleMatch
{
    public RuleMatch(ActivityKind kind, IReadOnlyDictionary<string, string>? tags)
    {
        Kind = kind;
        Tags = tags ?? new Dictionary<string, string>();
    }

    public ActivityKind Kind { get; }

    public IReadOnlyDictionary<string, string> Tags { get; }
}
