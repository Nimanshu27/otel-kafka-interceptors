using System.Diagnostics;

namespace Otel.Kafka.Interceptors.Options;

public enum TelemetrySpanKind
{
    Internal,
    Client,
    Server,
    Producer,
    Consumer
}

internal static class TelemetrySpanKindExtensions
{
    public static ActivityKind ToActivityKind(this TelemetrySpanKind kind) => kind switch
    {
        TelemetrySpanKind.Client => ActivityKind.Client,
        TelemetrySpanKind.Server => ActivityKind.Server,
        TelemetrySpanKind.Producer => ActivityKind.Producer,
        TelemetrySpanKind.Consumer => ActivityKind.Consumer,
        _ => ActivityKind.Internal
    };
}
