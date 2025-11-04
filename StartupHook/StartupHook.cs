using System;
using System.Diagnostics;
using System.Threading;
using Otel.Kafka.Interceptors.StartupHook;

public static class StartupHook
{
    private static int _initialized;

    public static void Initialize()
    {
        if (Interlocked.Exchange(ref _initialized, 1) == 1)
        {
            return;
        }

        var enabledValue = Environment.GetEnvironmentVariable("OTEL_KAFKA_INTERCEPTORS_ENABLED");
        if (!IsEnabled(enabledValue))
        {
            return;
        }

        try
        {
            DiagnosticListener.AllListeners.Subscribe(new HostingAllListenersObserver());
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"[OtelKafka/StartupHook] Failed to subscribe to diagnostics: {ex}");
        }
    }

    private static bool IsEnabled(string? value)
    {
        if (string.IsNullOrEmpty(value))
        {
            return true;
        }

        return value.Equals("true", StringComparison.OrdinalIgnoreCase) ||
               value.Equals("1", StringComparison.OrdinalIgnoreCase);
    }
}
