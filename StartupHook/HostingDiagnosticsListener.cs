using System;
using System.Collections.Generic;
using System.Diagnostics;
using Microsoft.Extensions.Hosting;
using Otel.Kafka.Interceptors.StartupHook.Registration;

namespace Otel.Kafka.Interceptors.StartupHook;

internal sealed class HostingAllListenersObserver : IObserver<DiagnosticListener>
{
    public void OnCompleted()
    {
    }

    public void OnError(Exception error)
    {
        Console.Error.WriteLine($"[OtelKafka/StartupHook] Listener error: {error}");
    }

    public void OnNext(DiagnosticListener value)
    {
        if (value.Name == "Microsoft.Extensions.Hosting")
        {
            value.Subscribe(new HostingEventsObserver());
        }
    }
}

internal sealed class HostingEventsObserver : IObserver<KeyValuePair<string, object?>>
{
    private bool _configured;

    public void OnCompleted()
    {
    }

    public void OnError(Exception error)
    {
        Console.Error.WriteLine($"[OtelKafka/StartupHook] Event error: {error}");
    }

    public void OnNext(KeyValuePair<string, object?> value)
    {
        if (_configured)
        {
            return;
        }

        if (!string.Equals(value.Key, "HostBuilding", StringComparison.Ordinal))
        {
            return;
        }

        var builder = TryGetHostBuilder(value.Value);
        if (builder == null)
        {
            return;
        }

        builder.ConfigureServices(ServiceConfigurator.Configure);
        _configured = true;
    }

    private static IHostBuilder? TryGetHostBuilder(object? payload)
    {
        if (payload is null)
        {
            return null;
        }

        if (payload is IHostBuilder directBuilder)
        {
            return directBuilder;
        }

        var type = payload.GetType();
        var property = type.GetProperty("HostBuilder")
            ?? type.GetProperty("Builder")
            ?? type.GetProperty("Value");

        if (property != null && property.GetValue(payload) is IHostBuilder builderFromProperty)
        {
            return builderFromProperty;
        }

        return null;
    }
}
