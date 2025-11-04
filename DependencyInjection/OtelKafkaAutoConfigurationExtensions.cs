using System;
using System.Linq;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

namespace Otel.Kafka.Interceptors.DependencyInjection;

public static class OtelKafkaAutoConfigurationExtensions
{
    private sealed class AutoSetupMarker
    {
    }

    public static IServiceCollection AddOtelKafkaAutoSetup(
        this IServiceCollection services,
        IConfiguration configuration,
        string configurationSectionPath = "OtelKafka:Interceptors",
        Action<TracerProviderBuilder>? configureTracing = null)
    {
        if (services.Any(sd => sd.ServiceType == typeof(AutoSetupMarker)))
        {
            return services;
        }

        var section = configuration.GetSection(configurationSectionPath);
        services.AddOtelKafkaInterceptors(section.Exists() ? section : null);

        services.AddSingleton<AutoSetupMarker>();

        var activitySourceName = section.GetValue<string?>("ActivitySourceName") ?? "Otel.Kafka.Interceptors";
        var serviceName = ResolveServiceName(configuration);

        services.AddOpenTelemetry()
            .WithTracing(builder =>
            {
                builder
                    .SetResourceBuilder(
                        ResourceBuilder.CreateDefault()
                            .AddService(serviceName: serviceName))
                    .AddSource(activitySourceName)
                    .AddHttpClientInstrumentation()
                    .AddOtlpExporter();

                configureTracing?.Invoke(builder);
            });

        return services;
    }

    private static string ResolveServiceName(IConfiguration configuration)
    {
        return configuration["OTEL_SERVICE_NAME"]
            ?? configuration["Otel:ServiceName"]
            ?? "kafka-service";
    }
}
