using System;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Otel.Kafka.Interceptors.DependencyInjection;

namespace Otel.Kafka.Interceptors.StartupHook.Registration;

internal static class ServiceConfigurator
{
    private static bool _logged;

    public static void Configure(HostBuilderContext context, IServiceCollection services)
    {
        try
        {
            IConfiguration configuration = context.Configuration;
            services.AddOtelKafkaAutoSetup(configuration);

            if (!_logged)
            {
                _logged = true;
                Console.Error.WriteLine("[OtelKafka/StartupHook] Otel Kafka auto-setup applied.");
            }
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"[OtelKafka/StartupHook] ConfigureServices failed: {ex}");
        }
    }
}
