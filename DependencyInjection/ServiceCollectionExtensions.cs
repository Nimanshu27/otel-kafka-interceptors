using System;
using System.Diagnostics;
using System.Linq;
using Castle.DynamicProxy;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Otel.Kafka.Interceptors.Interception;
using Otel.Kafka.Interceptors.Options;
using Otel.Kafka.Interceptors.Rules;

namespace Otel.Kafka.Interceptors.DependencyInjection;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddOtelKafkaInterceptors(
        this IServiceCollection services,
        IConfiguration? configurationSection = null,
        Action<OtelKafkaInterceptorOptions>? configure = null)
    {
        var bootstrapOptions = new OtelKafkaInterceptorOptions();

        if (configurationSection != null)
        {
            configurationSection.Bind(bootstrapOptions);
            services.Configure<OtelKafkaInterceptorOptions>(configurationSection);
        }
        if (configure != null)
        {
            configure(bootstrapOptions);
            services.Configure(configure);
        }
        services.TryAddSingleton<IProxyGenerator, ProxyGenerator>();
        services.TryAddSingleton<IRuleEngine, RuleEngine>();

        services.TryAddSingleton<ActivitySource>(sp =>
        {
            var opts = sp.GetRequiredService<IOptions<OtelKafkaInterceptorOptions>>().Value;
            return new ActivitySource(opts.ActivitySourceName ?? "Otel.Kafka.Interceptors");
        });

        services.TryAddSingleton<OpenTelemetryInterceptor>();

        AutomaticProxyRegistration.Apply(services, bootstrapOptions);

        return services;
    }

    public static IServiceCollection AddOtelKafkaProxiedScoped<TInterface, TImplementation>(
        this IServiceCollection services)
        where TInterface : class
        where TImplementation : class, TInterface
    {
        services.AddScoped<TImplementation>();
        services.AddScoped<TInterface>(sp =>
        {
            var generator = sp.GetRequiredService<IProxyGenerator>();
            var interceptor = sp.GetRequiredService<OpenTelemetryInterceptor>();
            var implementation = sp.GetRequiredService<TImplementation>();
            return generator.CreateInterfaceProxyWithTarget<TInterface>(implementation, interceptor);
        });

        return services;
    }



    private static ServiceLifetime ParseLifetime(string? lifetime) => lifetime?.Trim().ToLowerInvariant() switch
    {
        "singleton" => ServiceLifetime.Singleton,
        "transient" => ServiceLifetime.Transient,
        _ => ServiceLifetime.Scoped
    };
}
