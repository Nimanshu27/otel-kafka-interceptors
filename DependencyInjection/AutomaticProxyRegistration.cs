using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using Castle.DynamicProxy;
using Microsoft.Extensions.DependencyInjection;
using Otel.Kafka.Interceptors.Interception;
using Otel.Kafka.Interceptors.Options;

namespace Otel.Kafka.Interceptors.DependencyInjection;

internal static class AutomaticProxyRegistration
{
    public static void Apply(IServiceCollection services, OtelKafkaInterceptorOptions options)
    {
        var matcher = ProxyTargetMatcher.Create(options);
        if (!matcher.HasCandidates)
        {
            return;
        }

        if (services.Any(sd => sd.ServiceType == typeof(AutomaticProxyMarker)))
        {
            return;
        }

        services.AddSingleton<AutomaticProxyMarker>();

        for (var i = 0; i < services.Count; i++)
        {
            var descriptor = services[i];
            if (!matcher.ShouldIntercept(descriptor.ServiceType))
            {
                continue;
            }

            if (TryDecorate(descriptor, out var decorated))
            {
                services[i] = decorated;
            }
        }
    }

    private static bool TryDecorate(ServiceDescriptor descriptor, out ServiceDescriptor decorated)
    {
        decorated = descriptor;

        if (descriptor.ServiceType.IsGenericTypeDefinition)
        {
            return false;
        }

        var serviceType = descriptor.ServiceType;
        var implementationType = descriptor.ImplementationType;
        var implementationFactory = descriptor.ImplementationFactory;
        var implementationInstance = descriptor.ImplementationInstance;

        if (implementationType == null && implementationFactory == null && implementationInstance == null)
        {
            return false;
        }

        decorated = ServiceDescriptor.Describe(
            serviceType,
            provider =>
            {
                var target = GetTargetInstance(
                    provider,
                    implementationType,
                    implementationFactory,
                    implementationInstance);
                return CreateProxy(provider, serviceType, target);
            },
            descriptor.Lifetime);

        return true;
    }

    private static object GetTargetInstance(
        IServiceProvider provider,
        Type? implementationType,
        Func<IServiceProvider, object>? implementationFactory,
        object? implementationInstance)
    {
        if (implementationInstance != null)
        {
            return implementationInstance;
        }

        if (implementationFactory != null)
        {
            return implementationFactory(provider);
        }

        if (implementationType != null)
        {
            return ActivatorUtilities.CreateInstance(provider, implementationType);
        }

        throw new InvalidOperationException("Unable to determine implementation for proxy decoration.");
    }

    private static object CreateProxy(IServiceProvider provider, Type serviceType, object target)
    {
        var generator = provider.GetRequiredService<IProxyGenerator>();
        var interceptor = provider.GetRequiredService<OpenTelemetryInterceptor>();

        if (serviceType.IsInterface)
        {
            return generator.CreateInterfaceProxyWithTarget(serviceType, target, interceptor);
        }

        if (serviceType.IsClass)
        {
            return generator.CreateClassProxyWithTarget(serviceType, target, interceptor);
        }

        throw new InvalidOperationException($"Unsupported service type '{serviceType.FullName}' for proxy decoration.");
    }

    private sealed class AutomaticProxyMarker
    {
    }

    private sealed class ProxyTargetMatcher
    {
        private readonly HashSet<string> _exactMatches;
        private readonly List<PatternRule> _patterns;

        private ProxyTargetMatcher(HashSet<string> exactMatches, List<PatternRule> patterns)
        {
            _exactMatches = exactMatches;
            _patterns = patterns;
        }

        public bool HasCandidates => _exactMatches.Count > 0 || _patterns.Count > 0;

        public static ProxyTargetMatcher Create(OtelKafkaInterceptorOptions options)
        {
            var exactMatches = new HashSet<string>(StringComparer.Ordinal);

            if (options.Rules is { Count: > 0 })
            {
                foreach (var rule in options.Rules)
                {
                    if (string.IsNullOrWhiteSpace(rule.Type))
                    {
                        continue;
                    }

                    exactMatches.Add(rule.Type.Trim());
                }
            }

            var patterns = new List<PatternRule>();
            if (options.Expressions is { Count: > 0 })
            {
                foreach (var expression in options.Expressions)
                {
                    if (string.IsNullOrWhiteSpace(expression.TypePattern))
                    {
                        continue;
                    }

                    patterns.Add(new PatternRule(expression.TypePattern.Trim(), expression.UseRegex));
                }
            }

            return new ProxyTargetMatcher(exactMatches, patterns);
        }

        public bool ShouldIntercept(Type serviceType)
        {
            if (serviceType.IsGenericTypeDefinition)
            {
                return false;
            }

            var fullName = serviceType.FullName;
            var simpleName = serviceType.Name;

            if (fullName != null && _exactMatches.Contains(fullName))
            {
                return true;
            }

            if (_exactMatches.Contains(simpleName))
            {
                return true;
            }

            foreach (var pattern in _patterns)
            {
                if ((fullName != null && pattern.IsMatch(fullName)) ||
                    pattern.IsMatch(simpleName))
                {
                    return true;
                }
            }

            return false;
        }

        private sealed class PatternRule
        {
            private readonly Regex? _regex;
            private readonly string? _glob;

            public PatternRule(string pattern, bool useRegex)
            {
                if (useRegex)
                {
                    _regex = new Regex(pattern, RegexOptions.CultureInvariant | RegexOptions.Compiled);
                }
                else
                {
                    _glob = pattern;
                }
            }

            public bool IsMatch(string value)
            {
                if (_regex != null)
                {
                    return _regex.IsMatch(value);
                }

                if (_glob == null)
                {
                    return false;
                }

                return GlobMatch(_glob, value);
            }

            private static bool GlobMatch(string pattern, string value)
            {
                var regexPattern = "^" + Regex.Escape(pattern)
                    .Replace("\\*", ".*")
                    .Replace("\\?", ".") + "$";
                return Regex.IsMatch(value, regexPattern, RegexOptions.CultureInvariant);
            }
        }
    }
}
