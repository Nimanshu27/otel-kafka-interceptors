using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using Microsoft.Extensions.Options;
using Otel.Kafka.Interceptors.Options;

namespace Otel.Kafka.Interceptors.Rules;

internal sealed class RuleEngine : IRuleEngine
{
    private readonly IReadOnlyList<ExactRule> _exactRules;
    private readonly IReadOnlyList<PatternRule> _patternRules;

    public RuleEngine(IOptions<OtelKafkaInterceptorOptions> options)
    {
        var value = options.Value ?? new OtelKafkaInterceptorOptions();

        _exactRules = value.Rules
            .Where(r => !string.IsNullOrWhiteSpace(r.Type))
            .Select(r => new ExactRule(
                r.Type!.Trim(),
                r.Methods?.Where(m => !string.IsNullOrWhiteSpace(m))
                    .Select(m => m.Trim())
                    .ToArray() ?? Array.Empty<string>(),
                r.Kind.ToActivityKind(),
                r.Tags is { Count: > 0 }
                    ? new Dictionary<string, string>(r.Tags)
                    : new Dictionary<string, string>()))
            .ToArray();

        _patternRules = value.Expressions
            .Where(r => !string.IsNullOrWhiteSpace(r.TypePattern) || !string.IsNullOrWhiteSpace(r.MethodPattern))
            .Select(r => new PatternRule(
                r.TypePattern,
                r.MethodPattern,
                r.UseRegex,
                r.Kind.ToActivityKind(),
                r.Tags is { Count: > 0 }
                    ? new Dictionary<string, string>(r.Tags)
                    : new Dictionary<string, string>()))
            .ToArray();
    }

    public RuleMatch? Match(Type declaringType, MethodInfo method)
    {
        foreach (var rule in _exactRules)
        {
            if (rule.IsMatch(declaringType, method))
            {
                return rule.ToMatch();
            }
        }

        foreach (var rule in _patternRules)
        {
            if (rule.IsMatch(declaringType, method))
            {
                return rule.ToMatch();
            }
        }

        return null;
    }

    private sealed record ExactRule(
        string TypeName,
        IReadOnlyList<string> Methods,
        ActivityKind Kind,
        IReadOnlyDictionary<string, string> Tags)
    {
        public bool IsMatch(Type type, MethodInfo method)
        {
            var fullName = type.FullName ?? type.Name;
            if (!string.Equals(fullName, TypeName, StringComparison.Ordinal) &&
                !string.Equals(type.Name, TypeName, StringComparison.Ordinal))
            {
                return false;
            }

            if (Methods.Count is 0)
            {
                return true;
            }

            return Methods.Contains(method.Name, StringComparer.Ordinal);
        }

        public RuleMatch ToMatch() => new(Kind, Tags);
    }

    private sealed class PatternRule
    {
        private readonly Regex? _typeRegex;
        private readonly Regex? _methodRegex;
        private readonly string? _typeGlob;
        private readonly string? _methodGlob;
        private readonly ActivityKind _kind;
        private readonly IReadOnlyDictionary<string, string> _tags;

        public PatternRule(
            string? typePattern,
            string? methodPattern,
            bool useRegex,
            ActivityKind kind,
            IReadOnlyDictionary<string, string> tags)
        {
            _kind = kind;
            _tags = tags;

            if (useRegex)
            {
                _typeRegex = string.IsNullOrWhiteSpace(typePattern)
                    ? null
                    : new Regex(typePattern, RegexOptions.Compiled | RegexOptions.CultureInvariant);
                _methodRegex = string.IsNullOrWhiteSpace(methodPattern)
                    ? null
                    : new Regex(methodPattern, RegexOptions.Compiled | RegexOptions.CultureInvariant);
            }
            else
            {
                _typeGlob = typePattern;
                _methodGlob = methodPattern;
            }
        }

        public bool IsMatch(Type type, MethodInfo method)
        {
            var typeName = type.FullName ?? type.Name;

            if (_typeRegex != null)
            {
                if (!_typeRegex.IsMatch(typeName))
                {
                    return false;
                }
            }
            else if (!string.IsNullOrEmpty(_typeGlob) && !GlobMatch(_typeGlob, typeName))
            {
                return false;
            }

            if (_methodRegex != null)
            {
                return _methodRegex.IsMatch(method.Name);
            }

            if (!string.IsNullOrEmpty(_methodGlob))
            {
                return GlobMatch(_methodGlob, method.Name);
            }

            return true;
        }

        public RuleMatch ToMatch() => new(_kind, _tags);

        private static bool GlobMatch(string pattern, string value)
        {
            var regexPattern = "^" + Regex.Escape(pattern)
                .Replace("\\*", ".*")
                .Replace("\\?", ".") + "$";
            return Regex.IsMatch(value, regexPattern, RegexOptions.CultureInvariant);
        }
    }
}
