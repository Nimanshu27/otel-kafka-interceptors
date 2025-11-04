using System.Reflection;

namespace Otel.Kafka.Interceptors.Rules;

public interface IRuleEngine
{
    RuleMatch? Match(Type declaringType, MethodInfo method);
}
