using System.Diagnostics;
using System.Reflection;
using System.Text;

namespace Otel.Kafka.Interceptors.Interception;

public class OpenTelemetryInterceptor : IInterceptor
{
    private static readonly MethodInfo WrapGenericTaskMethod = typeof(OpenTelemetryInterceptor)
        .GetMethod(nameof(WrapGenericTaskAsync), BindingFlags.NonPublic | BindingFlags.Static)!;

    private readonly ActivitySource _activitySource;
    private readonly IRuleEngine _ruleEngine;
    private readonly KafkaOptions _kafkaOptions;
    private readonly TextMapPropagator _propagator;
    private readonly ILogger<OpenTelemetryInterceptor> _logger;

    public OpenTelemetryInterceptor(
        ActivitySource activitySource,
        IRuleEngine ruleEngine,
        IOptions<OtelKafkaInterceptorOptions> options,
        ILogger<OpenTelemetryInterceptor> logger)
    {
        _activitySource = activitySource;
        _ruleEngine = ruleEngine;
        _kafkaOptions = options.Value?.Kafka ?? new KafkaOptions();
        _propagator = Propagators.DefaultTextMapPropagator;
        _logger = logger;
    }

    public void Intercept(IInvocation invocation)
    {
        if (invocation.Method.ReturnType == typeof(void) ||
            !typeof(Task).IsAssignableFrom(invocation.Method.ReturnType))
        {
            InterceptSync(invocation);
            return;
        }

        InterceptAsync(invocation);
    }

    private void InterceptSync(IInvocation invocation)
    {
        var method = invocation.Method;
        var declaringType = method.DeclaringType ?? invocation.TargetType
            ?? throw new InvalidOperationException("Unable to determine declaring type for intercepted method.");

        var match = _ruleEngine.Match(declaringType, method);
        var operationName = BuildOperationName(method);
        var activity = _activitySource.StartActivity(operationName, match?.Kind ?? ActivityKind.Internal);
        ApplyTags(activity, match);

        try
        {
            invocation.Proceed();
            activity?.SetTag("status", "success");
        }
        catch (Exception ex)
        {
            TrackException(activity, ex);
            throw;
        }
        finally
        {
            activity?.Dispose();
        }
    }

    private void InterceptAsync(IInvocation invocation)
    {
        var method = invocation.Method;
        var declaringType = method.DeclaringType ?? invocation.TargetType
            ?? throw new InvalidOperationException("Unable to determine declaring type for intercepted method.");
        var match = _ruleEngine.Match(declaringType, method);

        var activityKind = match?.Kind ?? ActivityKind.Internal;
        var operationName = BuildOperationName(method);

        PropagationContext propagationContext = default;
        ConsumeResult<string, string>? consumeResult = null;

        if (_kafkaOptions.ExtractOnConsumer && activityKind == ActivityKind.Consumer)
        {
            consumeResult = FindConsumeResult(invocation);
            if (consumeResult?.Message?.Headers is { } headers)
            {
                propagationContext = Extract(headers);
            }
        }

        Activity? activity = propagationContext.ActivityContext != default
            ? _activitySource.StartActivity(operationName, activityKind, propagationContext.ActivityContext)
            : _activitySource.StartActivity(operationName, activityKind);

        ApplyTags(activity, match);

        if (activityKind == ActivityKind.Consumer && consumeResult != null)
        {
            EnrichConsumerActivity(activity, consumeResult);
        }

        if (activityKind == ActivityKind.Producer && _kafkaOptions.InjectOnProducer)
        {
            TryInject(invocation, activity);
        }

        invocation.Proceed();
        if (invocation.ReturnValue is Task task)
        {
            invocation.ReturnValue = WrapTask(invocation.Method.ReturnType, task, activity);
        }
        else
        {
            activity?.Dispose();
        }
    }

    private static string BuildOperationName(MethodInfo method)
    {
        var declaring = method.DeclaringType?.Name ?? "Unknown";
        return $"{declaring}.{method.Name}";
    }

    private static void ApplyTags(Activity? activity, RuleMatch? match)
    {
        if (activity == null || match?.Tags == null)
        {
            return;
        }

        foreach (var kvp in match.Tags)
        {
            activity.SetTag(kvp.Key, kvp.Value);
        }
    }

    private static void EnrichConsumerActivity(Activity? activity, ConsumeResult<string, string> result)
    {
        if (activity == null)
        {
            return;
        }

        activity.SetTag("messaging.system", "kafka");
        activity.SetTag("messaging.operation", "process");
        activity.SetTag("messaging.destination", result.Topic);
        activity.SetTag("messaging.kafka.partition", result.Partition.Value);
        activity.SetTag("messaging.kafka.offset", result.Offset.Value);

        if (!string.IsNullOrEmpty(result.Message?.Key))
        {
            activity.SetTag("messaging.kafka.message_key", result.Message.Key);
        }
    }

    private void TryInject(IInvocation invocation, Activity? activity)
    {
        if (activity == null)
        {
            return;
        }

        var headers = FindHeaders(invocation);
        if (headers == null)
        {
            return;
        }

        var context = new PropagationContext(activity.Context, default);
        _propagator.Inject(
            context,
            headers,
            static (carrier, key, value) =>
            {
                carrier.Remove(key);
                carrier.Add(key, Encoding.UTF8.GetBytes(value));
            });
    }

    private PropagationContext Extract(Headers headers)
    {
        return _propagator.Extract(
            default,
            headers,
            static (carrier, key) =>
            {
                if (!carrier.TryGetLastBytes(key, out var bytes))
                {
                    return Array.Empty<string>();
                }

                return new[] { Encoding.UTF8.GetString(bytes) };
            });
    }

    private static Headers? FindHeaders(IInvocation invocation)
    {
        foreach (var argument in invocation.Arguments)
        {
            if (argument is null)
            {
                continue;
            }

            if (argument is Headers directHeaders)
            {
                return directHeaders;
            }

            var headersProperty = argument.GetType().GetProperty(
                "Headers",
                BindingFlags.Instance | BindingFlags.Public);

            if (headersProperty?.GetValue(argument) is Headers propertyHeaders)
            {
                return propertyHeaders;
            }
        }

        return null;
    }

    private static ConsumeResult<string, string>? FindConsumeResult(IInvocation invocation)
    {
        foreach (var argument in invocation.Arguments)
        {
            if (argument is ConsumeResult<string, string> consumeResult)
            {
                return consumeResult;
            }
        }

        return null;
    }

    private static object WrapTask(Type returnType, Task task, Activity? activity)
    {
        if (returnType.IsGenericType && returnType.GetGenericTypeDefinition() == typeof(Task<>))
        {
            var resultType = returnType.GenericTypeArguments[0];
            var method = WrapGenericTaskMethod.MakeGenericMethod(resultType);
            var result = method.Invoke(null, new object[] { task, activity });
            if (result is null)
            {
                throw new InvalidOperationException("Failed to wrap Task<T> for interception.");
            }

            return result;
        }

        return WrapNonGenericTaskAsync(task, activity);
    }

    private static async Task WrapNonGenericTaskAsync(Task task, Activity? activity)
    {
        try
        {
            await task.ConfigureAwait(false);
            activity?.SetTag("status", "success");
        }
        catch (Exception ex)
        {
            TrackException(activity, ex);
            throw;
        }
        finally
        {
            activity?.Dispose();
        }
    }

    private static async Task<T> WrapGenericTaskAsync<T>(Task task, Activity? activity)
    {
        try
        {
            var result = await ((Task<T>)task).ConfigureAwait(false);
            activity?.SetTag("status", "success");
            return result;
        }
        catch (Exception ex)
        {
            TrackException(activity, ex);
            throw;
        }
        finally
        {
            activity?.Dispose();
        }
    }

    private static void TrackException(Activity? activity, Exception ex)
    {
        activity?.SetTag("status", "error");
        activity?.SetTag("error.type", ex.GetType().FullName);
        activity?.SetTag("error.message", ex.Message);
    }
}
