# Otel.Kafka.Interceptors Documentation

## Overview
`Otel.Kafka.Interceptors` is a .NET 8 library that adds OpenTelemetry tracing and Kafka propagation to your applications without polluting business code. It decorates existing services using Castle DynamicProxy, applies rules defined in configuration, and emits spans via the OpenTelemetry SDK. A companion startup hook can bootstrap everything at runtime, enabling true “drop-in” observability.

## Capabilities
- **Configuration-driven interception** for interfaces and classes using exact names, glob patterns, or regex.
- **Kafka propagation** with W3C headers (`traceparent`, `tracestate`) injected on producers and extracted on consumers.
- **Automatic DI decoration** that rewrites `IServiceCollection` descriptors to return proxies.
- **Startup hook integration** that wires the library before `Program.Main` executes (zero-touch).
- **OpenTelemetry pipeline** out-of-the-box with OTLP exporter, HTTP client instrumentation, and customizable tracing builder.
- **Granular tagging** through rules that attach span attributes, activity name formats, and semantic conventions.
- **Operational controls** such as runtime kill switch, log output, and error-safe interception.

## Package Information
| Package | Description | Target framework |
| --- | --- | --- |
| `Otel.Kafka.Interceptors` | Core interception SDK | `net8.0` |
| `Otel.Kafka.Interceptors.StartupHook` | Optional startup hook assembly | `net8.0` |

Dependencies include `Confluent.Kafka`, `Castle.Core`, `Microsoft.Extensions.*` (DI, Options), and `OpenTelemetry` 1.13.0.

## Quick Start

### 1. Install the package
```bash
dotnet add package Otel.Kafka.Interceptors
```

### 2. Provide configuration
```jsonc
"OtelKafka": {
  "Interceptors": {
    "ActivitySourceName": "MyApp",
    "Kafka": { "InjectOnProducer": true, "ExtractOnConsumer": true },
    "Rules": [
      {
        "Type": "MyApp.IMyProducer",
        "Methods": [ "SendAsync" ],
        "Kind": "Producer",
        "Tags": {
          "messaging.system": "kafka",
          "messaging.operation": "send"
        }
      }
    ]
  }
}
```

### 3. Choose a wiring approach

#### Zero-touch (recommended)
1. Copy `Otel.Kafka.Interceptors.StartupHook.dll` alongside your app (publish output, Docker image, etc.).
2. Set `DOTNET_STARTUP_HOOKS=/absolute/path/Otel.Kafka.Interceptors.StartupHook.dll`.
3. Make sure configuration contains `OtelKafka:Interceptors`.

The hook subscribes to hosting diagnostics and calls `services.AddOtelKafkaAutoSetup(configuration)` before the host is built.

#### Manual DI
```csharp
var section = builder.Configuration.GetSection("OtelKafka:Interceptors");

services.AddOtelKafkaInterceptors(section);
services.AddOtelKafkaProxiedScoped<IMyProducer, MyProducer>();
services.AddOtelKafkaProxiedScoped<IMyConsumer, MyConsumer>();

services.AddOpenTelemetry()
    .WithTracing(b =>
    {
        b.SetResourceBuilder(ResourceBuilder.CreateDefault().AddService("my-service"))
         .AddSource(section.GetValue<string>("ActivitySourceName") ?? "Otel.Kafka.Interceptors")
         .AddHttpClientInstrumentation()
         .AddOtlpExporter();
    });
```

## Configuration Reference
Section path: `OtelKafka:Interceptors`.

| Key | Type | Default | Notes |
| --- | --- | --- | --- |
| `ActivitySourceName` | string | `Otel.Kafka.Interceptors` | Source name for spans; ensure `AddSource` matches this. |
| `Kafka.InjectOnProducer` | bool | `true` | Adds headers to outbound messages. |
| `Kafka.ExtractOnConsumer` | bool | `true` | Reads headers from consumed messages. |
| `Kafka.EnvelopeArgumentType` | string? | `null` | Fully-qualified type name for custom message envelopes exposing a `Headers` property. |
| `Kafka.ConsumeResultArgumentType` | string? | `null` | Override when using `ConsumeResult<TKey, TValue>` with non-string generics. |
| `Rules[]` | array | `[]` | Exact-match rules using type name + optional method list. |
| `Expressions[]` | array | `[]` | Pattern-based rules using glob or regex. |

### `RuleDefinition`
```jsonc
{
  "Type": "Namespace.IFoo",
  "Methods": [ "SendAsync" ],
  "Kind": "Producer",
  "Tags": {
    "messaging.system": "kafka",
    "messaging.destination": "orders"
  }
}
```

### `ExpressionRuleDefinition`
```jsonc
{
  "TypePattern": "Namespace.*Consumer",
  "MethodPattern": "^Handle",
  "UseRegex": true,
  "Kind": "Consumer",
  "Tags": { "component": "handler" }
}
```

`Kind` accepts `Internal`, `Client`, `Server`, `Producer`, or `Consumer`.

## Interception Pipeline
1. `OpenTelemetryInterceptor` receives each call.
2. Declaring type/method are matched against rules and expressions.
3. An `Activity` is started with the configured span kind.
4. Producer flow: locate `Confluent.Kafka.Headers` or envelope, inject trace context.
5. Consumer flow: locate `ConsumeResult<TKey, TValue>` (string by default), extract trace context, tag topic/partition/offset/key.
6. Invocation proceeds; async return values are wrapped to record success/failure.
7. Exceptions trigger error tags before being rethrown.

## Automatic Proxy Registration
`AutomaticProxyRegistration.Apply` inspects the current `IServiceCollection`:
- Matches service types against rule definitions / expression patterns.
- Replaces descriptors with proxies created via Castle DynamicProxy.
- Skips generic type definitions and already-proxied services.
- Adds a marker to avoid double-registration.

You can call `AddOtelKafkaAutoSetup(configuration)` at the end of your DI configuration to apply this automatically.

## Startup Hook Details

- Namespace: global (class `StartupHook` with static `Initialize()`).
- Subscribes to `DiagnosticListener.AllListeners`.
- On `Microsoft.Extensions.Hosting` events, hooks into `HostBuilding` to add `ConfigureServices`.
- Respects `OTEL_KAFKA_INTERCEPTORS_ENABLED` (defaults to enabled).
- Writes errors and activation messages to stderr.

### Required environment variables
| Variable | Purpose |
| --- | --- |
| `DOTNET_STARTUP_HOOKS` | Points to `Otel.Kafka.Interceptors.StartupHook.dll`. |
| `OTEL_KAFKA_INTERCEPTORS_ENABLED` | Set to `false` or `0` to disable. |
| `OTEL_SERVICE_NAME`, `OTEL_EXPORTER_OTLP_*` | Standard OpenTelemetry configuration. |

## Kafka Message Support

Producer injections rely on:
- Direct `Headers` arguments.
- Objects with public `Headers` property returning `Confluent.Kafka.Headers`.
- Custom envelope types specified by `EnvelopeArgumentType`.

Consumer extraction searches for:
- `ConsumeResult<string, string>` by default.
- Types provided via `ConsumeResultArgumentType` for custom generics.

If headers are absent, spans still emit but without parent context.

## Customizing Tracing
Use the optional delegate in `AddOtelKafkaAutoSetup`:
```csharp
services.AddOtelKafkaAutoSetup(configuration, configureTracing: builder =>
{
    builder.AddAspNetCoreInstrumentation()
           .AddConsoleExporter()
           .SetSampler(new ParentBasedSampler(new TraceIdRatioBasedSampler(0.25)));
});
```

## Diagnostics & Troubleshooting
| Symptom | Suggested Fix |
| --- | --- |
| `Startup hook assembly … failed to load` | Verify DLL path and that the file is copied into the runtime environment. |
| `Could not load type 'StartupHook'` | Ensure you are using the provided hook where the class is in the global namespace. |
| No spans emitted | Confirm rule matches (`Type` or `TypePattern`) align with service names. Ensure `ActivitySourceName` is added to `AddSource`. |
| Headers not injected | Check that producer method exposes `Headers` or configure `EnvelopeArgumentType`. |
| Consumer not extracting | Update `ConsumeResultArgumentType` to your generic signature (e.g., `Confluent.Kafka.ConsumeResult<System.String, MyNamespace.MyPayload>`). |

Enable richer diagnostics by setting `Logging:LogLevel:Default=Debug` in configuration—interceptor and hook messages are emitted through `ILogger<T>`.

## Compatibility & Requirements
- .NET 8 or later.
- `Microsoft.Extensions.DependencyInjection` based DI (works with host builder, minimal APIs, ASP.NET Core).
- `Confluent.Kafka` 2.12.0 (override via your project file if needed).
- Tested against `OpenTelemetry` 1.13.0 packages.

## Roadmap Ideas
- Additional envelope detection for popular Kafka abstractions.
- Metrics/logging emitters alongside traces.
- Source generator for compile-time rule hints.
- First-class ASP.NET `IHostingStartup` package.

## Contributing
1. Fork the repository, run `dotnet build`.
2. Add tests/examples (future home under `tests/`).
3. Submit a PR describing the behavior change and configuration impact.

For bugs or feature requests, please open an issue with reproduction steps and configuration snippets.
