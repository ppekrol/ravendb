using Sparrow.Json;

namespace Raven.Server.ServerWide.Commands;

public interface IContextResultCommand
{
    JsonOperationContext ContextToWriteResult { get; set; }
    object CloneResult(JsonOperationContext context, object result);
}
