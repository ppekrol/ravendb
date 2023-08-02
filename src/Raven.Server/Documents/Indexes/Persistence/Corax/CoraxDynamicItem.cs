using Raven.Server.Documents.Indexes;

namespace Corax.Utils;

internal sealed class CoraxDynamicItem
{
    public string FieldName;
    public IndexField Field;
    public object Value;
}
