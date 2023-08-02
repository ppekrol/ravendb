using System.Collections.Generic;

namespace Raven.Server.Documents.ETL.Providers.OLAP
{
    internal sealed class ToOlapItem : ExtractedItem
    {
        public ToOlapItem(ToOlapItem item)
        {
            Etag = item.Etag;
            DocumentId = item.DocumentId;
            Document = item.Document;
            IsDelete = item.IsDelete;
            Collection = item.Collection;
            ChangeVector = item.ChangeVector;
        }

        public ToOlapItem(Document document, string collection) : base(document, collection, EtlItemType.Document)
        {
        }

        public List<OlapColumn> Properties { get; set; }
    }
}
