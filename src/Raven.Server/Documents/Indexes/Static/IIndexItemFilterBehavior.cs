namespace Raven.Server.Documents.Indexes.Static
{
    internal interface IIndexItemFilterBehavior
    {
        bool ShouldFilter(IndexItem item);
    }
}
