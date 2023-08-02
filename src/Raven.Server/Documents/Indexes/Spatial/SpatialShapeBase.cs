namespace Raven.Server.Documents.Indexes.Spatial
{
    internal abstract class SpatialShapeBase
    {
        public readonly SpatialShapeType Type;

        protected SpatialShapeBase(SpatialShapeType type)
        {
            Type = type;
        }
    }
}
