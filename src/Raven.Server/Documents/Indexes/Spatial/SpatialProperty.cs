namespace Raven.Server.Documents.Indexes.Spatial
{
    internal sealed class SpatialProperty
    {
        public string LatitudeProperty;
        public string LongitudeProperty;

        public SpatialProperty()
        {
        }

        public SpatialProperty(string latitudePropertyPath, string longitudePropertyPath)
        {
            LatitudeProperty = latitudePropertyPath;
            LongitudeProperty = longitudePropertyPath;
        }
    }
}
