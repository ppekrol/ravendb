using System;

namespace Raven.Server.Exceptions.ETL.ElasticSearch
{
    internal sealed class ElasticSearchLoadException : Exception
    {
        public ElasticSearchLoadException()
        {
        }

        public ElasticSearchLoadException(string message)
            : base(message)
        {
        }

        public ElasticSearchLoadException(string message, Exception inner)
            : base(message, inner)
        {
        }
    }
}
