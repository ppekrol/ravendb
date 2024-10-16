﻿using Raven.Client.Documents.Queries;

namespace Raven.Server.Documents.Queries
{
    public sealed class TermsQueryResultServerSide : TermsQueryResult
    {
        public static readonly TermsQueryResultServerSide NotModifiedResult = new TermsQueryResultServerSide { NotModified = true };

        public bool NotModified { get; private set; }
    }
}