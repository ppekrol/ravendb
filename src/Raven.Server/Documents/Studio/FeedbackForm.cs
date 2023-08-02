namespace Raven.Server.Documents.Studio
{
    internal sealed class FeedbackForm
    {
        public string Message { get; set; }

        public FeedbackProduct Product { get; set; }
        public FeedbackUser User { get; set; }

        internal sealed class FeedbackProduct
        {
            public string Name { get; set; }
            public string Version { get; set; }
            public string StudioVersion { get; set; }
            public string StudioView { get; set; }
            public string FeatureName { get; set; }
            public string FeatureImpression { get; set; } // 'positive' | 'negative' | null
        }

        internal sealed class FeedbackUser
        {
            public string Name { get; set; }
            public string Email { get; set; }
            public string UserAgent { get; set; }
        }
    }
}