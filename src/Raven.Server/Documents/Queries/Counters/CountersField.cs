namespace Raven.Server.Documents.Queries.Counters
{
    internal sealed class CountersField
    {
        public CountersField(int countersLength)
        {
            Counters = new string[countersLength];
        }

        private int _pos;

        public string[] Counters { get;}

        public string SourceAlias;

        public void AddCounter(string counter)
        {
            Counters[_pos++] = counter;
        }

    }
}
