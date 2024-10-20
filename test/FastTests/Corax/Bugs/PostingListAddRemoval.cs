using System.Collections.Generic;
using System.IO;
using System.Linq;
using FastTests.Voron;
using SharpCompress.Compressors;
using SharpCompress.Compressors.Deflate;
using Sparrow.Server;
using Sparrow.Threading;
using Voron.Util.PFor;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax.Bugs;

public class PostingListAddRemoval : StorageTest
{
    public PostingListAddRemoval(ITestOutputHelper output) : base(output)
    {
    }
    
    [Fact]
    public void AdditionsAndRemovalWorkInBulk()
    {
        var ops = ReadOperationsFrom("3-2015-10.txt.gz");
        using (var wtx = Env.WriteTransaction())
        {
            var set = wtx.OpenPostingList("test");
            foreach (var op in ops)
            {
                if (op.Add)
                {
                    foreach (var it in op.Ids)
                    {
                        set.Add(it);
                    }
                }
                else
                {
                    foreach (long it in op.Ids)
                    {
                        set.Remove(it);
                    }
                }
            }

            wtx.Commit();
        }

        using (var wtx = Env.WriteTransaction())
        {
            var set = wtx.OpenPostingList("test");
            set.DumpAllValues();
        }
    }


    [Fact]
    public unsafe void CanHandleLargeValues()
    {
        List<long> items = ReadNumbersFromResource("Corax.PostingList.AddsBiggerThanInt.txt").ToList();
        items.Sort();

        // full
        fixed (long* ll = items.ToArray())
        {
            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            using var e = new FastPForEncoder(bsc);
            int encodedSize = e.Encode(ll, items.Count);
            var buffer = new byte[ushort.MaxValue];
            fixed (byte* b = buffer)
            {
                var (count, sizeUsed) = e.Write(b, ushort.MaxValue);
                Assert.Equal(items.Count, count);
                Assert.Equal(sizeUsed, encodedSize);
            }
        }
        
        // partial
        fixed (long* ll = items.ToArray())
        {
            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            using var e = new FastPForEncoder(bsc);
            e.Encode(ll, items.Count);
            var read = 0;
            int i = 0;
            while (read < items.Count)
            {
                i++;
                int pageSizeWithoutMetadata = 8128;
                var buffer = new byte[pageSizeWithoutMetadata];
                fixed (byte* b = buffer)
                {
                    var (count, sizeUsed) = e.Write(b, pageSizeWithoutMetadata);
                    read += count;
                    Assert.True(sizeUsed<pageSizeWithoutMetadata);
                }
            }
        }
    }

    [Fact]
    public unsafe void CanWorkWhenEntryIdIsBiggerThanInt()
    {
        var maxSize = 0;
        List<long> items = ReadNumbersFromResource("Corax.PostingList.AddsBiggerThanInt.txt").ToList();
        items.Sort();

        
        maxSize = items.Count;
        using (var wtx = Env.WriteTransaction())
        {
            var set = wtx.OpenPostingList("test");
            foreach (long id in  items)
            {
                set.Add(id);
            }   
            wtx.Commit();
        }

        using (var rtx = Env.ReadTransaction())
        {
            var set = rtx.OpenPostingList("test");

            Assert.Equal(items, set.DumpAllValues());
            Assert.Equal(items.Count, set.State.NumberOfEntries);
        }

        var removals = ReadNumbersFromResource("Corax.PostingList.RemBiggerThanInt.txt").ToList();
        using (var wtx = Env.WriteTransaction())
        {
            var set = wtx.OpenPostingList("test");
            foreach (long id in removals)
            {
                set.Remove(id);
            }                
            wtx.Commit();
        }

        var removalSet = new HashSet<long>(removals);
        items.RemoveAll(l => removalSet.Contains(l));
        
        using (var rtx = Env.ReadTransaction())
        {
            var set = rtx.OpenPostingList("test");
            Assert.Equal(items, set.DumpAllValues());

            Assert.Equal(items.Count, set.State.NumberOfEntries);
        }
        
        using (var rtx = Env.ReadTransaction())
        {
            var matches = new long[maxSize * 2];
            var set = rtx.OpenPostingList("test");
            set.Iterate().Fill(matches, out int read);
            Assert.Equal(items.Count, read);
            for (int i = 0; i < items.Count; i++)
            {
                Assert.Equal(items[i], matches[i]);
            }
        }
    }


    [Theory]
    [InlineData(300)]
    [InlineData(5000)]
    [InlineData(int.MaxValue)]
    public void AdditionsAndRemovalWork(int size)
    {
        var maxSize = 0;
        List<long> items = ReadNumbersFromResource("Corax.Set.Adds.txt").Take(size).ToList();
        items.Sort();
        
        maxSize = items.Count;
        using (var wtx = Env.WriteTransaction())
        {
            var set = wtx.OpenPostingList("test");
            foreach (long id in  items)
            {
                set.Add(id);
            }   
            wtx.Commit();
        }

        using (var rtx = Env.ReadTransaction())
        {
            var set = rtx.OpenPostingList("test");

            Assert.Equal(items.Count, set.State.NumberOfEntries);
            Assert.Equal(items, set.DumpAllValues());
        }

        var removals = ReadNumbersFromResource("Corax.Set.Removals.txt").ToList();
        using (var wtx = Env.WriteTransaction())
        {
            var set = wtx.OpenPostingList("test");
            foreach (long id in removals)
            {
                set.Remove(id);
            }                
            wtx.Commit();
        }

        var removalSet = new HashSet<long>(removals);
        items.RemoveAll(l => removalSet.Contains(l));
        
        using (var rtx = Env.ReadTransaction())
        {
            var set = rtx.OpenPostingList("test");
            Assert.Equal(items, set.DumpAllValues());

            Assert.Equal(items.Count, set.State.NumberOfEntries);
        }
        
        using (var rtx = Env.ReadTransaction())
        {
            var matches = new long[maxSize * 2];
            var set = rtx.OpenPostingList("test");
            set.Iterate().Fill(matches, out int read);
            Assert.Equal(items.Count, read);
            for (int i = 0; i < items.Count; i++)
            {
                Assert.Equal(items[i], matches[i]);
            }
        }
    }

    private static List<long> ReadNumbersFromResource(string file)
    {
        var reader = new StreamReader(typeof(PostingListAddRemoval).Assembly.GetManifestResourceStream("FastTests.Corax.Bugs." + file));
        var adds = new List<long>();
        string line = null;
        while ((line = reader.ReadLine()) != null)
        {
            adds.Add(long.Parse(line));
        }

        return adds;
    }
    
    private static List<(bool Add, List<long> Ids)> ReadOperationsFrom(string file)
    {
        var reader = new StreamReader(new GZipStream(typeof(PostingListAddRemoval).Assembly.GetManifestResourceStream("FastTests.Corax.Bugs." + file), CompressionMode.Decompress));
        var adds = new List<(bool Add, List<long> Ids)>();
        string line = null;
        while ((line = reader.ReadLine()) != null)
        {
            bool add = line[0] == '+';
            var ids = line.Substring(1).Split(',').Select(long.Parse).ToList();
            adds.Add((add, ids));
        }

        return adds;
    }
}
