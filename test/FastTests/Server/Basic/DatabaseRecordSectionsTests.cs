using System;
using System.Linq;
using System.Reflection;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.ServerWide;
using Raven.Server.ServerWide;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Server.Basic
{
    public class DatabaseRecordSectionsTests : NoDisposalNeeded
    {
        public DatabaseRecordSectionsTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Cluster)]
        public void AllDatabaseRecordFieldsMustBeMappedToSection()
        {
            var fields = typeof(DatabaseRecord).GetFields(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)
                .Where(x => x.IsPublic || x.GetCustomAttribute<ForceJsonSerializationAttribute>() != null)
                .Select(x => x.Name)
                .ToHashSet();

            foreach (var field in fields)
            {
                Assert.True(DatabaseRecordSectionsExtensions.SectionByField.ContainsKey(field),
                    $"Field '{field}' of {nameof(DatabaseRecord)} must be mapped to a section in {nameof(DatabaseRecordSectionsExtensions)}.{nameof(DatabaseRecordSectionsExtensions.SectionByField)}, " +
                    $"and the features that depend on it must handle the changes of this section");
            }

            foreach (var field in DatabaseRecordSectionsExtensions.SectionByField.Keys)
                Assert.True(fields.Contains(field), $"Field '{field}' doesn't exist in {nameof(DatabaseRecord)}");
        }

        [RavenFact(RavenTestCategory.Cluster)]
        public void CanStampSectionsAndGetTheirVersion()
        {
            DatabaseRecordClusterState nullState = null;
            Assert.Equal(0, nullState.GetVersion(DatabaseRecordSections.All));

            var state = new DatabaseRecordClusterState();
            Assert.Equal(0, state.GetVersion(DatabaseRecordSections.All));

            state.Stamp(DatabaseRecordSections.Indexes, 10);
            Assert.Equal(10, state.LastIndexesIndex);
            Assert.Null(state.LastTopologyIndex);
            Assert.Equal(10, state.GetVersion(DatabaseRecordSections.Indexes));
            Assert.Equal(0, state.GetVersion(DatabaseRecordSections.Topology));
            Assert.Equal(10, state.GetVersion(DatabaseRecordSections.Indexes | DatabaseRecordSections.Topology));

            // configuration isn't tracked
            state.Stamp(DatabaseRecordSections.Configuration, 15);
            Assert.Equal(10, state.GetVersion(DatabaseRecordSections.All));

            state.Stamp(DatabaseRecordSections.All, 20);
            foreach (var field in typeof(DatabaseRecordClusterState).GetFields())
                Assert.Equal(20L, (long?)field.GetValue(state));
        }

        [RavenFact(RavenTestCategory.Cluster)]
        public void ConsumerShouldHandleOnlyChangesOfItsSections()
        {
            var consumer = new DatabaseRecordChangeConsumer("Test", DatabaseRecordSections.Indexes | DatabaseRecordSections.Topology);
            var record = new DatabaseRecord("Test");

            // nothing was handled yet, even if the record has no cluster state (e.g. created by an older version)
            Assert.True(consumer.ShouldProcess(record, forced: false, out var version));
            Assert.Equal(0, version);
            consumer.MarkProcessed(version);

            Assert.False(consumer.ShouldProcess(record, forced: false, out _));
            Assert.True(consumer.ShouldProcess(record, forced: true, out _));

            record.ClusterState = new DatabaseRecordClusterState();
            record.ClusterState.Stamp(DatabaseRecordSections.Etls | DatabaseRecordSections.PeriodicBackups, 5);
            Assert.False(consumer.ShouldProcess(record, forced: false, out _));

            record.ClusterState.Stamp(DatabaseRecordSections.Topology, 7);
            Assert.True(consumer.ShouldProcess(record, forced: false, out version));
            Assert.Equal(7, version);
            consumer.MarkProcessed(version);

            Assert.False(consumer.ShouldProcess(record, forced: false, out _));

            record.ClusterState.Stamp(DatabaseRecordSections.Indexes, 9);
            Assert.True(consumer.ShouldProcess(record, forced: false, out version));
            Assert.Equal(9, version);
        }

        [RavenFact(RavenTestCategory.Cluster)]
        public void EveryTaskTypeMustBeMappedToSection()
        {
            foreach (var taskType in Enum.GetValues<OngoingTaskType>())
            {
                var sections = DatabaseRecordSectionsExtensions.ForTaskType(taskType);

                if (taskType == OngoingTaskType.Subscription)
                {
                    // subscriptions aren't part of the database record
                    Assert.Equal(DatabaseRecordSections.All, sections);
                    continue;
                }

                Assert.True(sections != DatabaseRecordSections.All, $"Task type '{taskType}' must be mapped to the section of the database record it is stored in");
            }
        }
    }
}
