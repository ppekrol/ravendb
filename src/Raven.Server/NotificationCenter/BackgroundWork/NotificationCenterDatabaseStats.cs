﻿using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Server.Extensions;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.NotificationCenter.BackgroundWork;

internal sealed class NotificationCenterDatabaseStats
{
    public long CountOfConflicts;

    public long CountOfDocuments;

    public long LastEtag;

    public int CountOfIndexes;

    public int CountOfStaleIndexes;

    public string[] StaleIndexes;

    public long CountOfIndexingErrors;

    public string GlobalChangeVector;

    public DateTime? LastIndexingErrorTime;

    public Dictionary<string, DatabaseStatsChanged.ModifiedCollection> Collections;

    public bool Equals(NotificationCenterDatabaseStats other)
    {
        if (ReferenceEquals(null, other))
            return false;
        if (ReferenceEquals(this, other))
            return true;
        return CountOfConflicts == other.CountOfConflicts &&
               CountOfDocuments == other.CountOfDocuments &&
               CountOfIndexes == other.CountOfIndexes &&
               CountOfIndexingErrors == other.CountOfIndexingErrors &&
               LastEtag == other.LastEtag &&
               CountOfStaleIndexes == other.CountOfStaleIndexes &&
               GlobalChangeVector == other.GlobalChangeVector &&
               DictionaryExtensions.ContentEquals(Collections, other.Collections);
    }

    public override bool Equals(object obj)
    {
        if (ReferenceEquals(null, obj))
            return false;
        if (ReferenceEquals(this, obj))
            return true;

        var stats = obj as NotificationCenterDatabaseStats;
        if (stats == null)
            return false;

        return Equals((NotificationCenterDatabaseStats)obj);
    }

    public override int GetHashCode()
    {
        unchecked
        {
            var hashCode = CountOfDocuments.GetHashCode();
            hashCode = (hashCode * 397) ^ CountOfIndexes.GetHashCode();
            hashCode = (hashCode * 397) ^ LastEtag.GetHashCode();
            hashCode = (hashCode * 397) ^ CountOfIndexingErrors.GetHashCode();
            hashCode = (hashCode * 397) ^ CountOfStaleIndexes.GetHashCode();
            hashCode = (hashCode * 397) ^ GlobalChangeVector.GetHashCode();
            hashCode = (hashCode * 397) ^ (Collections != null ? Collections.GetHashCode() : 0);
            return hashCode;
        }
    }

    public void CombineWith(NotificationCenterDatabaseStats stats,  IChangeVectorOperationContext context)
    {
        CountOfDocuments += stats.CountOfDocuments;
        CountOfConflicts += stats.CountOfConflicts;
        
        CountOfIndexes = stats.CountOfIndexes; // every node has the same amount of indexes
        CountOfIndexingErrors += stats.CountOfIndexingErrors;

        if (StaleIndexes == null)
            StaleIndexes = stats.StaleIndexes;
        else if (stats.StaleIndexes != null)
        {
            var staleIndexes = new HashSet<string>(StaleIndexes, StringComparer.OrdinalIgnoreCase);
            staleIndexes.UnionWith(stats.StaleIndexes);

            StaleIndexes = staleIndexes.ToArray();
        }

        CountOfStaleIndexes = StaleIndexes?.Length ?? 0;

        LastEtag = -1;
        GlobalChangeVector = null;

        if (LastIndexingErrorTime == null)
            LastIndexingErrorTime = stats.LastIndexingErrorTime;
        else if (stats.LastIndexingErrorTime > LastIndexingErrorTime)
            LastIndexingErrorTime = stats.LastIndexingErrorTime;

        if (stats.Collections != null)
        {
            Collections ??= new Dictionary<string, DatabaseStatsChanged.ModifiedCollection>(StringComparer.OrdinalIgnoreCase);

            foreach (var kvp in stats.Collections)
            {
                if (Collections.TryGetValue(kvp.Key, out var current) == false)
                {
                    Collections[kvp.Key] = kvp.Value;
                    continue;
                }

                current.CombineWith(kvp.Value, context);
            }
        }
    }
}
