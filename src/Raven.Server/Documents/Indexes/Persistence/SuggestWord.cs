﻿using System;

namespace Raven.Server.Documents.Indexes.Persistence
{
    /// <summary>  SuggestWord Class, used in suggestSimilar method in SpellChecker class.
    /// 
    /// </summary>
    /// <author>  Nicolas Maisonneuve
    /// </author>
    internal sealed class SuggestWord : IComparable
    {
        /// <summary> the score of the word</summary>
        public float Score;

        /// <summary> The freq of the word</summary>
        public int Freq;

        /// <summary> the suggested word</summary>
        public string Term;

        public int CompareTo(SuggestWord a)
        {
            //first criteria: the edit distance
            if (Score > a.Score)
            {
                return 1;
            }
            if (Score < a.Score)
            {
                return -1;
            }

            //second criteria (if first criteria is equal): the popularity
            if (Freq > a.Freq)
            {
                return 1;
            }

            if (Freq < a.Freq)
            {
                return -1;
            }

            return 0;
        }

        public int CompareTo(object obj)
        {
            if (obj is SuggestWord sw == false)
                throw new NotSupportedException($"Cannot compare to: {obj.GetType().FullName}");

            return CompareTo(sw);
        }
    }
}
