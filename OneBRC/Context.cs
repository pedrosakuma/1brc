﻿using System.Runtime.InteropServices;
using System.Text;

namespace OneBRC
{
    internal class Context
    {
        public readonly Dictionary<int, Statistics> Keys;
        public readonly List<string> Ordered;
        public readonly int[] Indexes;
        public readonly int[] Lengths;
        public readonly int MaxBlockBufferSize;

        public unsafe ReadOnlySpan<byte> BlockSpan => new ReadOnlySpan<byte>(BlockPointer, BlockBufferSize);


        public unsafe byte* BlockPointer { set; private get; }
        public int BlockBufferSize;
        public int LinesCount;
        public Context(int maxBlockBufferSize)
        {
            Keys = new Dictionary<int, Statistics>(512);
            Ordered = new List<string>(512);
            MaxBlockBufferSize = maxBlockBufferSize;
            Indexes = new int[131072];
            Lengths = new int[131072];
        }

        internal Statistics GetOrAdd(ReadOnlySpan<byte> span)
        {
            int keyHashCode = GetHashCode(span);
            if (!Keys.TryGetValue(keyHashCode, out var floats))
            {
                var key = Encoding.UTF8.GetString(span);
                floats = new Statistics(key);
                Keys.Add(keyHashCode, floats);
                Ordered.Insert(~Ordered.BinarySearch(key), key);
            }
            return floats;
        }

        static int GetHashCode(ReadOnlySpan<byte> span)
        {
            int hashCode = 0;
            for (int i = 0; i < span.Length; i++)
                hashCode = 31 * hashCode + span[i];
            return hashCode;
        }

    }
}