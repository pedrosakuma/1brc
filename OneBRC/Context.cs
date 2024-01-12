using System;
using System.Collections.Concurrent;
using System.IO.MemoryMappedFiles;
using System.Runtime.InteropServices;
using System.Text;

namespace OneBRC
{
    internal class Context
    {
        public readonly Dictionary<Utf8StringUnsafe, Statistics> Keys;
        public readonly List<string> Ordered;
        public readonly ConcurrentQueue<Chunk> ChunkQueue;
        public readonly MemoryMappedFile MemoryMappedFile;

        public Context(ConcurrentQueue<Chunk> chunkQueue, MemoryMappedFile mmf)
        {
            Keys = new Dictionary<Utf8StringUnsafe, Statistics>(8192, new Utf8StringUnsafeEqualityComparer());
            Ordered = new List<string>(512);
            ChunkQueue = chunkQueue;
            MemoryMappedFile = mmf;
        }

        internal Statistics GetOrAdd(in Utf8StringUnsafe key)
        {
            ref var floats = ref CollectionsMarshal.GetValueRefOrAddDefault(Keys, key, out bool exists);
            if (!exists)
            {
                var s = Encoding.UTF8.GetString(key.Span);
                floats = new Statistics(s);
                Ordered.Insert(~Ordered.BinarySearch(s), s);
            }
            return floats;
        }
    }
}
