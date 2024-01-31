using System.Collections.Concurrent;
using System.Collections.Frozen;
using System.IO.MemoryMappedFiles;

namespace OneBRC
{
    internal class Context
    {
        public readonly FrozenDictionary<int, Statistics> SmallKeys;
        public readonly FrozenDictionary<Utf8StringUnsafe, Statistics> Keys;
        public readonly ConcurrentQueue<Chunk> ChunkQueue;
        public readonly MemoryMappedFile MappedFile;

        public Context(ConcurrentQueue<Chunk> chunkQueue, MemoryMappedFile mmf, FrozenDictionary<int, Statistics> smallKeys, FrozenDictionary<Utf8StringUnsafe, Statistics> keys)
        {
            ChunkQueue = chunkQueue;
            MappedFile = mmf;
            SmallKeys = smallKeys;
            Keys = keys;
        }

        internal unsafe Statistics Get(ref readonly Utf8StringUnsafe key)
        {
            return Keys[key];
        }
    }
}
