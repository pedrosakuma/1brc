using System.Collections.Concurrent;
using System.Collections.Frozen;
using System.IO.MemoryMappedFiles;

namespace OneBRC
{
    internal class Context
    {
        public readonly FrozenDictionary<Utf8StringUnsafe, Statistics> Keys;
        public readonly ConcurrentQueue<Chunk> ChunkQueue;
        public readonly MemoryMappedFile MappedFile;

        public Context(ConcurrentQueue<Chunk> chunkQueue, MemoryMappedFile mmf, FrozenDictionary<Utf8StringUnsafe, Statistics> keys)
        {
            ChunkQueue = chunkQueue;
            MappedFile = mmf;
            Keys = keys;
        }

        internal unsafe Statistics Get(ref readonly Utf8StringUnsafe key)
        {
            return Keys[key];
        }
    }
}
