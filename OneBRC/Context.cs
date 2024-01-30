using System.Collections.Concurrent;
using System.IO.MemoryMappedFiles;

namespace OneBRC
{
    internal class Context
    {
        public readonly IDictionary<Utf8StringUnsafe, Statistics> Keys;
        public readonly ConcurrentQueue<Chunk> ChunkQueue;
        public readonly MemoryMappedFile MappedFile;

        public Context(ConcurrentQueue<Chunk> chunkQueue, MemoryMappedFile mmf, IDictionary<Utf8StringUnsafe, Statistics> keys)
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
