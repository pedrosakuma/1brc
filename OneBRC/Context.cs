using System.Collections.Concurrent;
using System.IO.MemoryMappedFiles;
using System.Runtime.InteropServices;

namespace OneBRC
{
    internal class Context
    {
        public readonly Dictionary<Utf8StringUnsafe, Statistics> Keys;
        public readonly ConcurrentQueue<Chunk> ChunkQueue;
        public readonly MemoryMappedFile MappedFile;

        public Context(ConcurrentQueue<Chunk> chunkQueue, MemoryMappedFile mmf)
        {
            Keys = new Dictionary<Utf8StringUnsafe, Statistics>(262144, new Utf8StringUnsafeEqualityComparer());
            ChunkQueue = chunkQueue;
            MappedFile = mmf;
        }

        internal Statistics GetOrAdd(ref readonly Utf8StringUnsafe key)
        {
            ref var floats = ref CollectionsMarshal.GetValueRefOrAddDefault(Keys, key, out bool exists);
            if (!exists)
                floats = new Statistics();
            return floats!;
        }
    }
}
