using System.Collections.Concurrent;
using System.IO.MemoryMappedFiles;
using System.Runtime.InteropServices;

namespace OneBRC
{
    internal class Context
    {
        public readonly Dictionary<SmallKey, Statistics> SmallKeys;
        public readonly Dictionary<BigKey, Statistics> BigKeys;
        public readonly ConcurrentQueue<Chunk> ChunkQueue;
        public readonly MemoryMappedFile MappedFile;

        public Context(ConcurrentQueue<Chunk> chunkQueue, MemoryMappedFile mmf)
        {
            SmallKeys = new Dictionary<SmallKey, Statistics>(32768, new SmallKeyEqualityComparer());
            BigKeys = new Dictionary<BigKey, Statistics>(32768, new BigKeyEqualityComparer());
            ChunkQueue = chunkQueue;
            MappedFile = mmf;
        }

        internal Statistics GetOrAdd(SmallKey key)
        {
            ref var floats = ref CollectionsMarshal.GetValueRefOrAddDefault(SmallKeys, key, out bool exists);
            if (!exists)
                floats = new Statistics();
            return floats!;
        }
        internal Statistics GetOrAdd(BigKey key)
        {
            ref var floats = ref CollectionsMarshal.GetValueRefOrAddDefault(BigKeys, key, out bool exists);
            if (!exists)
                floats = new Statistics();
            return floats!;
        }

    }
}
