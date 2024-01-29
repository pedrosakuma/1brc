using System.Collections.Concurrent;
using System.IO.MemoryMappedFiles;
using System.Runtime.InteropServices;

namespace OneBRC
{
    internal class Context
    {
        public readonly byte[] Buffer = new byte[1024 * 1024];
        public int Position = 0;
        public readonly Dictionary<Utf8StringUnsafe, Statistics> Keys;
        public readonly ConcurrentQueue<Chunk> ChunkQueue;
        public readonly MemoryMappedFile MappedFile;

        public Context(ConcurrentQueue<Chunk> chunkQueue, MemoryMappedFile mmf)
        {
            Keys = new Dictionary<Utf8StringUnsafe, Statistics>(32768, new Utf8StringUnsafeEqualityComparer());
            ChunkQueue = chunkQueue;
            MappedFile = mmf;
        }

        internal Statistics GetOrAdd(ref readonly Utf8StringUnsafe key, out bool exists)
        {
            ref var floats = ref CollectionsMarshal.GetValueRefOrAddDefault(Keys, key, out exists);
            if (!exists)
                floats = new Statistics();
            return floats!;
        }
    }
}
