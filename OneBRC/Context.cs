using System.Collections.Concurrent;
using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace OneBRC
{
    internal class Context
    {
        private readonly byte[] Utf8StringUnsafeBuffer = new byte[1024 * 1024];
        private int BufferPosition = 0;
        public readonly Dictionary<Utf8StringUnsafe, Statistics> Keys;
        public readonly ConcurrentQueue<Chunk> ChunkQueue;
        public readonly MemoryMappedFile MappedFile;

        public Context(ConcurrentQueue<Chunk> chunkQueue, MemoryMappedFile mmf)
        {
            Keys = new Dictionary<Utf8StringUnsafe, Statistics>(262144);
            ChunkQueue = chunkQueue;
            MappedFile = mmf;
        }

        internal unsafe Statistics GetOrAdd(ref readonly Utf8StringUnsafe key)
        {
            if (!Keys.TryGetValue(key, out var floats))
            {
                ref var buffer = ref MemoryMarshal.GetArrayDataReference(Utf8StringUnsafeBuffer);
                ref var destination = ref Unsafe.Add(ref buffer, BufferPosition);
                Unsafe.CopyBlockUnaligned(ref destination, ref key.PointerRef, (uint)key.Length);
                floats = new Statistics();
                Keys.Add(new Utf8StringUnsafe(ref destination, key.Length), floats);
                BufferPosition += key.Length;
            }
            return floats!;
        }
    }
}
