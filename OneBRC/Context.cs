using Microsoft.Win32.SafeHandles;
using System.Collections.Concurrent;
using System.IO.MemoryMappedFiles;
using System.Runtime.InteropServices;
using System.Text;

namespace OneBRC
{
    internal class Context : IDisposable
    {
        public readonly Dictionary<Utf8StringUnsafe, Statistics> Keys;
        public readonly ConcurrentQueue<Chunk> ChunkQueue;
        private readonly SafeFileHandle FileHandle;
        public readonly MemoryMappedFile MemoryMappedFile;

        public Context(ConcurrentQueue<Chunk> chunkQueue, int index, string path)
        {
            Keys = new Dictionary<Utf8StringUnsafe, Statistics>(8192, new Utf8StringUnsafeEqualityComparer());
            ChunkQueue = chunkQueue;
            FileHandle = File.OpenHandle(path, FileMode.Open, FileAccess.Read, FileShare.Read, FileOptions.RandomAccess);
            MemoryMappedFile = MemoryMappedFile.CreateFromFile(FileHandle, $"{Path.GetFileName(path)}_{index}", 0, MemoryMappedFileAccess.Read, HandleInheritability.None, true);
        }

        internal Statistics GetOrAdd(in Utf8StringUnsafe key)
        {
            ref var floats = ref CollectionsMarshal.GetValueRefOrAddDefault(Keys, key, out bool exists);
            if (!exists)
                floats = new Statistics();
            return floats!;
        }

        public void Dispose()
        {
            MemoryMappedFile.Dispose();
            FileHandle.Dispose();
        }
    }
}
