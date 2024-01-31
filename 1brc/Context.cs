using System.Collections.Concurrent;
using System.IO.MemoryMappedFiles;

namespace OneBRC
{
    internal class Context
    {
        public readonly IDictionary<Utf8StringUnsafe, Statistics> Keys;
        public readonly Chunk[] Chunks;
        public readonly MemoryMappedFile MappedFile;

        public Context(Chunk[] chunks, MemoryMappedFile mmf, IDictionary<Utf8StringUnsafe, Statistics> keys)
        {
            Chunks = chunks;
            MappedFile = mmf;
            Keys = keys;
        }

        internal unsafe Statistics Get(ref readonly Utf8StringUnsafe key)
        {
            return Keys[key];
        }
    }
}
