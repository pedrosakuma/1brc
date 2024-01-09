using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace OneBRC
{
    internal class Context
    {
        public readonly MemoryMappedFile MemoryMappedFile;
        public readonly Dictionary<Utf8StringUnsafe, Statistics> Keys;
        public readonly List<Utf8StringUnsafe> Ordered;
        public long Position { get; }
        public int Size { get; }

        public Context(MemoryMappedFile mmf, long position, int size)
        {
            Keys = new Dictionary<Utf8StringUnsafe, Statistics>(512);
            Ordered = new List<Utf8StringUnsafe>(512);
            MemoryMappedFile = mmf;
            Position = position;
            Size = size;
        }

        internal ref Statistics GetOrAdd(Utf8StringUnsafe key)
        {
            ref Statistics? floats = ref CollectionsMarshal.GetValueRefOrAddDefault(Keys, key, out bool exists);
            if(!exists)
            {
                floats = new Statistics(key);
                Ordered.Insert(~Ordered.BinarySearch(key), key);
            }
#pragma warning disable CS8619 // Nullability of reference types in value doesn't match target type.
            return ref floats;
#pragma warning restore CS8619 // Nullability of reference types in value doesn't match target type.
        }
    }
}
