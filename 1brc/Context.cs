using System.Collections.Concurrent;
using System.Collections.Frozen;
using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;

namespace OneBRC
{
    internal class Context
    {
        public readonly FrozenDictionary<int, Statistics> SmallKeys;
        public readonly FrozenDictionary<Utf8StringUnsafe, Statistics> Keys;
        public readonly Chunks Chunks;
        public readonly MemoryMappedFile MappedFile;

        public Context(Chunks chunks, MemoryMappedFile mmf, FrozenDictionary<int, Statistics> smallKeys, FrozenDictionary<Utf8StringUnsafe, Statistics> keys)
        {
            Chunks = chunks;
            MappedFile = mmf;
            SmallKeys = smallKeys;
            Keys = keys;
        }

        internal unsafe Statistics Get(ref readonly Utf8StringUnsafe key)
        {
            switch (key.Length)
            {
                case 1:
                case 2:
                case 3:
                    {
                        int smallKey = Unsafe.ReadUnaligned<int>(ref key.PointerRef) & (1 << (key.Length * 8)) - 1;
                        return SmallKeys[smallKey];
                    }
                case 4:
                    {
                        int smallKey = Unsafe.ReadUnaligned<int>(ref key.PointerRef);
                        return SmallKeys[smallKey];
                    }
                default:
                    return Keys[key];
            }
        }
    }
}
