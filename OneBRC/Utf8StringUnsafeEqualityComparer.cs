using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;

namespace OneBRC
{
    internal readonly struct Utf8StringUnsafeEqualityComparer : IEqualityComparer<Utf8StringUnsafe>
    {
        private const uint Hash1Start = (5381 << 16) + 5381;
        private const uint Factor = 1_566_083_941;

        public unsafe bool Equals(Utf8StringUnsafe x, Utf8StringUnsafe y)
        {
            return SpanHelpers.SequenceEqual(ref Unsafe.AsRef<byte>(x.Pointer), ref Unsafe.AsRef<byte>(y.Pointer), x.Length);
        }

        public unsafe int GetHashCode([DisallowNull] Utf8StringUnsafe obj)
        {
            return Marvin.ComputeHash32(ref Unsafe.AsRef<byte>(obj.Pointer), obj.Length, Hash1Start, Factor);
        }

        public unsafe int GetHashCodeSwar([DisallowNull] Utf8StringUnsafe obj)
        {
            int length = (int)obj.Length;
            uint hash1, hash2;
            hash1 = Hash1Start;
            hash2 = (BitOperations.RotateLeft(hash1, 5) + hash1) ^ obj.Length;

            ref var r = ref Unsafe.AsRef<uint>(obj.Pointer);
            while (length >= 8)
            {
                hash1 = (BitOperations.RotateLeft(hash1, 5) + hash1) ^ r;
                hash2 = (BitOperations.RotateLeft(hash2, 5) + hash2) ^ Unsafe.Add(ref r, 1);
                r = ref Unsafe.Add(ref r, 2);
                length -= 8;
            }
            ref var b = ref Unsafe.As<uint, byte>(ref r);
            while (length-- > 0)
            {
                hash2 = (BitOperations.RotateLeft(hash2, 5) + hash2) ^ Unsafe.Add(ref b, length + 1);
            }
            return (int)(hash1 + (hash2 * Factor));
        }
    }
}
