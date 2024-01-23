using System.Diagnostics.CodeAnalysis;
using System.IO.Hashing;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;

namespace OneBRC
{
    internal readonly struct SmallKeyEqualityComparer : IEqualityComparer<SmallKey>
    {
        public unsafe bool Equals(SmallKey x, SmallKey y)
        {
            return x.vector.Equals(y.vector);
        }

        public unsafe int GetHashCode([DisallowNull] SmallKey obj)
        {
            return GetHashCode(obj.vector, 32);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int GetHashCode(Vector256<byte> name, int len)
        {
            return GetHashCode(name.AsInt64()[0], len);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int GetHashCode(long nameBytes, int len)
        {
            return (int)(nameBytes + (nameBytes >> 28)) + len;
        }
    }
}
