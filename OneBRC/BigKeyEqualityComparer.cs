using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;

namespace OneBRC
{
    internal readonly struct BigKeyEqualityComparer : IEqualityComparer<BigKey>
    {
        public unsafe bool Equals(BigKey x, BigKey y)
        {
            return x.vector0.Equals(y.vector0) 
                && x.vector1.Equals(y.vector1)
                && x.vector2.Equals(y.vector2)
                && x.vector3.Equals(y.vector3);
        }

        public unsafe int GetHashCode([DisallowNull] BigKey obj)
        {
            return GetHashCode(obj.vector0, obj.length);
            //return HashCode.Combine(
            //    GetHashCode(obj.vector0, obj.length),
            //    GetHashCode(obj.vector1, obj.length),
            //    GetHashCode(obj.vector2, obj.length),
            //    GetHashCode(obj.vector3, obj.length)
            //);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int GetHashCode(Vector256<byte> name, uint len)
        {
            return GetHashCode(name.AsInt64()[0], len);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int GetHashCode(long nameBytes, uint len)
        {
            return (int)((nameBytes + (nameBytes >> 28)) + len);
        }
    }
}
