using System.Diagnostics.CodeAnalysis;
using System.IO.Hashing;
using System.Numerics;
using System.Runtime.CompilerServices;

namespace OneBRC
{
    internal readonly struct Utf8StringUnsafeEqualityComparer : IEqualityComparer<Utf8StringUnsafe>
    {
        public unsafe bool Equals(Utf8StringUnsafe x, Utf8StringUnsafe y)
        {
            return SpanHelpers.SequenceEqual(ref Unsafe.AsRef<byte>(x.Pointer), ref Unsafe.AsRef<byte>(y.Pointer), x.Length);
        }

        public unsafe int GetHashCode([DisallowNull] Utf8StringUnsafe obj)
        {
            return (int)XxHash3.HashToUInt64(obj.Span).GetHashCode();
        }
    }
}
