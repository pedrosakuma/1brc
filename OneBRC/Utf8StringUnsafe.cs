using System.Diagnostics.CodeAnalysis;
using System.IO.Hashing;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace OneBRC
{
    public unsafe struct Utf8StringUnsafe : IEqualityComparer<Utf8StringUnsafe>, IEquatable<Utf8StringUnsafe>
    {
        internal readonly unsafe byte* Pointer;
        internal readonly int Length;
        public ReadOnlySpan<byte> Span => MemoryMarshal.CreateReadOnlySpan(ref Unsafe.AsRef<byte>(Pointer), (int)Length);

        public Utf8StringUnsafe(ref byte pointer, int length)
            : this((byte*)Unsafe.AsPointer(ref pointer), length)
        {
        }

        public Utf8StringUnsafe(byte* pointer, int length)
        {
            this.Pointer = pointer;
            this.Length = length;
        }

        public override string ToString()
        {
            return Encoding.UTF8.GetString(Span);
        }

        public override int GetHashCode()
        {
            return GetHashCode(this);
        }

        public bool Equals(Utf8StringUnsafe x, Utf8StringUnsafe y)
        {
            return x.Length == y.Length
            && Unsafe.AsRef<byte>(x.Pointer) == Unsafe.AsRef<byte>(y.Pointer);
            //return SpanHelpers.SequenceEqual(ref Unsafe.AsRef<byte>(x.Pointer), ref Unsafe.AsRef<byte>(y.Pointer), (nuint)x.Length);
        }

        public int GetHashCode([DisallowNull] Utf8StringUnsafe obj)
        {
            return XxHash3.HashToUInt64(obj.Span).GetHashCode();
        }

        public bool Equals(Utf8StringUnsafe other)
        {
            return Equals(this, other);
        }
    }
}
