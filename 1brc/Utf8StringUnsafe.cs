using System.Diagnostics.CodeAnalysis;
using System.IO.Hashing;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace OneBRC
{
    public unsafe struct Utf8StringUnsafe : IEqualityComparer<Utf8StringUnsafe>, IEquatable<Utf8StringUnsafe>
    {
        private readonly unsafe byte* Pointer;
        internal readonly int Length;
        public ReadOnlySpan<byte> Span => MemoryMarshal.CreateReadOnlySpan(ref PointerRef, (int)Length);
        public readonly ref byte PointerRef => ref Unsafe.AsRef<byte>(Pointer);

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

        [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
        public override int GetHashCode()
        {
            return GetHashCode(this);
        }

        [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
        public bool Equals(Utf8StringUnsafe x, Utf8StringUnsafe y)
        {
            return SpanHelpers.SequenceEqual(ref x.PointerRef, ref y.PointerRef, (nuint)x.Length);
        }

        [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
        public int GetHashCode([DisallowNull] Utf8StringUnsafe obj)
        {
            return XxHash3.HashToUInt64(obj.Span).GetHashCode();
        }

        [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
        public bool Equals(Utf8StringUnsafe other)
        {
            return Equals(this, other);
        }
    }
}
