using System.Runtime.CompilerServices;
using System.Text;

namespace OneBRC
{
    public unsafe struct Utf8StringUnsafe
    {
        internal readonly unsafe byte* Pointer;
        internal readonly int Length;
        public ReadOnlySpan<byte> Span => new ReadOnlySpan<byte>(Pointer, (int)Length);

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
    }
}
