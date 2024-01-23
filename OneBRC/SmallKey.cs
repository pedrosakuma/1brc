using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;
using System.Text;

namespace OneBRC;

public readonly struct SmallKey
{
    internal readonly Vector256<byte> vector;
    internal readonly uint length;

    public SmallKey(Vector256<byte> vector, uint length)
    {
        this.vector = vector;
        this.length = length;
    }

    public unsafe override string ToString()
    {
        byte* bytes = (byte*)Unsafe.AsPointer(ref Unsafe.AsRef(in vector));
        return Encoding.UTF8.GetString(bytes, (int)length);
    }
}
