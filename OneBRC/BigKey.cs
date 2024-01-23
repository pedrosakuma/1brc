using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;
using System.Text;

namespace OneBRC;

public readonly struct BigKey
{
    internal readonly Vector256<byte> vector0;
    internal readonly Vector256<byte> vector1;
    internal readonly Vector256<byte> vector2;
    internal readonly Vector256<byte> vector3;
    internal readonly uint length;

    public BigKey(Vector256<byte> vector0, Vector256<byte> vector1, Vector256<byte> vector2, Vector256<byte> vector3, uint length)
    {
        this.vector0 = vector0;
        this.vector1 = vector1;
        this.vector2 = vector2;
        this.vector3 = vector3;
        this.length = length;
    }

    public unsafe override string ToString()
    {
        char* chars = stackalloc char[(int)length];
        byte* bytes0 = (byte*)Unsafe.AsPointer(ref Unsafe.AsRef(in vector0));
        byte* bytes1 = (byte*)Unsafe.AsPointer(ref Unsafe.AsRef(in vector1));
        byte* bytes2 = (byte*)Unsafe.AsPointer(ref Unsafe.AsRef(in vector2));
        byte* bytes3 = (byte*)Unsafe.AsPointer(ref Unsafe.AsRef(in vector3));
        int currentLength = (int)length & 31;
        int written = Encoding.UTF8.GetChars(bytes0, currentLength, chars, currentLength);
        currentLength = (int)(length >> 5) & 31;
        written += Encoding.UTF8.GetChars(bytes1, currentLength, chars + written, currentLength);
        currentLength = (int)(length >> 10) & 31;
        written += Encoding.UTF8.GetChars(bytes1, currentLength, chars + written, currentLength);
        currentLength = (int)(length >> 15) & 31;
        written += Encoding.UTF8.GetChars(bytes1, currentLength, chars + written, currentLength);
        return new string(chars, 0, written);
    }
}