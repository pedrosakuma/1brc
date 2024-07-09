using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;

namespace OneBRC;

public unsafe static class VectorExtensions
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Vector256<byte> GetLeftMask(uint length)
    {
        ReadOnlySpan<byte> firstNMask = [
            255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
            255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
            000, 000, 000, 000, 000, 000, 000, 000, 000, 000, 000, 000, 000, 000, 000, 000,
            000, 000, 000, 000, 000, 000, 000, 000, 000, 000, 000, 000, 000, 000, 000, 000 ];
        return Vector256.Create(firstNMask.Slice(32 - (int)length, 32));
    }
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Vector256<byte> MaskLeftBytes(this Vector256<byte> data, uint length)
    {
        return Vector256.BitwiseAnd(GetLeftMask(length), data);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static uint IndexOf(this Vector256<byte> searchConstant, Vector256<byte> data)
    {
        return uint.TrailingZeroCount(
            Vector256.Equals(data, searchConstant)
                .ExtractMostSignificantBits()
            );
    }
}
