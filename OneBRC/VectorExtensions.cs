using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;

namespace OneBRC;

public unsafe static class VectorExtensions
{
    static byte* firstNMask = (byte*)GCHandle.Alloc(new byte[] {
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    000, 000, 000, 000, 000, 000, 000, 000, 000, 000, 000, 000, 000, 000, 000, 000,
    000, 000, 000, 000, 000, 000, 000, 000, 000, 000, 000, 000, 000, 000, 000, 000 }, GCHandleType.Pinned).AddrOfPinnedObject() + 32;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Vector256<byte> GetLeftMask(uint length)
    {
        return Vector256.Create(new ReadOnlySpan<byte>(firstNMask - length, 32));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Vector256<byte> MaskLeftBytes(this Vector256<byte> data, uint length)
    {
        return Vector256.BitwiseAnd(GetLeftMask(length), data);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Vector256<byte> MaskLeftBytes(this Vector256<byte> data, uint length, out Vector256<byte> mask)
    {
        mask = GetLeftMask(length);
        return Vector256.BitwiseAnd(mask, data);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static uint IndexOf(this Vector256<byte> data, byte searchValue)
    {
        return IndexOf(Vector256.Create(searchValue), data);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static uint IndexOf(this Vector256<byte> searchConstant, Vector256<byte> data)
    {
        var sMatches = Vector256.Equals(data, searchConstant);
        uint sMask = sMatches.ExtractMostSignificantBits();
        return uint.TrailingZeroCount(sMask);
    }
}
