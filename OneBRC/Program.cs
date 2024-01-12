using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;

namespace OneBRC;

class Program
{
    static readonly uint NewLineModifier = (uint)Environment.NewLine.Length - 1U;
    static readonly SearchValues<byte> LineBreakAndComma = SearchValues.Create(";\n"u8);

    static unsafe void Main(string[] args)
    {
        var sw = Stopwatch.StartNew();
        string path = args[0].Replace("~", Environment.GetFolderPath(Environment.SpecialFolder.UserProfile));
        int parallelism = Environment.ProcessorCount;
        int chunks = 32000;
        long length = GetFileLength(path);

        using (var mmf = MemoryMappedFile.CreateFromFile(path, FileMode.Open))
        {
            var chunkQueue = new ConcurrentQueue<Chunk>(
                CreateChunks(mmf, chunks, length)
            );

            var contexts = new Context[parallelism];
            var consumers = new Thread[parallelism];
            for (int i = 0; i < parallelism; i++)
            {
                contexts[i] = new Context(chunkQueue, mmf);
                if (Vector512.IsHardwareAccelerated)
                    consumers[i] = new Thread(ConsumeVector512);
                else if (Vector256.IsHardwareAccelerated)
                    consumers[i] = new Thread(ConsumeVector256);
                else
                    consumers[i] = new Thread(ConsumeSlow);
                consumers[i].Start(contexts[i]);
            }
            foreach (var consumer in consumers)
                consumer.Join();

            WriteOrderedStatistics(contexts.First().Ordered, GroupAndAggregateStatistics(contexts));
        }

        Console.WriteLine(sw.Elapsed);
    }

    private static unsafe Chunk[] CreateChunks(MemoryMappedFile mmf, int chunks, long length)
    {
        var result = new List<Chunk>();
        long blockSize = length / (long)chunks;

        using (var va = mmf.CreateViewAccessor(0, 0, MemoryMappedFileAccess.Read))
        {
            long position = 0;
            byte* ptr = (byte*)0;
            va.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);

            while (true)
            {
                long remainder = length - position;
                byte* ptrBlock = ptr + position;
                checked
                {
                    int size = (int)long.Min(blockSize, remainder);
                    if (size == 0)
                        break;

                    var span = new ReadOnlySpan<byte>(ptrBlock, size);
                    int lastIndexOfLineBreak = span.LastIndexOf((byte)'\n');
                    if (lastIndexOfLineBreak == -1)
                        break;

                    result.Add(new Chunk(position, lastIndexOfLineBreak));
                    position += (long)lastIndexOfLineBreak + 1;
                }
            }
        }
        return result.ToArray();
    }


    private static long GetFileLength(string path)
    {
        using (var file = File.OpenRead(path))
            return file.Length;
    }

    private static void WriteOrderedStatistics(List<string> ordered, Dictionary<string, Statistics> final)
    {
        bool first = true;
        Console.Write("{");
        foreach (var item in ordered)
        {
            Statistics statistics = final[item];
            if (first)
                first = false;
            else
                Console.Write(", ");

            Console.Write($"{item}={(statistics.Min / 10f).ToString("0.0")}/{(float)(statistics.Sum / 10f) / statistics.Count:0.0}/{(statistics.Max / 10f).ToString("0.0")}");
        }
        Console.WriteLine("}");
    }

    private static Dictionary<string, Statistics> GroupAndAggregateStatistics(Context[] contexts)
    {
        Dictionary<string, Statistics> final = new Dictionary<string, Statistics>(32768);
        foreach (var context in contexts)
        {
            foreach (var data in context.Keys)
            {
                if (!final.TryGetValue(data.Value.Key, out var stats))
                {
                    stats = new Statistics(data.Value.Key);
                    final.Add(stats.Key, stats);
                }
                stats.Count += data.Value.Count;
                stats.Sum += data.Value.Sum;
                stats.Min = int.Min(stats.Min, data.Value.Min);
                stats.Max = int.Max(stats.Max, data.Value.Max);
            }
        }
        return final;
    }

    private unsafe static void ConsumeSlow(object? obj)
    {
        ArgumentNullException.ThrowIfNull(obj);
        Context context = (Context)obj;

        using (var va = context.MemoryMappedFile.CreateViewAccessor(0, 0, MemoryMappedFileAccess.Read))
        {
            byte* ptr = (byte*)0;
            va.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);
            while (context.ChunkQueue.TryDequeue(out var chunk))
                ConsumeSlow(context, ptr + chunk.Position, chunk.Size);
        }
    }
    private unsafe static void ConsumeVector256(object? obj)
    {
        ArgumentNullException.ThrowIfNull(obj);
        Context context = (Context)obj;

        using (var va = context.MemoryMappedFile.CreateViewAccessor(0, 0, MemoryMappedFileAccess.Read))
        {
            byte* ptr = (byte*)0;
            va.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);
            while (context.ChunkQueue.TryDequeue(out var chunk))
                ConsumeWithVector256(context, ptr + chunk.Position, chunk.Size);
        }
    }
    private unsafe static void ConsumeVector512(object? obj)
    {
        ArgumentNullException.ThrowIfNull(obj);
        Context context = (Context)obj;

        using (var va = context.MemoryMappedFile.CreateViewAccessor(0, 0, MemoryMappedFileAccess.Read))
        {
            byte* ptr = (byte*)0;
            va.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);
            while (context.ChunkQueue.TryDequeue(out var chunk))
                ConsumeWithVector512(context, ptr + chunk.Position, chunk.Size);
        }
    }

    private static unsafe void ConsumeSlow(Context context, byte* ptr, int size)
    {
        Span<Utf8StringUnsafe> data = stackalloc Utf8StringUnsafe[2];

        ref byte searchSpace = ref Unsafe.AsRef<byte>(ptr);

        ref byte currentSearchSpace = ref searchSpace;
        ref byte end = ref Unsafe.Add(ref searchSpace, size);
        SerialRemainder(context, data, 0, ref currentSearchSpace, ref end);
    }

    private static unsafe void ConsumeWithVector512(Context context, byte* ptr, int size)
    {
        Span<Utf8StringUnsafe> data = stackalloc Utf8StringUnsafe[2];
        int dataIndex = 0;

        ref byte searchSpace = ref Unsafe.AsRef<byte>(ptr);

        ref byte currentSearchSpace = ref searchSpace;
        ref byte end = ref Unsafe.Add(ref searchSpace, size);
        ref byte oneVectorAwayFromEnd = ref Unsafe.Subtract(ref end, Vector256<byte>.Count);

        Vector512<byte> lineBreak = Vector512.Create((byte)'\n');
        Vector512<byte> lineComma = Vector512.Create((byte)';');

        while (!Unsafe.IsAddressGreaterThan(ref currentSearchSpace, ref oneVectorAwayFromEnd))
        {
            uint lastIndex = 0;
            var currentSearchSpaceVector = Vector512.LoadUnsafe(ref currentSearchSpace);
            ulong mask = Vector512.BitwiseOr(
                    Vector512.Equals(currentSearchSpaceVector, lineBreak),
                    Vector512.Equals(currentSearchSpaceVector, lineComma)
                ).ExtractMostSignificantBits();
            uint tzcnt = (uint)ulong.TrailingZeroCount(mask);
            while (tzcnt != 64)
            {
                uint foundIndex = tzcnt + 1;

                data[dataIndex] = new Utf8StringUnsafe(
                    (byte*)Unsafe.AsPointer(ref Unsafe.Add(ref currentSearchSpace, lastIndex)),
                    foundIndex - lastIndex - NewLineModifier);

                if (dataIndex == 1)
                {
                    context.GetOrAdd(data[0])
                        .Add(ParseTemperature(data[1].Span));
                }
                dataIndex ^= 1;

                mask = Bmi1.X64.ResetLowestSetBit(mask);
                tzcnt = (uint)ulong.TrailingZeroCount(mask);
                lastIndex = foundIndex;
            }
            currentSearchSpace = ref Unsafe.Add(ref currentSearchSpace, lastIndex);
        }
        SerialRemainder(context, data, dataIndex, ref currentSearchSpace, ref end);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static long ResetLowestSetBit(long value)
    {
        // It's lowered to BLSR on x86
        return value & (value - 1);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static int ResetLowestSetBit(int value)
    {
        // It's lowered to BLSR on x86
        return value & (value - 1);
    }

    private static unsafe void ConsumeWithVector256(Context context, byte* ptr, int size)
    {
        Span<Utf8StringUnsafe> data = stackalloc Utf8StringUnsafe[2];

        int dataIndex = 0;

        ref byte searchSpace = ref Unsafe.AsRef<byte>(ptr);

        ref byte currentSearchSpace = ref searchSpace;
        ref byte end = ref Unsafe.Add(ref searchSpace, size);
        ref byte oneVectorAwayFromEnd = ref Unsafe.Subtract(ref end, Vector256<byte>.Count);

        (int,int)[] indexesBuffer = new (int, int)[16];
        while (!Unsafe.IsAddressGreaterThan(ref currentSearchSpace, ref oneVectorAwayFromEnd))
        {
            uint lastIndex = 0;
            var currentSearchSpaceVector = Vector256.LoadUnsafe(ref currentSearchSpace);
            uint mask = Vector256.BitwiseOr(
                    Vector256.Equals(currentSearchSpaceVector, Vector256.Create((byte)'\n')),
                    Vector256.Equals(currentSearchSpaceVector, Vector256.Create((byte)';'))
                ).ExtractMostSignificantBits();

            uint tzcnt;
            while ((tzcnt = uint.TrailingZeroCount(mask)) != 32)
            {
                uint foundIndex = tzcnt + 1;
                data[dataIndex] = new Utf8StringUnsafe(
                    (byte*)Unsafe.AsPointer(ref Unsafe.Add(ref currentSearchSpace, lastIndex)),
                    foundIndex - lastIndex - NewLineModifier);
                lastIndex = foundIndex;

                if (dataIndex == 1)
                {
                    context.GetOrAdd(data[0])
                        .Add(ParseTemperature(data[1].Span));
                }
                dataIndex ^= 1;
                mask = Bmi1.ResetLowestSetBit(mask);
            }
            currentSearchSpace = ref Unsafe.Add(ref currentSearchSpace, lastIndex);
        }
        SerialRemainder(context, data, dataIndex, ref currentSearchSpace, ref end);
    }

    private static unsafe void SerialRemainder(Context context, Span<Utf8StringUnsafe> data, int dataIndex, ref byte currentSearchSpace, ref byte end)
    {
        if (!Unsafe.IsAddressGreaterThan(ref currentSearchSpace, ref end))
        {
            int lastIndex = 0;
            var remainderSpan = MemoryMarshal.CreateSpan(ref currentSearchSpace, (int)Unsafe.ByteOffset(ref currentSearchSpace, ref end));
            while (true)
            {
                int foundIndex = remainderSpan.IndexOfAny(LineBreakAndComma);
                if (foundIndex == -1)
                    break;

                data[dataIndex] = new Utf8StringUnsafe(
                    (byte*)Unsafe.AsPointer(ref currentSearchSpace),
                    (uint)foundIndex);

                if (dataIndex == 1)
                {
                    context.GetOrAdd(data[0])
                        .Add(ParseTemperature(data[1].Span));
                }
                dataIndex ^= 1;
                lastIndex = foundIndex;
                remainderSpan = remainderSpan.Slice(foundIndex + 1);
                currentSearchSpace = ref Unsafe.Add(ref currentSearchSpace, lastIndex + 1);
            }
            if (remainderSpan.Length > 0)
            {
                data[dataIndex] = new Utf8StringUnsafe(
                    (byte*)Unsafe.AsPointer(ref currentSearchSpace),
                    (uint)remainderSpan.Length);

                context.GetOrAdd(data[0])
                    .Add(ParseTemperature(data[1].Span));
            }
        }
    }

    const long DOT_BITS = 0x10101000;
    const long MAGIC_MULTIPLIER = (100 * 0x1000000 + 10 * 0x10000 + 1);

    /// <summary>
    /// royvanrijn
    /// </summary>
    /// <param name="tempText"></param>
    /// <returns></returns>
    static int ParseTemperature(ReadOnlySpan<byte> tempText)
    {
        ref var r = ref MemoryMarshal.GetReference(tempText);

        long word = Unsafe.As<byte, long>(ref r);
        int decimalSepPos = (int)long.TrailingZeroCount(~word & DOT_BITS);
        long signed = (~word << 59) >> 63;
        long designMask = ~(signed & 0xFF);
        long digits = ((word & designMask) << (28 - decimalSepPos)) & 0x0F000F0F00L;
        long absValue = ((digits * MAGIC_MULTIPLIER) >>> 32) & 0x3FF;
        return (int)((absValue ^ signed) - signed);
        //int currentPosition = 0;
        //int temp;
        //int negative = 1;
        //// Inspired by @yemreinci to unroll this even further
        //if (tempText[currentPosition] == (byte)'-')
        //{
        //    negative = -1;
        //    currentPosition++;
        //}
        //if (tempText[currentPosition + 1] == (byte)'.')
        //    temp = negative * ((tempText[currentPosition] - (byte)'0') * 10 + (tempText[currentPosition + 2] - (byte)'0'));
        //else
        //    temp = negative * ((tempText[currentPosition] - (byte)'0') * 100 + ((tempText[currentPosition + 1] - (byte)'0') * 10 + (tempText[currentPosition + 3] - (byte)'0')));
        //return temp;
    }
}
