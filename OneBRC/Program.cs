using System.Buffers;
using System.Diagnostics;
using System.IO.MemoryMappedFiles;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Text;

namespace OneBRC;

class Program
{
    static readonly int NewLineModifier = Environment.NewLine.Length - 1;
    static readonly SearchValues<byte> LineBreakAndComma = SearchValues.Create(";\n"u8);

    static unsafe void Main(string[] args)
    {
        var sw = Stopwatch.StartNew();
        string path = args[0].Replace("~", Environment.GetFolderPath(Environment.SpecialFolder.UserProfile));
        int parallelism = Environment.ProcessorCount;
        long length = GetFileLength(path);

        if (Vector512.IsHardwareAccelerated)
            Console.WriteLine("AVX512");            
        else if (Vector256.IsHardwareAccelerated)
            Console.WriteLine("AVX2");            

        using (var mmf = MemoryMappedFile.CreateFromFile(path, FileMode.Open))
        {
            var consumers = Enumerable.Range(0, parallelism)
                .Select(_ => new Thread(Consume)).ToArray();

            var contexts = MakeContexts(mmf, parallelism, length);
            for (int i = 0; i < consumers.Length; i++)
                consumers[i].Start(contexts[i]);

            foreach (var consumer in consumers)
                consumer.Join();

            WriteOrderedStatistics(contexts.First().Ordered, GroupAndAggregateStatistics(contexts));
        }

        Console.WriteLine(sw.Elapsed);
    }

    private static unsafe List<Context> MakeContexts(MemoryMappedFile mmf, int parallelism, long length)
    {
        var result = new List<Context>(parallelism);
        long blockSize = length / (long)parallelism;

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

                    result.Add(new Context(mmf, position, lastIndexOfLineBreak));
                    position += (long)lastIndexOfLineBreak + 1;
                }
            }
        }
        return result;
    }


    private static long GetFileLength(string path)
    {
        using (var file = File.OpenRead(path))
            return file.Length;
    }

    private static void WriteOrderedStatistics(List<Utf8StringUnsafe> ordered, Dictionary<string, Statistics> final)
    {
        bool first = true;
        Console.Write("{");
        foreach (var item in ordered)
        {
            var key = Encoding.UTF8.GetString(item.Span);
            Statistics statistics = final[key];
            if (first)
                first = false;
            else
                Console.Write(", ");

            Console.Write($"{key}={(statistics.Min / 10f).ToString("0.0")}/{(float)(statistics.Sum / 10f) / statistics.Count:0.0}/{(statistics.Max / 10f).ToString("0.0")}");
        }
        Console.WriteLine("}");
    }

    private static Dictionary<string, Statistics> GroupAndAggregateStatistics(List<Context> contexts)
    {
        Dictionary<string, Statistics> final = new Dictionary<string, Statistics>();
        foreach (var context in contexts)
        {
            foreach (var data in context.Keys)
            {
                if (!final.TryGetValue(Encoding.UTF8.GetString(data.Value.Key.Span), out var stats))
                {
                    stats = new Statistics(data.Value.Key);
                    final.Add(Encoding.UTF8.GetString(stats.Key.Span), stats);
                }
                stats.Count += data.Value.Count;
                stats.Sum += data.Value.Sum;
                stats.Min = int.Min(stats.Min, data.Value.Min);
                stats.Max = int.Max(stats.Max, data.Value.Max);
            }
        }

        return final;
    }

    private unsafe static void Consume(object? obj)
    {
        ArgumentNullException.ThrowIfNull(obj);
        var c = (Context)obj;

        using (var va = c.MemoryMappedFile.CreateViewAccessor(0, 0, MemoryMappedFileAccess.Read))
        {
            byte* ptr = (byte*)0;
            va.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);
            ptr += c.Position;
            if(Vector512.IsHardwareAccelerated)
                ConsumeWithVector512(c, ptr);
            else if(Vector256.IsHardwareAccelerated)
                ConsumeWithVector256(c, ptr);
        }
    }

    private static unsafe void ConsumeWithVector512(Context c, byte* ptr)
    {
        Span<Utf8StringUnsafe> data = stackalloc Utf8StringUnsafe[2];
        int dataIndex = 0;

        ref byte searchSpace = ref Unsafe.AsRef<byte>(ptr);

        ref byte currentSearchSpace = ref searchSpace;
        ref byte end = ref Unsafe.Add(ref searchSpace, c.Size);
        ref byte oneVectorAwayFromEnd = ref Unsafe.Subtract(ref end, Vector256<byte>.Count);

        Vector512<byte> lineBreak = Vector512.Create((byte)'\n');
        Vector512<byte> lineComma = Vector512.Create((byte)';');

        while (!Unsafe.IsAddressGreaterThan(ref currentSearchSpace, ref oneVectorAwayFromEnd))
        {
            int lastIndex = 0;
            var currentSearchSpaceVector = Vector512.LoadUnsafe(ref currentSearchSpace);
            long mask = (long)Vector512.BitwiseOr(
                    Vector512.Equals(currentSearchSpaceVector, lineBreak),
                    Vector512.Equals(currentSearchSpaceVector, lineComma)
                ).ExtractMostSignificantBits();
            int tzcnt = BitOperations.TrailingZeroCount(mask);
            while (tzcnt != 64)
            {
                int foundIndex = tzcnt + 1;

                data[dataIndex] = new Utf8StringUnsafe(
                    (byte*)Unsafe.AsPointer(ref Unsafe.Add(ref currentSearchSpace, lastIndex)),
                    foundIndex - lastIndex - NewLineModifier);

                if (dataIndex == 1)
                {
                    c.GetOrAdd(data[0])
                        .Add(ParseTemperature(data[1].Span));
                }
                dataIndex ^= 1;

                mask = ResetLowestSetBit(mask);
                tzcnt = (int)long.TrailingZeroCount(mask);
                lastIndex = foundIndex;
            }
            currentSearchSpace = ref Unsafe.Add(ref currentSearchSpace, lastIndex);
        }
        SerialRemainder(c, data, dataIndex, ref currentSearchSpace, ref end);
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

    private static unsafe void ConsumeWithVector256(Context c, byte* ptr)
    {
        Span<Utf8StringUnsafe> data = stackalloc Utf8StringUnsafe[2];
        int dataIndex = 0;
        
        ref byte searchSpace = ref Unsafe.AsRef<byte>(ptr);

        ref byte currentSearchSpace = ref searchSpace;
        ref byte end = ref Unsafe.Add(ref searchSpace, c.Size);
        ref byte oneVectorAwayFromEnd = ref Unsafe.Subtract(ref end, Vector256<byte>.Count);

        Vector256<byte> lineBreak = Vector256.Create((byte)'\n');
        Vector256<byte> lineComma = Vector256.Create((byte)';');

        while (!Unsafe.IsAddressGreaterThan(ref currentSearchSpace, ref oneVectorAwayFromEnd))
        {
            int lastIndex = 0;
            var currentSearchSpaceVector = Vector256.LoadUnsafe(ref currentSearchSpace);
            int mask = (int)Vector256.BitwiseOr(
                    Vector256.Equals(currentSearchSpaceVector, lineBreak),
                    Vector256.Equals(currentSearchSpaceVector, lineComma)
                ).ExtractMostSignificantBits();
            int tzcnt = BitOperations.TrailingZeroCount(mask);
            while (tzcnt != 32)
            {
                int foundIndex = tzcnt + 1;

                data[dataIndex] = new Utf8StringUnsafe(
                    (byte*)Unsafe.AsPointer(ref Unsafe.Add(ref currentSearchSpace, lastIndex)),
                    foundIndex - lastIndex - NewLineModifier);

                if (dataIndex == 1)
                {
                    c.GetOrAdd(data[0])
                        .Add(ParseTemperature(data[1].Span));
                }
                dataIndex ^= 1;

                mask = ResetLowestSetBit(mask);
                tzcnt = int.TrailingZeroCount(mask);
                lastIndex = foundIndex;
            }
            currentSearchSpace = ref Unsafe.Add(ref currentSearchSpace, lastIndex);
        }
        SerialRemainder(c, data, dataIndex, ref currentSearchSpace, ref end);
    }

    private static unsafe void SerialRemainder(Context c, Span<Utf8StringUnsafe> data, int dataIndex, ref byte currentSearchSpace, ref byte end)
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
                    foundIndex);

                if (dataIndex == 1)
                {
                    c.GetOrAdd(data[0])
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
                    remainderSpan.Length);

                c.GetOrAdd(data[0])
                    .Add(ParseTemperature(data[1].Span));
            }
        }
    }

    static readonly long DOT_BITS = 0x10101000;
    static readonly long MAGIC_MULTIPLIER = (100 * 0x1000000 + 10 * 0x10000 + 1);

    /// <summary>
    /// royvanrijn
    /// </summary>
    /// <param name="tempText"></param>
    /// <returns></returns>
    static int ParseTemperature(ReadOnlySpan<byte> tempText)
    {
        ref readonly var r = ref MemoryMarshal.AsRef<byte>(tempText);

        long word = MemoryMarshal.AsRef<long>(MemoryMarshal.CreateReadOnlySpan(in r, sizeof(long)));
        long invWord = ~word;
        int decimalSepPos = (int)long.TrailingZeroCount(invWord & DOT_BITS);
        long signed = (invWord << 59) >> 63;
        long designMask = ~(signed & 0xFF);
        long digits = ((word & designMask) << (28 - decimalSepPos)) & 0x0F000F0F00L;
        long absValue = ((digits * MAGIC_MULTIPLIER) >>> 32) & 0x3FF;
        return (int)((absValue ^ signed) - signed);
    }
}
