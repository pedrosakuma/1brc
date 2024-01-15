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
    static readonly SearchValues<byte> LineBreakAndComma = SearchValues.Create(";\n"u8);

    static unsafe void Main(string[] args)
    {
        var sw = Stopwatch.StartNew();
        string path = args[0].Replace("~", Environment.GetFolderPath(Environment.SpecialFolder.UserProfile));
        int parallelism = Environment.ProcessorCount;
        int chunks = Environment.ProcessorCount * 2000;
        long length = GetFileLength(path);

        var contexts = new Context[parallelism];
        var consumers = new Thread[parallelism];

        ConcurrentQueue<Chunk> chunkQueue;
        using (var fileHandle = File.OpenHandle(path, FileMode.Open, FileAccess.Read, FileShare.Read, FileOptions.RandomAccess))
        using (var mmf = MemoryMappedFile.CreateFromFile(fileHandle, Path.GetFileName(path), 0, MemoryMappedFileAccess.Read, HandleInheritability.None, true))
        {
            chunkQueue = new ConcurrentQueue<Chunk>(
                CreateChunks(mmf, chunks, length)
            );
            for (int i = 0; i < parallelism; i++)
            {
                int index = i;
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

            WriteOrderedStatistics(GroupAndAggregateStatistics(contexts));
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

    private static void WriteOrderedStatistics(Dictionary<string, Statistics> final)
    {
        bool first = true;
        var c = Console.Out;
        c.Write("{");
        foreach (var item in final.Keys.Order())
        {
            Statistics statistics = final[item];
            if (first)
                first = false;
            else
                c.Write(", ");

            c.Write($"{item}={(statistics.Min / 10f).ToString("0.0")}/{(float)(statistics.Sum / 10f) / statistics.Count:0.0}/{(statistics.Max / 10f).ToString("0.0")}");
        }
        c.WriteLine("}");
        c.Flush();
    }

    private static Dictionary<string, Statistics> GroupAndAggregateStatistics(Context[] contexts)
    {
        Dictionary<string, Statistics> final = new Dictionary<string, Statistics>(32768);
        foreach (var context in contexts)
        {
            foreach (var data in context.Keys)
            {
                string key = data.Key.ToString();
                if (!final.TryGetValue(key, out var stats))
                {
                    stats = new Statistics();
                    final.Add(key, stats);
                }
                stats.Count += data.Value.Count;
                stats.Sum += data.Value.Sum;
                stats.Min = short.Min(stats.Min, data.Value.Min);
                stats.Max = short.Max(stats.Max, data.Value.Max);
            }
        }
        return final;
    }

    private unsafe static void ConsumeSlow(object? obj)
    {
        ArgumentNullException.ThrowIfNull(obj);
        Context context = (Context)obj;

        using (var va = context.MappedFile.CreateViewAccessor(0, 0, MemoryMappedFileAccess.Read))
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

        using (var va = context.MappedFile.CreateViewAccessor(0, 0, MemoryMappedFileAccess.Read))
        {
            byte* ptr = (byte*)0;
            va.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);
            while (context.ChunkQueue.TryDequeue(out var chunk))
                ConsumeWithVector256(context, ref Unsafe.AsRef<byte>(ptr + chunk.Position), chunk.Size);
        }
    }
    private unsafe static void ConsumeVector512(object? obj)
    {
        ArgumentNullException.ThrowIfNull(obj);
        Context context = (Context)obj;

        using (var va = context.MappedFile.CreateViewAccessor(0, 0, MemoryMappedFileAccess.Read))
        {
            byte* ptr = (byte*)0;
            va.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);
            while (context.ChunkQueue.TryDequeue(out var chunk))
                ConsumeWithVector512(context, ref Unsafe.AsRef<byte>(ptr + chunk.Position), chunk.Size);
        }
    }

    private static unsafe void ConsumeSlow(Context context, byte* ptr, int size)
    {
        Utf8StringUnsafe[] data = new Utf8StringUnsafe[16];
        ref var dataRef = ref MemoryMarshal.GetArrayDataReference(data);

        ref byte searchSpace = ref Unsafe.AsRef<byte>(ptr);

        ref byte currentSearchSpace = ref searchSpace;
        ref byte end = ref Unsafe.Add(ref searchSpace, size);
        SerialRemainder(context, ref dataRef, 0, ref currentSearchSpace, ref end);
    }

    private static void ConsumeWithVector512(Context context, ref byte searchSpace, int size)
    {
        Utf8StringUnsafe[] data = new Utf8StringUnsafe[16];
        ref var dataRef = ref MemoryMarshal.GetArrayDataReference(data);
        int dataIndex = 0;

        ref byte currentSearchSpace = ref searchSpace;
        ref byte end = ref Unsafe.Add(ref searchSpace, size);
        ref byte oneVectorAwayFromEnd = ref Unsafe.Subtract(ref end, Vector256<byte>.Count);

        while (!Unsafe.IsAddressGreaterThan(ref currentSearchSpace, ref oneVectorAwayFromEnd))
        {
            uint lastIndex = 0;
            var currentSearchSpaceVector = Vector512.LoadUnsafe(ref currentSearchSpace);
            ulong mask = Vector512.BitwiseOr(
                    Vector512.Equals(currentSearchSpaceVector, Vector512.Create((byte)'\n')),
                    Vector512.Equals(currentSearchSpaceVector, Vector512.Create((byte)';'))
                ).ExtractMostSignificantBits();
            if (mask != 0)
            {
                uint tzcnt;
                while ((tzcnt = (uint)ulong.TrailingZeroCount(mask)) != 64)
                {
                    uint foundIndex = tzcnt + 1;

                    Unsafe.Add(ref dataRef, dataIndex++) = new Utf8StringUnsafe(
                        ref Unsafe.Add(ref currentSearchSpace, lastIndex),
                        foundIndex - lastIndex - 1);

                    mask = Bmi1.X64.ResetLowestSetBit(mask);
                    lastIndex = foundIndex;
                }
            }
            else
            {
                var remainderSpan = MemoryMarshal.CreateSpan(ref currentSearchSpace, Math.Min((int)Unsafe.ByteOffset(ref currentSearchSpace, ref end), 128));
                int foundIndex = remainderSpan.IndexOfAny(LineBreakAndComma);
                if (foundIndex == -1)
                    break;

                Unsafe.Add(ref dataRef, dataIndex++) = new Utf8StringUnsafe(
                    ref Unsafe.Add(ref currentSearchSpace, lastIndex),
                    (uint)foundIndex - lastIndex - 1);
                lastIndex = (uint)foundIndex + 1;
            }
            dataIndex -= GetOrAddBlock(context, ref dataRef, dataIndex);
            currentSearchSpace = ref Unsafe.Add(ref currentSearchSpace, lastIndex);
        }
        SerialRemainder(context, ref dataRef, dataIndex, ref currentSearchSpace, ref end);
    }

    private static void ConsumeWithVector256(Context context, ref byte searchSpace, int size)
    {
        Utf8StringUnsafe[] data = new Utf8StringUnsafe[16];
        ref var dataRef = ref MemoryMarshal.GetArrayDataReference(data);
        int dataIndex = 0;

        ref byte currentSearchSpace = ref searchSpace;
        ref byte end = ref Unsafe.Add(ref searchSpace, size);
        ref byte oneVectorAwayFromEnd = ref Unsafe.Subtract(ref end, Vector256<byte>.Count);

        while (!Unsafe.IsAddressGreaterThan(ref currentSearchSpace, ref oneVectorAwayFromEnd))
        {
            uint lastIndex = 0;
            var currentSearchSpaceVector = Vector256.LoadUnsafe(ref currentSearchSpace);
            uint mask = Vector256.BitwiseOr(
                    Vector256.Equals(currentSearchSpaceVector, Vector256.Create((byte)'\n')),
                    Vector256.Equals(currentSearchSpaceVector, Vector256.Create((byte)';'))
                ).ExtractMostSignificantBits();
            if (mask != 0)
            {
                uint tzcnt;
                while ((tzcnt = uint.TrailingZeroCount(mask)) != 32)
                {
                    uint foundIndex = tzcnt + 1;
                    Unsafe.Add(ref dataRef, dataIndex++) = new Utf8StringUnsafe(
                        ref Unsafe.Add(ref currentSearchSpace, lastIndex),
                        foundIndex - lastIndex - 1);

                    mask = Bmi1.ResetLowestSetBit(mask);
                    lastIndex = foundIndex;
                }
            }
            else
            {
                var remainderSpan = MemoryMarshal.CreateSpan(ref currentSearchSpace, Math.Min((int)Unsafe.ByteOffset(ref currentSearchSpace, ref end), 128));
                int foundIndex = remainderSpan.IndexOfAny(LineBreakAndComma);
                if (foundIndex == -1)
                    break;

                Unsafe.Add(ref dataRef, dataIndex++) = new Utf8StringUnsafe(
                    ref Unsafe.Add(ref currentSearchSpace, lastIndex),
                    (uint)foundIndex - lastIndex - 1);
                lastIndex = (uint)foundIndex + 1;
            }

            dataIndex -= GetOrAddBlock(context, ref dataRef, dataIndex);
            currentSearchSpace = ref Unsafe.Add(ref currentSearchSpace, lastIndex);
        }
        SerialRemainder(context, ref dataRef, dataIndex, ref currentSearchSpace, ref end);
    }

    private static int GetOrAddBlock(Context context, ref Utf8StringUnsafe dataRef, int dataIndex)
    {
        int i = 0;
        for (; i < dataIndex - 1; i += 2)
        {
            context.GetOrAdd(Unsafe.Add(ref dataRef, i))
                .Add(ParseTemperature(Unsafe.Add(ref dataRef, i + 1)));
        }
        Unsafe.Add(ref dataRef, 0) = Unsafe.Add(ref dataRef, dataIndex - 1);
        return i;
    }

    private static void SerialRemainder(Context context, ref Utf8StringUnsafe dataRef, int dataIndex, ref byte currentSearchSpace, ref byte end)
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

                Unsafe.Add(ref dataRef, dataIndex) = new Utf8StringUnsafe(
                    ref currentSearchSpace,
                    (uint)foundIndex);

                if (dataIndex == 1)
                {
                    context.GetOrAdd(dataRef)
                        .Add(ParseTemperature(Unsafe.Add(ref dataRef, 1)));
                }
                dataIndex ^= 1;
                lastIndex = foundIndex;
                remainderSpan = remainderSpan.Slice(foundIndex + 1);
                currentSearchSpace = ref Unsafe.Add(ref currentSearchSpace, lastIndex + 1);
            }
            if (remainderSpan.Length > 0)
            {
                Unsafe.Add(ref dataRef, dataIndex) = new Utf8StringUnsafe(
                    ref currentSearchSpace,
                    (uint)remainderSpan.Length);

                context.GetOrAdd(Unsafe.Add(ref dataRef, 0))
                    .Add(ParseTemperature(Unsafe.Add(ref dataRef, 1)));
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
    static unsafe int ParseTemperature(Utf8StringUnsafe data)
    {
        long word = Unsafe.AsRef<long>(data.Pointer);

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
