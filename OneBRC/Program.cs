using Microsoft.Win32.SafeHandles;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;

namespace OneBRC;

class Program
{
    static unsafe void Main(string[] args)
    {
        var sw = Stopwatch.StartNew();
        string path = args[0].Replace("~", Environment.GetFolderPath(Environment.SpecialFolder.UserProfile));
        int parallelism = Environment.ProcessorCount;
        int chunks = Environment.ProcessorCount * 2000;

        var contexts = new Context[parallelism];
        var consumers = new Thread[parallelism];

        ConcurrentQueue<Chunk> chunkQueue;
        using (var fileHandle = GetFileHandle(path))
        using (var mmf = GetMemoryMappedFile(path, fileHandle))
        {
            long length = GetFileLength(fileHandle);
            chunkQueue = new ConcurrentQueue<Chunk>(
                CreateChunks(mmf, chunks, length)
            );
            for (int i = 0; i < parallelism; i++)
            {
                int index = i;
                contexts[i] = new Context(chunkQueue, mmf);
                consumers[i] = new Thread(Consume);
                consumers[i].Start(contexts[i]);
            }
            foreach (var consumer in consumers)
                consumer.Join();

            WriteOrderedStatistics(GroupAndAggregateStatistics(contexts));
        }

        Console.WriteLine(sw.Elapsed);
    }

    private static unsafe MemoryMappedFile GetMemoryMappedFile(string path, SafeFileHandle fileHandle)
    {
        return MemoryMappedFile.CreateFromFile(fileHandle, null, 0, MemoryMappedFileAccess.Read, HandleInheritability.None, false);
    }

    private static unsafe SafeFileHandle GetFileHandle(string path)
    {
        return File.OpenHandle(path, FileMode.Open, FileAccess.Read, FileShare.Read, FileOptions.RandomAccess);
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
                    result.Add(new Chunk(position, lastIndexOfLineBreak));
                    position += (long)lastIndexOfLineBreak + 1;
                }
            }
        }
        return result.ToArray();
    }


    private static long GetFileLength(SafeFileHandle handle)
    {
        return RandomAccess.GetLength(handle);
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

    private unsafe static void Consume(object? obj)
    {
        ArgumentNullException.ThrowIfNull(obj);
        Context context = (Context)obj;

        using (var va = context.MappedFile.CreateViewAccessor(0, 0, MemoryMappedFileAccess.Read))
        {
            byte* ptr = (byte*)0;
            va.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);
            while (context.ChunkQueue.TryDequeue(out var chunk))
                Consume(context, ptr + chunk.Position, chunk.Size);
        }
    }

    private static unsafe void Consume(Context context, byte* ptr, int size)
    {
        ref byte searchSpace = ref Unsafe.AsRef<byte>(ptr);

        ref byte currentSearchSpace = ref searchSpace;
        ref byte end = ref Unsafe.Add(ref searchSpace, size);
        SerialRemainder(context, ref currentSearchSpace, ref end);
    }

    const long DOT_BITS = 0x10101000;
    const long MAGIC_MULTIPLIER = (100 * 0x1000000 + 10 * 0x10000 + 1);

    private static void SerialRemainder(Context context, ref byte currentSearchSpace, ref byte end)
    {
        ref var initialSearchSpace = ref currentSearchSpace;
        while (!Unsafe.IsAddressGreaterThan(ref currentSearchSpace, ref end))
        {
            uint index = IndexOf(ref currentSearchSpace, (byte)';');
            var key = new Utf8StringUnsafe(ref currentSearchSpace, index);

            currentSearchSpace = ref Unsafe.Add(ref currentSearchSpace, key.Length + 1);
            long word = Unsafe.As<byte, long>(ref currentSearchSpace);

            int decimalSepPos = (int)long.TrailingZeroCount(~word & DOT_BITS);
            long signed = (~word << 59) >> 63;
            long designMask = ~(signed & 0xFF);
            long digits = ((word & designMask) << (28 - decimalSepPos)) & 0x0F000F0F00L;
            long absValue = ((digits * MAGIC_MULTIPLIER) >>> 32) & 0x3FF;
            int measurement = (int)((absValue ^ signed) - signed);
            currentSearchSpace = ref Unsafe.Add(ref currentSearchSpace, (decimalSepPos >> 3) + 3);
            context.GetOrAdd(key)
                .Add(measurement);
        }
    }

    private static uint IndexOf(ref byte start, byte v)
    {
        if (Vector256.IsHardwareAccelerated)
            return IndexOfVector256(ref start, v);
        else if (Vector512.IsHardwareAccelerated)
            return IndexOfVector512(ref start, v);
        else
            return IndexOfScalar(ref start, v);
    }

    private static uint IndexOfScalar(ref byte start, byte v)
    {
        return (uint)MemoryMarshal.CreateReadOnlySpan(ref start, 256)
            .IndexOf(v);
    }

    private static uint IndexOfVector256(ref byte start, byte v)
    {
        ref var currentSearchSpace = ref Unsafe.As<byte, Vector256<byte>>(ref start);
        uint mask;
        int index = 0;
        while ((mask = Vector256.Equals(currentSearchSpace, Vector256.Create(v))
            .ExtractMostSignificantBits()) == 0)
        {
            currentSearchSpace = ref Unsafe.Add(ref currentSearchSpace, 1);
            index += Vector256<byte>.Count;
        }
        return uint.TrailingZeroCount(mask) + (uint)index;
    }
    private static uint IndexOfVector512(ref byte start, byte v)
    {
        ref var currentSearchSpace = ref Unsafe.As<byte, Vector512<byte>>(ref start);
        ulong mask;
        int index = 0;
        while ((mask = Vector512.Equals(currentSearchSpace, Vector512.Create(v))
            .ExtractMostSignificantBits()) == 0)
        {
            currentSearchSpace = ref Unsafe.Add(ref currentSearchSpace, 1);
            index += Vector512<byte>.Count;
        }
        return (uint)ulong.TrailingZeroCount(mask) + (uint)index;
    }
}
