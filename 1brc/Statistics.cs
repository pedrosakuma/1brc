using System.Runtime.CompilerServices;

namespace OneBRC
{
    internal class Statistics : IComparable<Statistics>
    {
        public readonly string Key;
        public int Count;
        public long Sum;
        public short Min;
        public short Max;
        public Statistics(string key)
        {
            Count = 0;
            Sum = 0;
            Min = short.MaxValue;
            Max = short.MinValue;
            Key = key;
        }

        public int CompareTo(Statistics? other)
        {
            return this.Key.CompareTo(other!.Key);
        }

        internal void Add(short value)
        {
            Count++;
            Sum += value;
            Min = short.Min(Min, value);
            Max = short.Max(Max, value);
        }

        public override string ToString()
        {
            return $"{Key}={Min/10f:0.0}/{(Sum/Count)/10f:0.0}/{Max/10f:0.0}";
        }
    }
}
