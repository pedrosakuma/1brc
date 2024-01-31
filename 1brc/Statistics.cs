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
    }
}
