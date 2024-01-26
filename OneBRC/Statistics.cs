namespace OneBRC
{
    internal class Statistics
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
            this.Key = key;
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
