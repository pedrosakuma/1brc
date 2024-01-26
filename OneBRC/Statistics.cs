namespace OneBRC
{
    internal class Statistics
    {
        public int Count;
        public long Sum;
        public short Min;
        public short Max;
        public Statistics()
        {
            Count = 0;
            Sum = 0;
            Min = short.MaxValue;
            Max = short.MinValue;
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
