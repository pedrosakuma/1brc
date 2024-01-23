namespace OneBRC
{
    internal class Statistics
    {
        public long Sum;
        public int Count;
        public short Min;
        public short Max;

        internal void Add(int value)
        {
            Count++;
            Sum += value;
            Min = short.Min(Min, (short)value);
            Max = short.Max(Max, (short)value);
        }
    }
}
