namespace OneBRC
{
    internal class Statistics
    {
        public int Count;
        public long Sum;
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
