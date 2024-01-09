namespace OneBRC
{
    public unsafe readonly struct KeyValueData
    {
        public readonly Utf8StringUnsafe Key;
        public readonly Utf8StringUnsafe Value;

        public KeyValueData(Utf8StringUnsafe key, byte* valuePointer, int valueLength)
        {
            this.Key = key;
            this.Value = new Utf8StringUnsafe(valuePointer, valueLength);
        }
    }
}