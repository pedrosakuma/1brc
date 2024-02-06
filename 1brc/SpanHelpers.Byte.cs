using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;

namespace OneBRC
{
    internal static class SpanHelpers
    {
        static readonly int[] vecDecodeTable = new int[256 * 8]
        {
            0, 0, 0, 0, 0, 0, 0, 0, /* 0x00 (00000000) */
            1, 0, 0, 0, 0, 0, 0, 0, /* 0x01 (00000001) */
            2, 0, 0, 0, 0, 0, 0, 0, /* 0x02 (00000010) */
            1, 2, 0, 0, 0, 0, 0, 0, /* 0x03 (00000011) */
            3, 0, 0, 0, 0, 0, 0, 0, /* 0x04 (00000100) */
            1, 3, 0, 0, 0, 0, 0, 0, /* 0x05 (00000101) */
            2, 3, 0, 0, 0, 0, 0, 0, /* 0x06 (00000110) */
            1, 2, 3, 0, 0, 0, 0, 0, /* 0x07 (00000111) */
            4, 0, 0, 0, 0, 0, 0, 0, /* 0x08 (00001000) */
            1, 4, 0, 0, 0, 0, 0, 0, /* 0x09 (00001001) */
            2, 4, 0, 0, 0, 0, 0, 0, /* 0x0A (00001010) */
            1, 2, 4, 0, 0, 0, 0, 0, /* 0x0B (00001011) */
            3, 4, 0, 0, 0, 0, 0, 0, /* 0x0C (00001100) */
            1, 3, 4, 0, 0, 0, 0, 0, /* 0x0D (00001101) */
            2, 3, 4, 0, 0, 0, 0, 0, /* 0x0E (00001110) */
            1, 2, 3, 4, 0, 0, 0, 0, /* 0x0F (00001111) */
            5, 0, 0, 0, 0, 0, 0, 0, /* 0x10 (00010000) */
            1, 5, 0, 0, 0, 0, 0, 0, /* 0x11 (00010001) */
            2, 5, 0, 0, 0, 0, 0, 0, /* 0x12 (00010010) */
            1, 2, 5, 0, 0, 0, 0, 0, /* 0x13 (00010011) */
            3, 5, 0, 0, 0, 0, 0, 0, /* 0x14 (00010100) */
            1, 3, 5, 0, 0, 0, 0, 0, /* 0x15 (00010101) */
            2, 3, 5, 0, 0, 0, 0, 0, /* 0x16 (00010110) */
            1, 2, 3, 5, 0, 0, 0, 0, /* 0x17 (00010111) */
            4, 5, 0, 0, 0, 0, 0, 0, /* 0x18 (00011000) */
            1, 4, 5, 0, 0, 0, 0, 0, /* 0x19 (00011001) */
            2, 4, 5, 0, 0, 0, 0, 0, /* 0x1A (00011010) */
            1, 2, 4, 5, 0, 0, 0, 0, /* 0x1B (00011011) */
            3, 4, 5, 0, 0, 0, 0, 0, /* 0x1C (00011100) */
            1, 3, 4, 5, 0, 0, 0, 0, /* 0x1D (00011101) */
            2, 3, 4, 5, 0, 0, 0, 0, /* 0x1E (00011110) */
            1, 2, 3, 4, 5, 0, 0, 0, /* 0x1F (00011111) */
            6, 0, 0, 0, 0, 0, 0, 0, /* 0x20 (00100000) */
            1, 6, 0, 0, 0, 0, 0, 0, /* 0x21 (00100001) */
            2, 6, 0, 0, 0, 0, 0, 0, /* 0x22 (00100010) */
            1, 2, 6, 0, 0, 0, 0, 0, /* 0x23 (00100011) */
            3, 6, 0, 0, 0, 0, 0, 0, /* 0x24 (00100100) */
            1, 3, 6, 0, 0, 0, 0, 0, /* 0x25 (00100101) */
            2, 3, 6, 0, 0, 0, 0, 0, /* 0x26 (00100110) */
            1, 2, 3, 6, 0, 0, 0, 0, /* 0x27 (00100111) */
            4, 6, 0, 0, 0, 0, 0, 0, /* 0x28 (00101000) */
            1, 4, 6, 0, 0, 0, 0, 0, /* 0x29 (00101001) */
            2, 4, 6, 0, 0, 0, 0, 0, /* 0x2A (00101010) */
            1, 2, 4, 6, 0, 0, 0, 0, /* 0x2B (00101011) */
            3, 4, 6, 0, 0, 0, 0, 0, /* 0x2C (00101100) */
            1, 3, 4, 6, 0, 0, 0, 0, /* 0x2D (00101101) */
            2, 3, 4, 6, 0, 0, 0, 0, /* 0x2E (00101110) */
            1, 2, 3, 4, 6, 0, 0, 0, /* 0x2F (00101111) */
            5, 6, 0, 0, 0, 0, 0, 0, /* 0x30 (00110000) */
            1, 5, 6, 0, 0, 0, 0, 0, /* 0x31 (00110001) */
            2, 5, 6, 0, 0, 0, 0, 0, /* 0x32 (00110010) */
            1, 2, 5, 6, 0, 0, 0, 0, /* 0x33 (00110011) */
            3, 5, 6, 0, 0, 0, 0, 0, /* 0x34 (00110100) */
            1, 3, 5, 6, 0, 0, 0, 0, /* 0x35 (00110101) */
            2, 3, 5, 6, 0, 0, 0, 0, /* 0x36 (00110110) */
            1, 2, 3, 5, 6, 0, 0, 0, /* 0x37 (00110111) */
            4, 5, 6, 0, 0, 0, 0, 0, /* 0x38 (00111000) */
            1, 4, 5, 6, 0, 0, 0, 0, /* 0x39 (00111001) */
            2, 4, 5, 6, 0, 0, 0, 0, /* 0x3A (00111010) */
            1, 2, 4, 5, 6, 0, 0, 0, /* 0x3B (00111011) */
            3, 4, 5, 6, 0, 0, 0, 0, /* 0x3C (00111100) */
            1, 3, 4, 5, 6, 0, 0, 0, /* 0x3D (00111101) */
            2, 3, 4, 5, 6, 0, 0, 0, /* 0x3E (00111110) */
            1, 2, 3, 4, 5, 6, 0, 0, /* 0x3F (00111111) */
            7, 0, 0, 0, 0, 0, 0, 0, /* 0x40 (01000000) */
            1, 7, 0, 0, 0, 0, 0, 0, /* 0x41 (01000001) */
            2, 7, 0, 0, 0, 0, 0, 0, /* 0x42 (01000010) */
            1, 2, 7, 0, 0, 0, 0, 0, /* 0x43 (01000011) */
            3, 7, 0, 0, 0, 0, 0, 0, /* 0x44 (01000100) */
            1, 3, 7, 0, 0, 0, 0, 0, /* 0x45 (01000101) */
            2, 3, 7, 0, 0, 0, 0, 0, /* 0x46 (01000110) */
            1, 2, 3, 7, 0, 0, 0, 0, /* 0x47 (01000111) */
            4, 7, 0, 0, 0, 0, 0, 0, /* 0x48 (01001000) */
            1, 4, 7, 0, 0, 0, 0, 0, /* 0x49 (01001001) */
            2, 4, 7, 0, 0, 0, 0, 0, /* 0x4A (01001010) */
            1, 2, 4, 7, 0, 0, 0, 0, /* 0x4B (01001011) */
            3, 4, 7, 0, 0, 0, 0, 0, /* 0x4C (01001100) */
            1, 3, 4, 7, 0, 0, 0, 0, /* 0x4D (01001101) */
            2, 3, 4, 7, 0, 0, 0, 0, /* 0x4E (01001110) */
            1, 2, 3, 4, 7, 0, 0, 0, /* 0x4F (01001111) */
            5, 7, 0, 0, 0, 0, 0, 0, /* 0x50 (01010000) */
            1, 5, 7, 0, 0, 0, 0, 0, /* 0x51 (01010001) */
            2, 5, 7, 0, 0, 0, 0, 0, /* 0x52 (01010010) */
            1, 2, 5, 7, 0, 0, 0, 0, /* 0x53 (01010011) */
            3, 5, 7, 0, 0, 0, 0, 0, /* 0x54 (01010100) */
            1, 3, 5, 7, 0, 0, 0, 0, /* 0x55 (01010101) */
            2, 3, 5, 7, 0, 0, 0, 0, /* 0x56 (01010110) */
            1, 2, 3, 5, 7, 0, 0, 0, /* 0x57 (01010111) */
            4, 5, 7, 0, 0, 0, 0, 0, /* 0x58 (01011000) */
            1, 4, 5, 7, 0, 0, 0, 0, /* 0x59 (01011001) */
            2, 4, 5, 7, 0, 0, 0, 0, /* 0x5A (01011010) */
            1, 2, 4, 5, 7, 0, 0, 0, /* 0x5B (01011011) */
            3, 4, 5, 7, 0, 0, 0, 0, /* 0x5C (01011100) */
            1, 3, 4, 5, 7, 0, 0, 0, /* 0x5D (01011101) */
            2, 3, 4, 5, 7, 0, 0, 0, /* 0x5E (01011110) */
            1, 2, 3, 4, 5, 7, 0, 0, /* 0x5F (01011111) */
            6, 7, 0, 0, 0, 0, 0, 0, /* 0x60 (01100000) */
            1, 6, 7, 0, 0, 0, 0, 0, /* 0x61 (01100001) */
            2, 6, 7, 0, 0, 0, 0, 0, /* 0x62 (01100010) */
            1, 2, 6, 7, 0, 0, 0, 0, /* 0x63 (01100011) */
            3, 6, 7, 0, 0, 0, 0, 0, /* 0x64 (01100100) */
            1, 3, 6, 7, 0, 0, 0, 0, /* 0x65 (01100101) */
            2, 3, 6, 7, 0, 0, 0, 0, /* 0x66 (01100110) */
            1, 2, 3, 6, 7, 0, 0, 0, /* 0x67 (01100111) */
            4, 6, 7, 0, 0, 0, 0, 0, /* 0x68 (01101000) */
            1, 4, 6, 7, 0, 0, 0, 0, /* 0x69 (01101001) */
            2, 4, 6, 7, 0, 0, 0, 0, /* 0x6A (01101010) */
            1, 2, 4, 6, 7, 0, 0, 0, /* 0x6B (01101011) */
            3, 4, 6, 7, 0, 0, 0, 0, /* 0x6C (01101100) */
            1, 3, 4, 6, 7, 0, 0, 0, /* 0x6D (01101101) */
            2, 3, 4, 6, 7, 0, 0, 0, /* 0x6E (01101110) */
            1, 2, 3, 4, 6, 7, 0, 0, /* 0x6F (01101111) */
            5, 6, 7, 0, 0, 0, 0, 0, /* 0x70 (01110000) */
            1, 5, 6, 7, 0, 0, 0, 0, /* 0x71 (01110001) */
            2, 5, 6, 7, 0, 0, 0, 0, /* 0x72 (01110010) */
            1, 2, 5, 6, 7, 0, 0, 0, /* 0x73 (01110011) */
            3, 5, 6, 7, 0, 0, 0, 0, /* 0x74 (01110100) */
            1, 3, 5, 6, 7, 0, 0, 0, /* 0x75 (01110101) */
            2, 3, 5, 6, 7, 0, 0, 0, /* 0x76 (01110110) */
            1, 2, 3, 5, 6, 7, 0, 0, /* 0x77 (01110111) */
            4, 5, 6, 7, 0, 0, 0, 0, /* 0x78 (01111000) */
            1, 4, 5, 6, 7, 0, 0, 0, /* 0x79 (01111001) */
            2, 4, 5, 6, 7, 0, 0, 0, /* 0x7A (01111010) */
            1, 2, 4, 5, 6, 7, 0, 0, /* 0x7B (01111011) */
            3, 4, 5, 6, 7, 0, 0, 0, /* 0x7C (01111100) */
            1, 3, 4, 5, 6, 7, 0, 0, /* 0x7D (01111101) */
            2, 3, 4, 5, 6, 7, 0, 0, /* 0x7E (01111110) */
            1, 2, 3, 4, 5, 6, 7, 0, /* 0x7F (01111111) */
            8, 0, 0, 0, 0, 0, 0, 0, /* 0x80 (10000000) */
            1, 8, 0, 0, 0, 0, 0, 0, /* 0x81 (10000001) */
            2, 8, 0, 0, 0, 0, 0, 0, /* 0x82 (10000010) */
            1, 2, 8, 0, 0, 0, 0, 0, /* 0x83 (10000011) */
            3, 8, 0, 0, 0, 0, 0, 0, /* 0x84 (10000100) */
            1, 3, 8, 0, 0, 0, 0, 0, /* 0x85 (10000101) */
            2, 3, 8, 0, 0, 0, 0, 0, /* 0x86 (10000110) */
            1, 2, 3, 8, 0, 0, 0, 0, /* 0x87 (10000111) */
            4, 8, 0, 0, 0, 0, 0, 0, /* 0x88 (10001000) */
            1, 4, 8, 0, 0, 0, 0, 0, /* 0x89 (10001001) */
            2, 4, 8, 0, 0, 0, 0, 0, /* 0x8A (10001010) */
            1, 2, 4, 8, 0, 0, 0, 0, /* 0x8B (10001011) */
            3, 4, 8, 0, 0, 0, 0, 0, /* 0x8C (10001100) */
            1, 3, 4, 8, 0, 0, 0, 0, /* 0x8D (10001101) */
            2, 3, 4, 8, 0, 0, 0, 0, /* 0x8E (10001110) */
            1, 2, 3, 4, 8, 0, 0, 0, /* 0x8F (10001111) */
            5, 8, 0, 0, 0, 0, 0, 0, /* 0x90 (10010000) */
            1, 5, 8, 0, 0, 0, 0, 0, /* 0x91 (10010001) */
            2, 5, 8, 0, 0, 0, 0, 0, /* 0x92 (10010010) */
            1, 2, 5, 8, 0, 0, 0, 0, /* 0x93 (10010011) */
            3, 5, 8, 0, 0, 0, 0, 0, /* 0x94 (10010100) */
            1, 3, 5, 8, 0, 0, 0, 0, /* 0x95 (10010101) */
            2, 3, 5, 8, 0, 0, 0, 0, /* 0x96 (10010110) */
            1, 2, 3, 5, 8, 0, 0, 0, /* 0x97 (10010111) */
            4, 5, 8, 0, 0, 0, 0, 0, /* 0x98 (10011000) */
            1, 4, 5, 8, 0, 0, 0, 0, /* 0x99 (10011001) */
            2, 4, 5, 8, 0, 0, 0, 0, /* 0x9A (10011010) */
            1, 2, 4, 5, 8, 0, 0, 0, /* 0x9B (10011011) */
            3, 4, 5, 8, 0, 0, 0, 0, /* 0x9C (10011100) */
            1, 3, 4, 5, 8, 0, 0, 0, /* 0x9D (10011101) */
            2, 3, 4, 5, 8, 0, 0, 0, /* 0x9E (10011110) */
            1, 2, 3, 4, 5, 8, 0, 0, /* 0x9F (10011111) */
            6, 8, 0, 0, 0, 0, 0, 0, /* 0xA0 (10100000) */
            1, 6, 8, 0, 0, 0, 0, 0, /* 0xA1 (10100001) */
            2, 6, 8, 0, 0, 0, 0, 0, /* 0xA2 (10100010) */
            1, 2, 6, 8, 0, 0, 0, 0, /* 0xA3 (10100011) */
            3, 6, 8, 0, 0, 0, 0, 0, /* 0xA4 (10100100) */
            1, 3, 6, 8, 0, 0, 0, 0, /* 0xA5 (10100101) */
            2, 3, 6, 8, 0, 0, 0, 0, /* 0xA6 (10100110) */
            1, 2, 3, 6, 8, 0, 0, 0, /* 0xA7 (10100111) */
            4, 6, 8, 0, 0, 0, 0, 0, /* 0xA8 (10101000) */
            1, 4, 6, 8, 0, 0, 0, 0, /* 0xA9 (10101001) */
            2, 4, 6, 8, 0, 0, 0, 0, /* 0xAA (10101010) */
            1, 2, 4, 6, 8, 0, 0, 0, /* 0xAB (10101011) */
            3, 4, 6, 8, 0, 0, 0, 0, /* 0xAC (10101100) */
            1, 3, 4, 6, 8, 0, 0, 0, /* 0xAD (10101101) */
            2, 3, 4, 6, 8, 0, 0, 0, /* 0xAE (10101110) */
            1, 2, 3, 4, 6, 8, 0, 0, /* 0xAF (10101111) */
            5, 6, 8, 0, 0, 0, 0, 0, /* 0xB0 (10110000) */
            1, 5, 6, 8, 0, 0, 0, 0, /* 0xB1 (10110001) */
            2, 5, 6, 8, 0, 0, 0, 0, /* 0xB2 (10110010) */
            1, 2, 5, 6, 8, 0, 0, 0, /* 0xB3 (10110011) */
            3, 5, 6, 8, 0, 0, 0, 0, /* 0xB4 (10110100) */
            1, 3, 5, 6, 8, 0, 0, 0, /* 0xB5 (10110101) */
            2, 3, 5, 6, 8, 0, 0, 0, /* 0xB6 (10110110) */
            1, 2, 3, 5, 6, 8, 0, 0, /* 0xB7 (10110111) */
            4, 5, 6, 8, 0, 0, 0, 0, /* 0xB8 (10111000) */
            1, 4, 5, 6, 8, 0, 0, 0, /* 0xB9 (10111001) */
            2, 4, 5, 6, 8, 0, 0, 0, /* 0xBA (10111010) */
            1, 2, 4, 5, 6, 8, 0, 0, /* 0xBB (10111011) */
            3, 4, 5, 6, 8, 0, 0, 0, /* 0xBC (10111100) */
            1, 3, 4, 5, 6, 8, 0, 0, /* 0xBD (10111101) */
            2, 3, 4, 5, 6, 8, 0, 0, /* 0xBE (10111110) */
            1, 2, 3, 4, 5, 6, 8, 0, /* 0xBF (10111111) */
            7, 8, 0, 0, 0, 0, 0, 0, /* 0xC0 (11000000) */
            1, 7, 8, 0, 0, 0, 0, 0, /* 0xC1 (11000001) */
            2, 7, 8, 0, 0, 0, 0, 0, /* 0xC2 (11000010) */
            1, 2, 7, 8, 0, 0, 0, 0, /* 0xC3 (11000011) */
            3, 7, 8, 0, 0, 0, 0, 0, /* 0xC4 (11000100) */
            1, 3, 7, 8, 0, 0, 0, 0, /* 0xC5 (11000101) */
            2, 3, 7, 8, 0, 0, 0, 0, /* 0xC6 (11000110) */
            1, 2, 3, 7, 8, 0, 0, 0, /* 0xC7 (11000111) */
            4, 7, 8, 0, 0, 0, 0, 0, /* 0xC8 (11001000) */
            1, 4, 7, 8, 0, 0, 0, 0, /* 0xC9 (11001001) */
            2, 4, 7, 8, 0, 0, 0, 0, /* 0xCA (11001010) */
            1, 2, 4, 7, 8, 0, 0, 0, /* 0xCB (11001011) */
            3, 4, 7, 8, 0, 0, 0, 0, /* 0xCC (11001100) */
            1, 3, 4, 7, 8, 0, 0, 0, /* 0xCD (11001101) */
            2, 3, 4, 7, 8, 0, 0, 0, /* 0xCE (11001110) */
            1, 2, 3, 4, 7, 8, 0, 0, /* 0xCF (11001111) */
            5, 7, 8, 0, 0, 0, 0, 0, /* 0xD0 (11010000) */
            1, 5, 7, 8, 0, 0, 0, 0, /* 0xD1 (11010001) */
            2, 5, 7, 8, 0, 0, 0, 0, /* 0xD2 (11010010) */
            1, 2, 5, 7, 8, 0, 0, 0, /* 0xD3 (11010011) */
            3, 5, 7, 8, 0, 0, 0, 0, /* 0xD4 (11010100) */
            1, 3, 5, 7, 8, 0, 0, 0, /* 0xD5 (11010101) */
            2, 3, 5, 7, 8, 0, 0, 0, /* 0xD6 (11010110) */
            1, 2, 3, 5, 7, 8, 0, 0, /* 0xD7 (11010111) */
            4, 5, 7, 8, 0, 0, 0, 0, /* 0xD8 (11011000) */
            1, 4, 5, 7, 8, 0, 0, 0, /* 0xD9 (11011001) */
            2, 4, 5, 7, 8, 0, 0, 0, /* 0xDA (11011010) */
            1, 2, 4, 5, 7, 8, 0, 0, /* 0xDB (11011011) */
            3, 4, 5, 7, 8, 0, 0, 0, /* 0xDC (11011100) */
            1, 3, 4, 5, 7, 8, 0, 0, /* 0xDD (11011101) */
            2, 3, 4, 5, 7, 8, 0, 0, /* 0xDE (11011110) */
            1, 2, 3, 4, 5, 7, 8, 0, /* 0xDF (11011111) */
            6, 7, 8, 0, 0, 0, 0, 0, /* 0xE0 (11100000) */
            1, 6, 7, 8, 0, 0, 0, 0, /* 0xE1 (11100001) */
            2, 6, 7, 8, 0, 0, 0, 0, /* 0xE2 (11100010) */
            1, 2, 6, 7, 8, 0, 0, 0, /* 0xE3 (11100011) */
            3, 6, 7, 8, 0, 0, 0, 0, /* 0xE4 (11100100) */
            1, 3, 6, 7, 8, 0, 0, 0, /* 0xE5 (11100101) */
            2, 3, 6, 7, 8, 0, 0, 0, /* 0xE6 (11100110) */
            1, 2, 3, 6, 7, 8, 0, 0, /* 0xE7 (11100111) */
            4, 6, 7, 8, 0, 0, 0, 0, /* 0xE8 (11101000) */
            1, 4, 6, 7, 8, 0, 0, 0, /* 0xE9 (11101001) */
            2, 4, 6, 7, 8, 0, 0, 0, /* 0xEA (11101010) */
            1, 2, 4, 6, 7, 8, 0, 0, /* 0xEB (11101011) */
            3, 4, 6, 7, 8, 0, 0, 0, /* 0xEC (11101100) */
            1, 3, 4, 6, 7, 8, 0, 0, /* 0xED (11101101) */
            2, 3, 4, 6, 7, 8, 0, 0, /* 0xEE (11101110) */
            1, 2, 3, 4, 6, 7, 8, 0, /* 0xEF (11101111) */
            5, 6, 7, 8, 0, 0, 0, 0, /* 0xF0 (11110000) */
            1, 5, 6, 7, 8, 0, 0, 0, /* 0xF1 (11110001) */
            2, 5, 6, 7, 8, 0, 0, 0, /* 0xF2 (11110010) */
            1, 2, 5, 6, 7, 8, 0, 0, /* 0xF3 (11110011) */
            3, 5, 6, 7, 8, 0, 0, 0, /* 0xF4 (11110100) */
            1, 3, 5, 6, 7, 8, 0, 0, /* 0xF5 (11110101) */
            2, 3, 5, 6, 7, 8, 0, 0, /* 0xF6 (11110110) */
            1, 2, 3, 5, 6, 7, 8, 0, /* 0xF7 (11110111) */
            4, 5, 6, 7, 8, 0, 0, 0, /* 0xF8 (11111000) */
            1, 4, 5, 6, 7, 8, 0, 0, /* 0xF9 (11111001) */
            2, 4, 5, 6, 7, 8, 0, 0, /* 0xFA (11111010) */
            1, 2, 4, 5, 6, 7, 8, 0, /* 0xFB (11111011) */
            3, 4, 5, 6, 7, 8, 0, 0, /* 0xFC (11111100) */
            1, 3, 4, 5, 6, 7, 8, 0, /* 0xFD (11111101) */
            2, 3, 4, 5, 6, 7, 8, 0, /* 0xFE (11111110) */
            1, 2, 3, 4, 5, 6, 7, 8  /* 0xFF (11111111) */
        };
        static readonly byte[] lengthTable = new byte[256]
        {
            0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4,
            1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
            1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
            2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
            1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
            2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
            2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
            3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
            1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
            2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
            2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
            3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
            2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
            3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
            3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
            4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8
        };
        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        public static unsafe int ExtractIndexes(this uint mask, ref int initialOutput, int offset)
        {
            if (mask == 0)
                return 0;

            ref var refCurrentOutput = ref initialOutput;
            ref var refDecodeTable = ref Unsafe.As<int, Vector256<int>>(ref MemoryMarshal.GetArrayDataReference(vecDecodeTable));
            ref var refLengthTable = ref MemoryMarshal.GetArrayDataReference(lengthTable);
            ref var bytes = ref Unsafe.As<uint, byte>(ref mask);

            Vector256<int> baseVec = Vector256.Create<int>(offset - 1);
            Vector256<int> add8 = Vector256.Create<int>(8);
            Vector256<int> add16 = Vector256.Create<int>(16);

            for (int k = 0; k < 2; ++k)
            {
                byte byteA = (byte)mask;
                byte byteB = (byte)(mask >> 8);
                Vector256<int> vecA = Unsafe.Add(ref refDecodeTable, byteA);
                Vector256<int> vecB = Unsafe.Add(ref refDecodeTable, byteB);
                mask >>= 16;
                vecA += baseVec;
                vecB += baseVec + add8;
                baseVec += add16;

                Unsafe.As<int, Vector256<int>>(ref refCurrentOutput) = vecA;
                refCurrentOutput = ref Unsafe.Add(ref refCurrentOutput, Unsafe.Add(ref refLengthTable, byteA));
                Unsafe.As<int, Vector256<int>>(ref refCurrentOutput) = vecB;
                refCurrentOutput = ref Unsafe.Add(ref refCurrentOutput, Unsafe.Add(ref refLengthTable, byteB));
            }
            return (int)(Unsafe.ByteOffset(ref initialOutput, ref refCurrentOutput) / sizeof(int));

        }
        [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
        public static unsafe int ExtractIndexes(this ulong mask, ref int initialOutput, int offset)
        {
            ref var refCurrentOutput = ref initialOutput;
            ref var refDecodeTable = ref Unsafe.As<int, Vector256<int>>(ref MemoryMarshal.GetArrayDataReference(vecDecodeTable));
            ref var refLengthTable = ref MemoryMarshal.GetArrayDataReference(lengthTable);

            Vector256<int> baseVec = Vector256.Create<int>(offset - 1);
            Vector256<int> add8 = Vector256.Create<int>(8);
            Vector256<int> add16 = Vector256.Create<int>(16);

            for (int k = 0; k < 4; ++k)
            {
                byte byteA = (byte)mask;
                byte byteB = (byte)(mask >> 8);
                mask >>= 16;
                Vector256<int> vecA = Unsafe.Add(ref refDecodeTable, byteA);
                Vector256<int> vecB = Unsafe.Add(ref refDecodeTable, byteB);
                vecA += baseVec;
                vecB += baseVec + add8;
                baseVec += add16;

                Unsafe.As<int, Vector256<int>>(ref refCurrentOutput) = vecA;
                refCurrentOutput = ref Unsafe.Add(ref refCurrentOutput, Unsafe.Add(ref refLengthTable, byteA));
                Unsafe.As<int, Vector256<int>>(ref refCurrentOutput) = vecB;
                refCurrentOutput = ref Unsafe.Add(ref refCurrentOutput, Unsafe.Add(ref refLengthTable, byteB));
            }
            return (int)(Unsafe.ByteOffset(ref initialOutput, ref refCurrentOutput) / sizeof(int));
        }
        
        /// <summary>
        /// @noahfalk
        /// </summary>
        /// <param name="tempUtf8Bytes"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
        public static Vector256<short> ParseQuadFixedPoint(this Vector256<long> words)
        {
            var quadFixedPointLeftAlignShuffle = Vector256.Create(
                (byte)0, 2, 3, 4, 3, 2, 1, 0,
                8, 10, 11, 12, 11, 10, 9, 8,
                16, 18, 19, 20, 19, 18, 17, 16,
                24, 26, 27, 28, 27, 26, 25, 24);
            var dotMask = Vector256.Create(
                0, (byte)'.', (byte)'.', 0, 0, 0, 0, 0,
                0, (byte)'.', (byte)'.', 0, 0, 0, 0, 0,
                0, (byte)'.', (byte)'.', 0, 0, 0, 0, 0,
                0, (byte)'.', (byte)'.', 0, 0, 0, 0, 0);
            var dotMult = Vector256.Create(
                3, 2, 0, 0, 0, 0, 0, 0,
                3, 2, 0, 0, 0, 0, 0, 0,
                3, 2, 0, 0, 0, 0, 0, 0,
                3, 2, 0, 0, 0, 0, 0, 0);
            var fixedPointMult1LeftAligned = Vector256.Create(
                1, 0, 10, 100, 0, 0, 0, 0,
                1, 0, 10, 100, 0, 0, 0, 0,
                1, 0, 10, 100, 0, 0, 0, 0,
                1, 0, 10, 100, 0, 0, 0, 0);

            var v = Avx2.Shuffle(words.AsByte(), quadFixedPointLeftAlignShuffle);
            var negativeMask = Vector256.ShiftRightArithmetic(
                Vector256.ShiftLeft(Vector256.Equals(v,
                    Vector256.Create((long)'-').AsByte()).AsInt32(),
                    24),
                24).AsInt16();
            var partialSums = Avx2.MultiplyAddAdjacent(
                Avx2.SubtractSaturate(
                    Avx2.ShiftRightLogicalVariable(
                        v.AsInt64(), 
                        Avx2.ShiftLeftLogical(
                            Avx2.Subtract(
                                Vector256.Create(5L),
                                Avx2.And(
                                    Avx2.MultiplyAddAdjacent(
                                        Avx2.ShiftRightLogical(Vector256.Equals(v, dotMask).AsInt64(), 8).AsByte(),
                                        dotMult
                                    ).AsInt64(),
                                    Vector256.Create(3L)
                                )
                            ), 
                            3
                        ).AsUInt64()
                    ).AsByte(), 
                    Vector256.Create<byte>((byte)'0')
                ), 
                fixedPointMult1LeftAligned
            );
            Vector256<short> absFixedPoint = Vector256.Add(Avx2.ShiftRightLogical(partialSums.AsInt32(), 16).AsInt16(), partialSums);
            return Avx2.BlendVariable(absFixedPoint, -absFixedPoint, negativeMask);
        }
        public static unsafe bool SequenceEqual(ref byte first, ref byte second, nuint length)
        {
            bool result;
            // Use nint for arithmetic to avoid unnecessary 64->32->64 truncations
            if (length >= (nuint)sizeof(nuint))
            {
                // Conditional jmp forward to favor shorter lengths. (See comment at "Equal:" label)
                // The longer lengths can make back the time due to branch misprediction
                // better than shorter lengths.
                goto Longer;
            }

            {
                uint differentBits = 0;
                nuint offset = (length & 2);
                if (offset != 0)
                {
                    differentBits = LoadUShort(ref first);
                    differentBits -= LoadUShort(ref second);
                }
                if ((length & 1) != 0)
                {
                    differentBits |= (uint)Unsafe.AddByteOffset(ref first, offset) - (uint)Unsafe.AddByteOffset(ref second, offset);
                }
                result = (differentBits == 0);
                goto Result;
            }

        Longer:
            // Only check that the ref is the same if buffers are large,
            // and hence its worth avoiding doing unnecessary comparisons
            if (!Unsafe.AreSame(ref first, ref second))
            {
                // C# compiler inverts this test, making the outer goto the conditional jmp.
                goto Vector;
            }

            // This becomes a conditional jmp forward to not favor it.
            goto Equal;

        Result:
            return result;
        // When the sequence is equal; which is the longest execution, we want it to determine that
        // as fast as possible so we do not want the early outs to be "predicted not taken" branches.
        Equal:
            return true;

        Vector:
            if (Vector128.IsHardwareAccelerated)
            {
                if (Vector512.IsHardwareAccelerated && length >= (nuint)Vector512<byte>.Count)
                {
                    nuint offset = 0;
                    nuint lengthToExamine = length - (nuint)Vector512<byte>.Count;
                    // Unsigned, so it shouldn't have overflowed larger than length (rather than negative)
                    Debug.Assert(lengthToExamine < length);
                    if (lengthToExamine != 0)
                    {
                        do
                        {
                            if (Vector512.LoadUnsafe(ref first, offset) !=
                                Vector512.LoadUnsafe(ref second, offset))
                            {
                                goto NotEqual;
                            }
                            offset += (nuint)Vector512<byte>.Count;
                        } while (lengthToExamine > offset);
                    }

                    // Do final compare as Vector512<byte>.Count from end rather than start
                    if (Vector512.LoadUnsafe(ref first, lengthToExamine) ==
                        Vector512.LoadUnsafe(ref second, lengthToExamine))
                    {
                        // C# compiler inverts this test, making the outer goto the conditional jmp.
                        goto Equal;
                    }

                    // This becomes a conditional jmp forward to not favor it.
                    goto NotEqual;
                }
                else if (Vector256.IsHardwareAccelerated && length >= (nuint)Vector256<byte>.Count)
                {
                    nuint offset = 0;
                    nuint lengthToExamine = length - (nuint)Vector256<byte>.Count;
                    // Unsigned, so it shouldn't have overflowed larger than length (rather than negative)
                    Debug.Assert(lengthToExamine < length);
                    if (lengthToExamine != 0)
                    {
                        do
                        {
                            if (Vector256.LoadUnsafe(ref first, offset) !=
                                Vector256.LoadUnsafe(ref second, offset))
                            {
                                goto NotEqual;
                            }
                            offset += (nuint)Vector256<byte>.Count;
                        } while (lengthToExamine > offset);
                    }

                    // Do final compare as Vector256<byte>.Count from end rather than start
                    if (Vector256.LoadUnsafe(ref first, lengthToExamine) ==
                        Vector256.LoadUnsafe(ref second, lengthToExamine))
                    {
                        // C# compiler inverts this test, making the outer goto the conditional jmp.
                        goto Equal;
                    }

                    // This becomes a conditional jmp forward to not favor it.
                    goto NotEqual;
                }
                else if (length >= (nuint)Vector128<byte>.Count)
                {
                    nuint offset = 0;
                    nuint lengthToExamine = length - (nuint)Vector128<byte>.Count;
                    // Unsigned, so it shouldn't have overflowed larger than length (rather than negative)
                    Debug.Assert(lengthToExamine < length);
                    if (lengthToExamine != 0)
                    {
                        do
                        {
                            if (Vector128.LoadUnsafe(ref first, offset) !=
                                Vector128.LoadUnsafe(ref second, offset))
                            {
                                goto NotEqual;
                            }
                            offset += (nuint)Vector128<byte>.Count;
                        } while (lengthToExamine > offset);
                    }

                    // Do final compare as Vector128<byte>.Count from end rather than start
                    if (Vector128.LoadUnsafe(ref first, lengthToExamine) ==
                        Vector128.LoadUnsafe(ref second, lengthToExamine))
                    {
                        // C# compiler inverts this test, making the outer goto the conditional jmp.
                        goto Equal;
                    }

                    // This becomes a conditional jmp forward to not favor it.
                    goto NotEqual;
                }
            }

            {
                Debug.Assert(length >= (nuint)sizeof(nuint));
                {
                    nuint offset = 0;
                    nuint lengthToExamine = length - (nuint)sizeof(nuint);
                    // Unsigned, so it shouldn't have overflowed larger than length (rather than negative)
                    Debug.Assert(lengthToExamine < length);
                    if (lengthToExamine > 0)
                    {
                        do
                        {
                            // Compare unsigned so not do a sign extend mov on 64 bit
                            if (LoadNUInt(ref first, offset) != LoadNUInt(ref second, offset))
                            {
                                goto NotEqual;
                            }
                            offset += (nuint)sizeof(nuint);
                        } while (lengthToExamine > offset);
                    }

                    // Do final compare as sizeof(nuint) from end rather than start
                    result = (LoadNUInt(ref first, lengthToExamine) == LoadNUInt(ref second, lengthToExamine));
                    goto Result;
                }
            }

        // As there are so many true/false exit points the Jit will coalesce them to one location.
        // We want them at the end so the conditional early exit jmps are all jmp forwards so the
        // branch predictor in a uninitialized state will not take them e.g.
        // - loops are conditional jmps backwards and predicted
        // - exceptions are conditional forwards jmps and not predicted
        NotEqual:
            return false;
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static ushort LoadUShort(ref byte start)
            => Unsafe.ReadUnaligned<ushort>(ref start);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static uint LoadUInt(ref byte start)
            => Unsafe.ReadUnaligned<uint>(ref start);
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static nuint LoadNUInt(ref byte start)
            => Unsafe.ReadUnaligned<nuint>(ref start);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static nuint LoadNUInt(ref byte start, nuint offset)
            => Unsafe.ReadUnaligned<nuint>(ref Unsafe.AddByteOffset(ref start, offset));
    }
}
