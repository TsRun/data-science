#include "Graph.hpp"
#include <iostream>
#include <fstream>
#include <cmath>
#include <vector>
#include <stdint.h>

namespace PngUtils
{
	static uint32_t	crc_table[256];
	static bool		table_computed = false;

	static void make_crc_table(void)
	{
		uint32_t c;
		for (int n = 0; n < 256; n++) {
			c = (uint32_t)n;
			for (int k = 0; k < 8; k++) {
				if (c & 1)
					c = 0xedb88320L ^ (c >> 1);
				else
					c = c >> 1;
			}
			crc_table[n] = c;
		}
		table_computed = true;
	}

	static uint32_t update_crc(uint32_t crc, const unsigned char *buf, int len)
	{
		uint32_t c = crc;
		if (!table_computed)
			make_crc_table();
		for (int n = 0; n < len; n++) {
			c = crc_table[(c ^ buf[n]) & 0xff] ^ (c >> 8);
		}
		return c;
	}

	static uint32_t crc32(const unsigned char *buf, int len)
	{
		return update_crc(0xffffffffL, buf, len) ^ 0xffffffffL;
	}

	static uint32_t adler32(unsigned char *data, size_t len)
	{
		uint32_t a = 1, b = 0;
		size_t index;
		for (index = 0; index < len; ++index)
		{
			a = (a + data[index]) % 65521;
			b = (b + a) % 65521;
		}
		return (b << 16) | a;
	}

	static void bigEndian(uint32_t val, unsigned char* bytes)
	{
		bytes[0] = (val >> 24) & 0xFF;
		bytes[1] = (val >> 16) & 0xFF;
		bytes[2] = (val >> 8) & 0xFF;
		bytes[3] = (val) & 0xFF;
	}
}

Graph::Graph() :
	_size(0, 0)
{
}

Graph::~Graph()
{
}

void	Graph::_fitBounds(const Vector2& p_point)
{
	float maxX = _size.getX();
	float maxY = _size.getY();

	if (p_point.getX() + 1 > maxX)
		maxX = p_point.getX() + 1;
	if (p_point.getY() + 1 > maxY)
		maxY = p_point.getY() + 1;
	
	_size = Vector2(maxX, maxY);
}

void	Graph::addPoint(const Vector2& p_point)
{
	if (p_point.getX() < 0 || p_point.getY() < 0)
		return ;
	_points.push_back(p_point);
	_fitBounds(p_point);
}

void	Graph::addLine(const Vector2& p_a, const Vector2& p_b)
{
	if (p_a.getX() < 0 || p_a.getY() < 0 || p_b.getX() < 0 || p_b.getY() < 0)
		return;
	_lines.push_back(std::make_pair(p_a, p_b));
	_fitBounds(p_a);
	_fitBounds(p_b);
}

void	Graph::readFromFile(const char *filename)
{
	std::ifstream infile(filename);
	if (!infile.is_open())
	{
		std::cerr << "Error: could not open file " << filename << std::endl;
		return;
	}

	float x, y;
	while (infile >> x >> y)
	{
		addPoint(Vector2(x, y));
	}
	infile.close();
}

void	Graph::display() const
{
	for (int y = (int)_size.getY() - 1; y >= 0; --y)
	{
		std::cout << "& " << y << " ";
		for (int x = 0; x < (int)_size.getX(); ++x)
		{
			bool found = false;
			for (std::vector<Vector2>::const_iterator it = _points.begin(); it != _points.end(); ++it)
			{
				if ((int)it->getX() == x && (int)it->getY() == y)
				{
					found = true;
					break;
				}
			}
			if (found)
				std::cout << "X ";
			else
				std::cout << ". ";
		}
		std::cout << std::endl;
	}
	
	std::cout << "&   "; 
	for (int x = 0; x < (int)_size.getX(); ++x)
	{
		std::cout << x << " ";
	}
	std::cout << std::endl;
}

static void drawLineGeneric(std::vector<unsigned char>& pixels, int w, int h, int x0, int y0, int x1, int y1, unsigned char r, unsigned char g, unsigned char b)
{
	int dx = std::abs(x1 - x0);
	int sx = x0 < x1 ? 1 : -1;
	int dy = -std::abs(y1 - y0);
	int sy = y0 < y1 ? 1 : -1;
	int err = dx + dy;
	
	while (true)
	{
		if (x0 >= 0 && x0 < w && y0 >= 0 && y0 < h)
		{
			int idx = (y0 * w + x0) * 3;
			pixels[idx] = r;
			pixels[idx + 1] = g;
			pixels[idx + 2] = b;
		}
		if (x0 == x1 && y0 == y1) break;
		int e2 = 2 * err;
		if (e2 >= dy) { err += dy; x0 += sx; }
		if (e2 <= dx) { err += dx; y0 += sy; }
	}
}

static void drawDigit(std::vector<unsigned char>& pixels, int w, int h, int x, int y, int digit)
{
	static const unsigned char font[10][5] = {
		{7, 5, 5, 5, 7}, 
		{2, 6, 2, 2, 7}, 
		{7, 1, 7, 4, 7}, 
		{7, 1, 7, 1, 7}, 
		{5, 5, 7, 1, 1}, 
		{7, 4, 7, 1, 7}, 
		{7, 4, 7, 5, 7}, 
		{7, 1, 1, 2, 2}, 
		{7, 5, 7, 5, 7}, 
		{7, 5, 7, 1, 7}  
	};
	
	if (digit < 0 || digit > 9) return;
	
	for (int row = 0; row < 5; ++row)
	{
		for (int col = 0; col < 3; ++col)
		{
			if ((font[digit][row] >> (2 - col)) & 1)
			{
				int px = x + col;
				int py = y + row;
				if (px >= 0 && px < w && py >= 0 && py < h)
				{
					int idx = (py * w + px) * 3;
					pixels[idx] = 0;
					pixels[idx+1] = 0;
					pixels[idx+2] = 0; 
				}
			}
		}
	}
}

static void drawNumber(std::vector<unsigned char>& pixels, int w, int h, int x, int y, int number)
{
	if (number == 0)
	{
		drawDigit(pixels, w, h, x, y, 0);
		return;
	}
	char buf[16];
	snprintf(buf, sizeof(buf), "%d", number);
	int offset = 0;
	for (int i=0; buf[i]; i++)
	{
		drawDigit(pixels, w, h, x + offset, y, buf[i] - '0');
		offset += 4; 
	}
}

void	Graph::saveToPNG(const char *filename) const
{
	int scale = 40;
	int marginLeft = 30;
	int marginBottom = 20;

	int logicW = (int)_size.getX();
	int logicH = (int)_size.getY();

	int w = logicW * scale + marginLeft + scale; 
	int h = logicH * scale + marginBottom + scale; 
	
	std::vector<unsigned char> rawPixels(w * h * 3, 255);

	int zeroX = marginLeft + 10;
	int zeroY = h - marginBottom - 10;

	for (int i = 0; i <= logicW; ++i)
	{
		int x = zeroX + i * scale;
		drawLineGeneric(rawPixels, w, h, x, zeroY, x, zeroY - logicH * scale, 200, 200, 200);
		drawLineGeneric(rawPixels, w, h, x, zeroY, x, zeroY + 5, 0, 0, 0);
		drawNumber(rawPixels, w, h, x - 3, zeroY + 8, i);
	}
	for (int j = 0; j <= logicH; ++j)
	{
		int y = zeroY - j * scale;
		drawLineGeneric(rawPixels, w, h, zeroX, y, zeroX + logicW * scale, y, 200, 200, 200);
		drawLineGeneric(rawPixels, w, h, zeroX, y, zeroX - 5, y, 0, 0, 0);
		drawNumber(rawPixels, w, h, zeroX - 15, y - 2, j);
	}

	drawLineGeneric(rawPixels, w, h, zeroX, zeroY, zeroX + logicW * scale, zeroY, 0, 0, 0); 
	drawLineGeneric(rawPixels, w, h, zeroX, zeroY, zeroX, zeroY - logicH * scale, 0, 0, 0); 

	for (std::vector< std::pair<Vector2, Vector2> >::const_iterator it = _lines.begin(); it != _lines.end(); ++it)
	{
		int x0 = zeroX + (int)it->first.getX() * scale;
		int y0 = zeroY - (int)it->first.getY() * scale;
		
		int x1 = zeroX + (int)it->second.getX() * scale;
		int y1 = zeroY - (int)it->second.getY() * scale;

		drawLineGeneric(rawPixels, w, h, x0, y0, x1, y1, 0, 0, 0);
	}

	for (std::vector<Vector2>::const_iterator it = _points.begin(); it != _points.end(); ++it)
	{
		int cx = zeroX + (int)it->getX() * scale;
		int cy = zeroY - (int)it->getY() * scale;
		int r = 5; 
		for (int dy = -r; dy <= r; dy++)
		{
			for (int dx = -r; dx <= r; dx++)
			{
				int px = cx + dx;
				int py = cy + dy;
				if (px >= 0 && px < w && py >= 0 && py < h)
				{
					int idx = (py * w + px) * 3;
					rawPixels[idx] = 255;   
					rawPixels[idx + 1] = 0;
					rawPixels[idx + 2] = 0;
				}
			}
		}
	}

	std::ofstream file(filename, std::ios::binary);
	if (!file.is_open())
	{
		std::cerr << "Could not create file " << filename << std::endl;
		return;
	}

	const unsigned char png_signature[] = {0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A};
	file.write((char*)png_signature, 8);

	{
		unsigned char ihdr[13];
		PngUtils::bigEndian(w, ihdr);
		PngUtils::bigEndian(h, ihdr + 4);
		ihdr[8] = 8; 
		ihdr[9] = 2; 
		ihdr[10] = 0; 
		ihdr[11] = 0; 
		ihdr[12] = 0; 
		
		unsigned char lengthBytes[4];
		PngUtils::bigEndian(13, lengthBytes);
		file.write((char*)lengthBytes, 4);        
		file.write("IHDR", 4);                    
		file.write((char*)ihdr, 13);              
		
		unsigned char crcData[17];
		const char* type = "IHDR";
		for(int i=0; i<4; i++) crcData[i] = type[i];
		for(int i=0; i<13; i++) crcData[4+i] = ihdr[i];
		
		uint32_t crc = PngUtils::crc32(crcData, 17);
		unsigned char crcBytes[4];
		PngUtils::bigEndian(crc, crcBytes);
		file.write((char*)crcBytes, 4);
	}

	{
		std::vector<unsigned char> filteredData;
		for (int y = 0; y < h; y++)
		{
			filteredData.push_back(0); 
			for (int x = 0; x < w; x++)
			{
				int idx = (y * w + x) * 3;
				filteredData.push_back(rawPixels[idx]);
				filteredData.push_back(rawPixels[idx+1]);
				filteredData.push_back(rawPixels[idx+2]);
			}
		}

		std::vector<unsigned char> zlibData;
		zlibData.push_back(0x78);
		zlibData.push_back(0x01);

		size_t pos = 0;
		while (pos < filteredData.size())
		{
			size_t chunkSize = filteredData.size() - pos;
			if (chunkSize > 65535) chunkSize = 65535;
			bool last = (pos + chunkSize == filteredData.size());

			unsigned char bType = last ? 0x01 : 0x00;
			zlibData.push_back(bType);
			
			uint16_t len = (uint16_t)chunkSize;
			uint16_t nlen = ~len;
			zlibData.push_back(len & 0xFF);
			zlibData.push_back((len >> 8) & 0xFF);
			zlibData.push_back(nlen & 0xFF);
			zlibData.push_back((nlen >> 8) & 0xFF);

			for (size_t i = 0; i < chunkSize; ++i)
				zlibData.push_back(filteredData[pos + i]);
			
			pos += chunkSize;
		}

		uint32_t adler = PngUtils::adler32(&filteredData[0], filteredData.size());
		unsigned char adlerBytes[4];
		PngUtils::bigEndian(adler, adlerBytes);
		zlibData.push_back(adlerBytes[0]);
		zlibData.push_back(adlerBytes[1]);
		zlibData.push_back(adlerBytes[2]);
		zlibData.push_back(adlerBytes[3]);

		uint32_t idatLen = zlibData.size();
		unsigned char lenBytes[4];
		PngUtils::bigEndian(idatLen, lenBytes);
		file.write((char*)lenBytes, 4);
		file.write("IDAT", 4);
		file.write((char*)&zlibData[0], zlibData.size());

		std::vector<unsigned char> crcBuffer;
		crcBuffer.push_back('I'); crcBuffer.push_back('D'); crcBuffer.push_back('A'); crcBuffer.push_back('T');
		crcBuffer.insert(crcBuffer.end(), zlibData.begin(), zlibData.end());
		uint32_t crc = PngUtils::crc32(&crcBuffer[0], crcBuffer.size());
		unsigned char crcBytes[4];
		PngUtils::bigEndian(crc, crcBytes);
		file.write((char*)crcBytes, 4);
	}

	{
		unsigned char lenBytes[4] = {0,0,0,0};
		file.write((char*)lenBytes, 4);
		file.write("IEND", 4);
		unsigned char crcData[] = {'I', 'E', 'N', 'D'};
		uint32_t crc = PngUtils::crc32(crcData, 4);
		unsigned char crcBytes[4];
		PngUtils::bigEndian(crc, crcBytes);
		file.write((char*)crcBytes, 4);
	}

	file.close();
	std::cout << "Graph saved to " << filename << std::endl;
}
