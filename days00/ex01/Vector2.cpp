#include "Vector2.hpp"

Vector2::Vector2(float p_x, float p_y) :
	_x(p_x),
	_y(p_y)
{
}

Vector2::Vector2() :
	_x(0),
	_y(0)
{
}

Vector2::~Vector2()
{
}

float	Vector2::getX() const
{
	return (_x);
}

float	Vector2::getY() const
{
	return (_y);
}
