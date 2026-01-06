#ifndef VECTOR2_HPP
# define VECTOR2_HPP

class Vector2
{
private:
	float	_x;
	float	_y;

public:
	Vector2(float p_x, float p_y);
	Vector2(); // Default constructor needed for containers sometimes, or just explicit
	~Vector2();

	float	getX() const;
	float	getY() const;
};

#endif
