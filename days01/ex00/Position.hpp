#ifndef POSITION_HPP
# define POSITION_HPP

# include <iostream>

struct Position
{
	int x;
	int y;
	int z;

	Position() : x(0), y(0), z(0) { std::cout << "Position created" << std::endl; }
	Position(int p_x, int p_y, int p_z) : x(p_x), y(p_y), z(p_z) { std::cout << "Position created with values" << std::endl; }
	~Position() { std::cout << "Position destroyed" << std::endl; }
};

#endif
