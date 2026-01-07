#ifndef SHOVEL_HPP
# define SHOVEL_HPP

# include "Tool.hpp"

class Shovel : public Tool
{
public:
	Shovel() : Tool() { std::cout << "Shovel created" << std::endl; }
	~Shovel() { std::cout << "Shovel destroyed" << std::endl; }

	void use()
	{
		std::cout << "Shovel used. Digging..." << std::endl;
		_numberOfUses++;
	}
};

#endif
