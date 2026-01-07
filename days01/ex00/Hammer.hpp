#ifndef HAMMER_HPP
# define HAMMER_HPP

# include "Tool.hpp"

class Hammer : public Tool
{
public:
	Hammer() : Tool() { std::cout << "Hammer created" << std::endl; }
	~Hammer() { std::cout << "Hammer destroyed" << std::endl; }

	void use()
	{
		std::cout << "Hammer used. Banging..." << std::endl;
		_numberOfUses++;
	}
};

#endif
