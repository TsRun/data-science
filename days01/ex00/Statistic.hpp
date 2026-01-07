#ifndef STATISTIC_HPP
# define STATISTIC_HPP

# include <iostream>

struct Statistic
{
	int level;
	int exp;

	Statistic() : level(0), exp(0) { std::cout << "Statistic created" << std::endl; }
	Statistic(int p_level, int p_exp) : level(p_level), exp(p_exp) { std::cout << "Statistic created with values" << std::endl; }
	~Statistic() { std::cout << "Statistic destroyed" << std::endl; }
};

#endif
