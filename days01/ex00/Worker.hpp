#ifndef WORKER_HPP
# define WORKER_HPP

# include "Position.hpp"
# include "Statistic.hpp"
# include "Tool.hpp"
# include <vector>
# include <algorithm>
# include <set>

class Workshop;

class Worker
{
private:
	Position				coordonnee;
	Statistic				stat;
	std::vector<Tool *>		tools;
	std::set<Workshop *>	workshops;

public:
	Worker() : coordonnee(), stat()
    {
        std::cout << "Worker created" << std::endl;
    }
	
	~Worker(); 

	void addTool(Tool *tool);
	void removeTool(Tool *tool);
	
	void registerToWorkshop(Workshop *workshop);
	void leaveWorkshop(Workshop *workshop);
    
    void work();
};

#endif
