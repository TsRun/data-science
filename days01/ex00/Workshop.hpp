#ifndef WORKSHOP_HPP
# define WORKSHOP_HPP

# include "Worker.hpp"
# include <set>
# include <iostream>

class Workshop
{
private:
	std::set<Worker *>	_workers;

public:
	Workshop() { std::cout << "Workshop created" << std::endl; }
	~Workshop() { std::cout << "Workshop destroyed" << std::endl; }

	void registerWorker(Worker *worker)
	{
		if (_workers.find(worker) == _workers.end())
		{
			_workers.insert(worker);
            std::cout << "Worker registered to workshop" << std::endl;
			worker->registerToWorkshop(this);
		}
	}

	void releaseWorker(Worker *worker)
	{
		if (_workers.find(worker) != _workers.end())
		{
			_workers.erase(worker);
            std::cout << "Worker released from workshop" << std::endl;
			worker->leaveWorkshop(this);
		}
	}

	void executeWorkDay()
	{
        std::cout << "Workshop is executing a work day..." << std::endl;
		for (std::set<Worker *>::iterator it = _workers.begin(); it != _workers.end(); ++it)
		{
			(*it)->work();
		}
	}
};

#endif
