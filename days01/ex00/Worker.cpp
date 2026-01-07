#include "Worker.hpp"
#include "Workshop.hpp"

Worker::~Worker()
{
	std::cout << "Worker destroyed" << std::endl;
    while (!workshops.empty())
    {
        Workshop* w = *workshops.begin();
        leaveWorkshop(w);
         w->releaseWorker(this);
    }
}

void Worker::addTool(Tool *tool)
{
	if (tool)
	{
		Worker* currentOwner = tool->getWorker();
		if (currentOwner != NULL)
		{
			currentOwner->removeTool(tool);
		}

		tools.push_back(tool);
		tool->setWorker(this);
		std::cout << "Tool added to worker" << std::endl;
	}
}

void Worker::removeTool(Tool *tool)
{
	std::vector<Tool *>::iterator it = std::find(tools.begin(), tools.end(), tool);
	if (it != tools.end())
	{
		tools.erase(it);
		tool->setWorker(NULL);
		std::cout << "Tool removed from worker" << std::endl;
	}
}

void Worker::registerToWorkshop(Workshop *workshop)
{
	if (workshops.find(workshop) == workshops.end())
	{
		workshops.insert(workshop);
		workshop->registerWorker(this);
	}
}

void Worker::leaveWorkshop(Workshop *workshop)
{
	if (workshops.find(workshop) != workshops.end())
	{
		workshops.erase(workshop);
		workshop->releaseWorker(this);
	}
}

void Worker::work()
{
	std::cout << "Worker starts working..." << std::endl;
	if (tools.empty())
	{
		std::cout << "Worker has no tools to work with!" << std::endl;
	}
	else
	{
		for (std::vector<Tool *>::iterator it = tools.begin(); it != tools.end(); ++it)
		{
			(*it)->use();
		}
	}
}
