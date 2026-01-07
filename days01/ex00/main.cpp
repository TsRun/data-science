#include "Position.hpp"
#include "Statistic.hpp"
#include "Worker.hpp"
#include "Shovel.hpp"
#include "Hammer.hpp"
#include "Workshop.hpp"

int main()
{
	std::cout << "--- Creating Workers ---" << std::endl;
	Worker worker1;
	Worker worker2;

	std::cout << "\n--- Creating Tools ---" << std::endl;
	Shovel shovel;
	Hammer hammer;

	std::cout << "\n--- Assigning Tools ---" << std::endl;
	worker1.addTool(&shovel);
	worker1.addTool(&hammer);

	std::cout << "\n--- Creating Workshops ---" << std::endl;
	Workshop workshop1;
	Workshop workshop2;

	std::cout << "\n--- Registering Workers ---" << std::endl;
	workshop1.registerWorker(&worker1);
	workshop2.registerWorker(&worker1);
	workshop2.registerWorker(&worker2);

	std::cout << "\n--- Work Day ---" << std::endl;
	std::cout << "Workshop 1:" << std::endl;
	workshop1.executeWorkDay();
	
	std::cout << "Workshop 2:" << std::endl;
	workshop2.executeWorkDay();

	std::cout << "\n--- Stealing Tool ---" << std::endl;
	std::cout << "Worker 2 takes the shovel from Worker 1..." << std::endl;
	worker2.addTool(&shovel);
	
	std::cout << "\n--- Work Day Again ---" << std::endl;
	std::cout << "Workshop 1:" << std::endl;
	workshop1.executeWorkDay();

	std::cout << "Workshop 2:" << std::endl;
	workshop2.executeWorkDay();

	std::cout << "\n--- Cleanup (Destructors) ---" << std::endl;
	return 0;
}

