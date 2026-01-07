#include "Graph.hpp"
#include "Vector2.hpp"
#include <iostream>

int main()
{
	Graph graph;

	std::cout << "Loading points from 'points.txt'..." << std::endl;
	
	std::ofstream outfile("points.txt");
	outfile << "0 0" << std::endl;
	outfile << "2 2" << std::endl;
	outfile << "6 2" << std::endl;
	outfile << "2 3" << std::endl;
	outfile.close();

	graph.readFromFile("points.txt");

	graph.addLine(Vector2(0, 0), Vector2(2, 2));
    graph.addLine(Vector2(2, 2), Vector2(6, 2));
    graph.addLine(Vector2(6, 2), Vector2(2, 3));

	std::cout << "Displaying graph with points:" << std::endl;
	graph.display();

	std::cout << "Saving graph to 'graph.png'..." << std::endl;
	graph.saveToPNG("graph.png");

	return (0);
}
