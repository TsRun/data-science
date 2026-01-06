#ifndef GRAPH_HPP
# define GRAPH_HPP

# include "Vector2.hpp"
# include <vector>
# include <fstream>
# include <string>
# include <list>
# include <utility> // for std::pair
# include <cmath>

class Graph
{
private:
	Vector2					_size;
	std::vector<Vector2>	_points;
	// Store lines as pairs of points
	std::vector< std::pair<Vector2, Vector2> > _lines;

	void	_fitBounds(const Vector2& p_point);

public:
	Graph();
	~Graph();

	void	addPoint(const Vector2& p_point);
	void	addLine(const Vector2& p_a, const Vector2& p_b);
	void	readFromFile(const char *filename);
	void	display() const;
	void	saveToPNG(const char *filename) const;
};

#endif
