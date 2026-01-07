#ifndef TOOL_HPP
# define TOOL_HPP

# include <iostream>
# include <string>

class Worker;

class Tool
{
protected:
	int			_numberOfUses;
	Worker*		_worker;

public:
    Tool() : _numberOfUses(0), _worker(NULL) { std::cout << "Tool created" << std::endl; }
	virtual ~Tool() { std::cout << "Tool destroyed" << std::endl; }

	virtual void use() = 0;

	void setWorker(Worker* worker)
    {
        _worker = worker;
    }

	Worker* getWorker() const
    {
        return _worker;
    }
};

#endif
