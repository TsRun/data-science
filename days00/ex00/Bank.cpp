#include "Bank.hpp"

// Account Implementation
int	Bank::Account::_nbAccounts = 0;

Bank::Account::Account() :
	_id(_nbAccounts++),
	_value(0)
{
}

const int	&Bank::Account::getId() const
{
	return (_id);
}

const int	&Bank::Account::getValue() const
{
	return (_value);
}

std::ostream& operator << (std::ostream& p_os, const Bank::Account& p_account)
{
	p_os << "[" << p_account._id << "] - [" << p_account._value << "]";
	return (p_os);
}

// Bank Implementation
Bank::Bank() :
	_liquidity(0)
{
}

Bank::~Bank()
{
	for (std::map<int, Account *>::iterator it = _clientAccounts.begin(); it != _clientAccounts.end(); ++it)
		delete it->second;
	_clientAccounts.clear();
}

const int	&Bank::getLiquidity() const
{
	return (_liquidity);
}

void	Bank::setLiquidity(const int &p_liquidity)
{
	_liquidity = p_liquidity;
}

void	Bank::createAccount()
{
	Account *newAccount = new Account();
	_clientAccounts.insert(std::pair<int, Account *>(newAccount->getId(), newAccount));
}

void	Bank::deleteAccount(const int &p_id)
{
	std::map<int, Account *>::iterator it = _clientAccounts.find(p_id);
	if (it != _clientAccounts.end())
	{
		delete it->second;
		_clientAccounts.erase(it);
	}
	else
	{
		throw AccountNotFoundException();
	}
}

void	Bank::giveLoan(const int &p_id, const int &p_amount)
{
	if (_liquidity < p_amount)
		return ;
	std::map<int, Account *>::iterator it = _clientAccounts.find(p_id);
	if (it != _clientAccounts.end())
	{
		_liquidity -= p_amount;
		it->second->_value += p_amount;
	}
	else
	{
		throw AccountNotFoundException();
	}
}

void	Bank::deposit(const int &p_id, const int &p_amount)
{
	std::map<int, Account *>::iterator it = _clientAccounts.find(p_id);
	if (it != _clientAccounts.end())
	{
		int fee = p_amount * 0.05;
		_liquidity += fee;
		it->second->_value += (p_amount - fee);
	}
	else
	{
		throw AccountNotFoundException();
	}
}

Bank::Account&	Bank::operator[](int p_id)
{
	std::map<int, Account *>::iterator it = _clientAccounts.find(p_id);
	if (it == _clientAccounts.end())
		throw AccountNotFoundException();
	return (*it->second);
}

std::ostream& operator << (std::ostream& p_os, const Bank& p_bank)
{
	p_os << "Bank informations : " << std::endl;
	p_os << "Liquidity : " << p_bank._liquidity << std::endl;
	for (std::map<int, Bank::Account *>::const_iterator it = p_bank._clientAccounts.begin(); it != p_bank._clientAccounts.end(); ++it)
		p_os << *(it->second) << std::endl;
	return (p_os);
}
