#ifndef BANK_HPP
# define BANK_HPP

# include <iostream>
# include <map>
# include <exception>

class Bank
{
public:
	class Account
	{
		friend class Bank;

	private:
		int				_id;
		int				_value;
		static int		_nbAccounts;

		Account();
		Account(const Account&);
		Account& operator=(const Account&);

	public:
		const int	&getId() const;
		const int	&getValue() const;

		friend std::ostream& operator << (std::ostream& p_os, const Account& p_account);
	};

private:
	int							_liquidity;
	std::map<int, Account *>	_clientAccounts;

	Bank& operator=(const Bank&);

public:
	Bank();
	~Bank();

	const int	&getLiquidity() const;
	void		setLiquidity(const int &p_liquidity);
	void		createAccount();
	void		deleteAccount(const int &p_id);
	void		giveLoan(const int &p_id, const int &p_amount);
	void		deposit(const int &p_id, const int &p_amount);

	Account&	operator[](int p_id);

	class AccountNotFoundException : public std::exception
	{
		public:
			virtual const char* what() const throw()
			{
				return ("Account not found");
			}
	};

	friend std::ostream& operator << (std::ostream& p_os, const Bank& p_bank);
};

#endif
