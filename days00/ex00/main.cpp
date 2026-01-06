#include "Bank.hpp"

int	main()
{
	Bank bank = Bank();
	bank.setLiquidity(999);
	bank.createAccount();
	bank.createAccount();

	std::cout << "Initial state :" << std::endl;
	std::cout << bank << std::endl;

	try
	{
		bank.deposit(0, 100);
		std::cout << "Deposit 100 to account 0 :" << std::endl;
		std::cout << bank << std::endl;

		bank.giveLoan(1, 100);
		std::cout << "Loan 100 to account 1 :" << std::endl;
		std::cout << bank << std::endl;

		// Test operator[]
		std::cout << "Accessing Account 1 via operator[] :" << std::endl;
		std::cout << bank[1] << std::endl;

		bank.deleteAccount(0);
		std::cout << "Delete account 0 :" << std::endl;
		std::cout << bank << std::endl;
		
		std::cout << "CHECK FOR ID UNIQUENESS AFTER DELETION" << std::endl;
		bank.createAccount();
		std::cout << "Created new account (should have ID 2, not 0) :" << std::endl;
		std::cout << bank << std::endl;

		std::cout << "Trying to delete invalid account 99 :" << std::endl;
		bank.deleteAccount(99);
	}
	catch (std::exception &e)
	{
		std::cout << "Caught exception: " << e.what() << std::endl;
	}

	std::cout << "--------------------------------" << std::endl;
	std::cout << "Trying to access deleted account 0:" << std::endl;
	try
	{
		std::cout << bank[0] << std::endl;
	}
	catch (std::exception &e)
	{
		std::cout << "Caught exception: " << e.what() << std::endl;
	}

	return (0);
}
