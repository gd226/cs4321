#include <iostream>
#include <ctime>
using namespace std;

#include "minirel.h"

#include "SortTestDriver.h"

int MINIBASE_RESTART_FLAG = 0;


int main(int argc, char *argv[])
{
	// Initialize Minibase and the B+ tree.
	char *dbName ="sortdb";
	char *logName ="sortlog";

	remove(dbName);
	remove(logName);

	Status status;
	minibase_globals = new SystemDefs(status, dbName, logName, 2000, 500, 1000);

	if (status != OK) {
	cerr << "ERROR: Couldn'initialize the Minibase globals" << endl;
	minibase_errors.show_errors();

	cerr << "Hit [enter] to continue..." << endl;
	cin.get();
	exit(1);
	}

	SortTestDriver std;

	std.TestAll();

	delete minibase_globals;

	std::cout << "Hit [enter] to continue..." << endl;
	std::cin.get();

	return 0;
}

