#include "deadlock_detector.h"
#include <vector>

#define maxT 50 //maxNumOfCurrentTransaction

bool abortT[maxT];

//POSSIBLE DATA STRUCTURE FOR WAIT-FOR GRAPH
bool waitFor[maxT][maxT];

void DeadlockDetector::BuildWaitForGraph()
{
	for each(KeyValuePair<int, ReadWriteFIFOLock^> kvp in LockManager::lockTable)
	{
		int oid = kvp.Key;
		ReadWriteFIFOLock^ lock = kvp.Value;

		// Make sure Monitor::Exit(lock->_m) is called for every Monitor::Enter
		// (so watch out if you use "continue" or "break")
		Monitor::Enter(lock->_m);

		//BEGIN OF TODO

		//TRAVERSAL waitQ
		for each (Request^ req in lock->waitQ)
		{
			req->pid;
			req->type;
		}

		//TRAVERSAL lockingList
		for each (int pid in lock->lockingList)
		{
		}

		if (lock->waitQ->Count > 0)
		{
			//FIRST ELEMENT OF waitQ
			Request^ req = lock->waitQ->First->Value;
							
			if (lock->waitQ->Count > 1)
			{
				//SECOND ELEMENT OF waitQ
				Request^ r = lock->waitQ->First->Next->Value;
			}
		}

		//END OF TODO

		Monitor::Exit(lock->_m);
	}
}

void DeadlockDetector::AnalyzeWaitForGraph()
{
	// TODO : Add your code here.
}


void DeadlockDetector::AbortTransactions()
{
	//DO NOT CHANGE ANY CODE HERE
	bool deadlock = false;

	for (int i = 0; i < maxT; ++i)
		if (abortT[i])
		{
			deadlock = true;
			break;
		}

	if (!deadlock) {
		Console::WriteLine("no deadlock found");
	}
	else {
		for (int i = 0; i < maxT; ++i)
			if (abortT[i])
			{
				Console::WriteLine("DD: Abort Transaction: " + i);
			}

		for each(KeyValuePair<int, ReadWriteFIFOLock^> kvp in LockManager::lockTable)
		{
			int oid = kvp.Key;
			ReadWriteFIFOLock^ lock = kvp.Value;

			Monitor::Enter(lock->_m);

			bool pall = false;

			for each(Request^ req in lock->waitQ)
				if (abortT[req->pid])
				{
					lock->abortList->Add(req->pid);
					lock->wakeUpList->Add(req->pid);
					Console::WriteLine("DD: Transaction " + req->pid + " cancel object " + oid);
					pall = true;
				}

			if (pall)
			{
				Monitor::PulseAll(lock->_m);
			}

			Monitor::Exit(lock->_m);
		}
	}
}

void DeadlockDetector::run()
{
	while (true)
	{
		Thread::Sleep(timeInterval);

		//BEGIN OF TODO
		memset(waitFor, 0 ,sizeof(waitFor));
		//INITIALIZE ANY OTHER DATA STRUCTURES YOU DECLARE.
		//END OF TODO

		memset(abortT, 0 ,sizeof(abortT));

		Monitor::Enter(LockManager::lockTable);

		BuildWaitForGraph();

		AnalyzeWaitForGraph();

		AbortTransactions();

		Monitor::Exit(LockManager::lockTable);
	}
}
