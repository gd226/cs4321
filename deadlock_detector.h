#ifndef _DEADLOCK_H
#define _DEADLOCK_H

#include "lock.h"

public ref class DeadlockDetector
{
public:
	// the interval between consecutive runs of deadlock detection.
	int timeInterval;

	DeadlockDetector()
	{
		this->timeInterval = 1000;
	}

	DeadlockDetector(int time)
	{
		this->timeInterval = time;
	}

	void run();

	//--------------------------------------------------------------------
	// DeadlockDetector::BuildWaitForGraph
	//
	// Input    : None.
	// Purpose  : Build a wait for graph based on information in lockTable.
	// Return   : None.
	//--------------------------------------------------------------------
	void BuildWaitForGraph();
	
	//--------------------------------------------------------------------
	// DeadlockDetector::AnalyzeWaitForGraph
	//
	// Input    : None.
	// Purpose  : Analyze the wait for graph. Detect deadlock.
	//			  Decide on victims in case of deadlock
	// Return   : None.
	//--------------------------------------------------------------------
	void AnalyzeWaitForGraph();

	//--------------------------------------------------------------------
	// DeadlockDetector::AbortTransactions
	//
	// Input    : None.
	// Purpose  : Abort the victim transactions as decided by analysis.
	// Return   : None.
	//--------------------------------------------------------------------
	void AbortTransactions();
};

#endif //_DEADLOCK_H