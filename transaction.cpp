#include "transaction.h"
#include "lock.h"
#include <vector>
#include <algorithm>
using namespace std;

Transaction::Transaction()
{
	this->status = NEW;
}

Status Transaction::AbortTransaction()
{
	this->status = ABORTED;
	ReleaseAllLocks();
	cout << "Transaction " << this->tid << " Aborted" << endl;
	return OK;
}

void Transaction::InsertLock(int oid, bool shared)
{
	bool LockRes = true;
	if (!shared)
		LockRes = LockManager::AcquireExclusiveLock(this->tid, oid);
	else 
		LockRes = LockManager::AcquireSharedLock(this->tid, oid);

	if (!LockRes){
		AbortTransaction();
		return;
	}

	pair <int, bool> NewPair(oid, shared);
	if (shared)
		LockList.push_back(NewPair);
	else
	{
		int i;
		for(i = 0; i < LockList.size(); i++)
		{
			if (LockList.at(i).first == oid)
			{
				LockList.at(i).second = shared;
				break;
			}
		}
		if (i == LockList.size())
			LockList.push_back(NewPair);
	}
}

void Transaction::ReleaseAllLocks()
{
	int i;
	for (i = LockList.size() - 1; i >= 0; i--)
	{
		if(LockList.at(i).second)
			LockManager::ReleaseSharedLock(this->tid, LockList.at(i).first);
		else
			LockManager::ReleaseExclusiveLock(this->tid, LockList.at(i).first);
		LockList.pop_back();
	}
}

Status Transaction::Read(KeyType key, DataType &value)
{
	int LockNo = LockList.size();
	InsertLock(key, true);
	if (LockList.size() != LockNo + 1)
		return FAIL;
	TSHI->GetValue(key, value);
	return OK;
}

Status Transaction::AddWritePair(KeyType key, DataType value, OpType op)
{
	KVP NewKVP = {key, value, op}; 
	writeList.push_back(NewKVP);
	return OK;
}

Status Transaction::GroupWrite()
{
	vector<KVP>::iterator it;
	int LockNo = LockList.size();
	for(it = writeList.begin(); it < writeList.end(); it++)
	{
		InsertLock(it->key, false);
		if (LockList.size() != LockNo + 1)
			break;
		else
			LockNo++;
	}
	if (LockList.size() != LockNo)
		return FAIL;

	this ->status = GROUPWRITE;
	for(int i = writeList.size() - 1; i >= 0; i--)
	{
		KVP it = writeList.at(i);
		if (it.op == UPDATE)
			TSHI->UpdateValue(it.key, it.value);
		else if (it.op == DELETE)
			TSHI->DeleteKey(it.key);
		else if (it.op == INSERT)
			TSHI->InsertKeyValue(it.key, it.value);
		writeList.pop_back();
	}
	return OK;
}

Status Transaction::StartTranscation()
{
	if (this->status != NEW)
	{
		return FAIL;
	}

	this->status = RUNNING;

	cout << "Transaction " << this->tid << " Started" << endl;
	return OK;
}

Status Transaction::EndTransaction()
{
	if (this->status != ABORTED) {
		ReleaseAllLocks();
	}

	return OK;
}

Status Transaction::CommitTransaction()
{
	if ((this->status != RUNNING) && (this->status != GROUPWRITE))
	{
		return FAIL;
	}

	cout << "Transaction " << this->tid << " Committed" << endl;
	this->status = COMMITTED;
	return OK;
}
