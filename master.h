/**
 master.cpp
 Purpose: exploit multiple cores/machines by using a master thread/node handing out jobs to workers, and handles failures of workers.

 Use case:
 1. A master splits work into a number of jobs.
 2. A master registers a worker when a Register RPC is received.
 3. A master thread/node hands out computation jobs via RPC to the workers and waits for them to finish.
 4. A master waits for an available worker or a worker to finish, before it can hand out more jobs.

 FaultÂ­-tolerant:
 1. The master reÂ­assigns the job given to the failed worker to another worker, if a RPC fails.
 2. TODO: master faultÂ­-tolerant.

 @author Rui Liu
 @version 1.0 Nov 24, 2016
 */

#ifndef MASTER_H_
#define MASTER_H_

#include <vector>
#include <iostream>
#include <memory>
#include <mutex>
#include <utility>
#include <unordered_map>
#include <string>
#include <condition_variable>
#include <chrono>
#include <thread>

#include "util/channel.h"
#include "util/common.h"

#include <grpc++/grpc++.h>
#include <grpc/support/log.h>
#include "client_server.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientAsyncResponseReader;
using grpc::CompletionQueue;

using client_server::RegisterRequest;
using client_server::RegisterReply;
using client_server::DoJobRequest;
using client_server::DoJobReply;
using client_server::Common;

//  Registered worker's information
class RegisteredWorker {
public:
	/**
	 A new worker that is registered.

	 @param channel: create a gRPC channel for the stub, specifying the worker's address and port we want to connect.
	 @param address: the worker's address
	 */
	RegisteredWorker(std::shared_ptr<Channel> channel, std::string me) :
			stub_(Common::NewStub(channel)), address_(me) {
	}

	std::string address_;
	std::unique_ptr<Common::Stub> stub_;
};

class Master final : public Common::Service {
private:
	Status Register(ServerContext* context, const RegisterRequest* request,
			RegisterReply* reply) override;

	void RegisterHandler();

	void AsyncCompleteRpcDoJob();

	void CallDoJob(std::string worker_address, std::string file, int jobnumber,
			std::string type, int other_phase_jobs);

	void HandOutJobs(std::vector<int>& completed_jobs,
			std::vector<int>& unassigned_jobs, std::string type, int n_jobs,
			int other_phase_jobs);

	void ProcessJob(std::vector<int>& completed_jobs,
			std::vector<int>& unassigned_jobs, std::string type,
			int other_phase_jobs);

	void HandleResponse(std::vector<int>& completed_jobs,
			std::vector<int>& unassigned_jobs, int n_jobs);

public:
	void RunMaster();

	// The constructor to initialize states and variables
	explicit Master(std::string _file, int _n_map, int _n_reduce) :
			file(_file), n_map(_n_map), n_reduce(_n_reduce), ready(false), completed(
			false) {
	}

	~Master(void) {
		for (auto wk : map_workers) {
			delete wk.second;
			map_workers.erase(wk.first);
		}
		cout << "Object is being deleted" << endl;
	}

private:
	bool ready;bool completed;
	int n_map;
	int n_reduce;
	std::string file;
	std::unordered_map<std::string, RegisteredWorker*> map_workers;

	std::mutex m_completed;
	std::mutex m_workermap;
	std::mutex m_ready;
	std::condition_variable cv;

	// use go style channel to â€œget concurrency rightâ€, it doesn't support the functions of select and close.
	// to some level, this allows to synchronize without explicit locks or condition variables.
	channel<std::string> channel_register;
	channel<std::string> channel_available;
	channel<std::pair<bool, DoJobReply> > channel_response;

	CompletionQueue register_cq_;

	struct AsyncClientCall {
		std::string address;
		DoJobReply reply;
		ClientContext context;
		Status status;
		std::unique_ptr<ClientAsyncResponseReader<DoJobReply>> response_reader;
	};
};

#endif /* MASTER_H_ */

