/**
 worker.cpp
 Purpose: the worker nodes (client side) of multiple cores/machines perform computations
 (e.g., execute Map() and Reduce() functions).

 Use case:
 1. When a worker starts, it sends a Register RPC to the master.
 2. If a job RPC is received, the worker will perform the computation (e.g., Map or Reduce), and send back the reply after the job is done.

 @author Rui Liu
 @version 2.0 Nov 24, 2016
 */

#ifndef WORKER_H_
#define WORKER_H_

#include <iostream>
#include <memory>
#include <thread>
#include <string>

#include "util/common.h"

#include <grpc++/grpc++.h>
#include <grpc/support/log.h>
#include "client_server.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerCompletionQueue;

using grpc::Status;
using grpc::Channel;
using grpc::ClientContext;

using client_server::RegisterRequest;
using client_server::RegisterReply;
using client_server::DoJobRequest;
using client_server::DoJobReply;
using client_server::Common;

class Worker final : public Common::AsyncService {
public:
	~Worker() {
		server_->Shutdown();
		dojob_cq_->Shutdown();
	}

	/**
	 Creates a new worker that can be used to perform computations.

	 @param channel: create a gRPC channel for the stub, specifying the server address and port we want to connect.
	 @param address: specify the worker's address
	 */
	explicit Worker(std::shared_ptr<Channel> channel, std::string address) :
			stub_(Common::NewStub(channel)), address_(address) {
	}

	void Run();
	std::string Register();

private:
	void AsyncCompleteRegisterRpc();
	void HandleDoJobRpcs();

	std::string address_;
	// as a client
	std::unique_ptr<Common::Stub> stub_;
	// as a server
	std::unique_ptr<ServerCompletionQueue> dojob_cq_;
	std::unique_ptr<Server> server_;

	class CallData {
	public:
		CallData(Common::AsyncService* service, ServerCompletionQueue* cq) :
				service_(service), cq_(cq), responder_(&ctx_), status_(CREATE) {
			Proceed();
		}

		void Proceed();
		void DoJob();

		void DoMap();
		void DoReduce();

	private:
		DoJobRequest request_;
		DoJobReply reply_;

		// As an asynchronous server.
		Common::AsyncService* service_;
		ServerCompletionQueue* cq_;
		ServerContext ctx_;
		// The means to get back to the client.
		ServerAsyncResponseWriter<DoJobReply> responder_;
		enum CallStatus {
			CREATE, PROCESS, FINISH
		};
		CallStatus status_;  // The current serving state.
	};
};

#endif /* WORKER_H_ */

