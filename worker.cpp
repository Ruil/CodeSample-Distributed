#include "worker.h"

// Set up a connection with the master, register with the master, and wait for jobs from the master
void Worker::Run() {
	ServerBuilder builder;
	// 1. listen on the given address
	builder.AddListeningPort(address_, grpc::InsecureServerCredentials());

	// 2. register "service"
	builder.RegisterService(this);

	// 3. assemble the server.;
	dojob_cq_ = builder.AddCompletionQueue();
	server_ = builder.BuildAndStart();
	std::cout << "Server listening on " << address_ << std::endl;

	// 4. register send rpc to server
	Register();

	// 5. As a server, handle rpcs
	HandleDoJobRpcs();

	// 6. wait for the server to shutdown.
	server_->Wait();
}

// Tell the master we exist and ready to work.
std::string Worker::Register() {
	// Register request we are sending to the master node.
	RegisterRequest request;
	request.set_name(address_);
	RegisterReply reply;
	ClientContext context;

	// The actual Register RPC.
	Status status = stub_->Register(&context, request, &reply);
	if (status.ok()) {
		std::cout << "Worker Registered: " << address_ << std::endl;
		return reply.message();
	} else {
		std::cout << status.error_code() << ": " << status.error_message()
				<< std::endl;
		return "RPC failed";
	}
}

// Spawn a new CallData instance to serve new clients.
void Worker::HandleDoJobRpcs() {
	new CallData(this, dojob_cq_.get());
	void* tag;  // uniquely identifies a request.
	bool ok;
	while (true) {
		// Block waiting to read the next event from the completion queue.
		// The return value of Next should always be checked.
		GPR_ASSERT(dojob_cq_->Next(&tag, &ok));
		GPR_ASSERT(ok);
		static_cast<CallData*>(tag)->Proceed();
	}
}

// The master node sent us a job.
void Worker::CallData::DoJob() {
	std::string type = request_.jobtype();
	int jobnumber = request_.jobnumber();

	// The action depends on the job type.
	if (type == Map) {
		DoMap();
	} else if (type == Reduce) {
		DoReduce();
	}

	// Send back
	reply_.set_message(OK);
	reply_.set_jobnumber(jobnumber);
}

// Once CallData is created, invoke the DoJob process right away.
void Worker::CallData::Proceed() {
	if (status_ == CREATE) {
		status_ = PROCESS;
		// Initial state,  *request* that start processing DoJob requests
		service_->RequestDoJob(&ctx_, &request_, &responder_, cq_, cq_, this);
	} else if (status_ == PROCESS) {
		new CallData(service_, cq_);
		// The actual processing.
		DoJob();
		// And we are done!
		status_ = FINISH;
		responder_.Finish(reply_, Status::OK, this);
	} else {
		GPR_ASSERT(status_ == FINISH);
		// Once in the FINISH state, deallocate ourselves (CallData).
		delete this;
	}
}

// Map phase: read split for job, call Map for that split, and create nreduce partitions.
void Worker::CallData::DoMap() {
	std::cout << "DoMap: " << OK << std::endl;
}

// Reduce phase: read map outputs for partition job, sort them by key, call reduce for eachkey
void Worker::CallData::DoReduce() {
	std::cout << "Do Reduce: " << OK << std::endl;
}

// Create a worker and start run
void RunWorker(std::string master_address, std::string self_address) {
	Worker service(
			grpc::CreateChannel(master_address,
					grpc::InsecureChannelCredentials()), self_address);
	service.Run();
}

// Create n workers for the purpose of testing.
int main(int argc, char** argv) {
	int n_worker = 10;

	std::string server_address = "localhost:50051";
	std::string worker_address_root = "localhost:";
	std::vector<std::thread> threads;

	// start n workers
	for (int i = 0; i < n_worker; i++) {
		threads.push_back(
				std::thread(RunWorker, server_address,
						worker_address_root + std::to_string(50061 + i)));
	}

	// wait to finish
	for (auto& th : threads)
		th.join();

	return 0;
}


