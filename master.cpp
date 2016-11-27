#include "master.h"

//  A worker is arrived. The master prepares to register it.
Status Master::Register(ServerContext* context, const RegisterRequest* request,
		RegisterReply* reply) {
	std::string worker_address = request->name();
	std::cout << "Register RPC recieved: " << worker_address << std::endl;

	// master's reply to the worker
	reply->set_message(worker_address);

	// send the worker's information to a go style channel
	channel_register.put(worker_address);
	return Status::OK;
}

//  The master registers new workers and put them into a worker's pool.
void Master::RegisterHandler() {
	// running a register thread though the master's lifetime.
	while (true) {
		// receives block when the register pool is empty, otherwise go ahead to register a worker
		std::string worker_address = channel_register.get();

		// register a worker
		RegisteredWorker* worker = new RegisteredWorker(
				grpc::CreateChannel(worker_address,
						grpc::InsecureChannelCredentials()), worker_address);

		// protect workers' unordered_map
		std::unique_lock < std::mutex > lk(m_workermap);
		map_workers[worker->address_] = worker;
		lk.unlock();

		// after the register, a worker is available so that the master can put it to work when necessary
		channel_available.put(worker->address_);
	}
}

/**
 If a job need to be done and a worker is available, put the worker to process the job.
 The multi-process synchronization problem is similar to the producer-consumer model:
 1. ProcessJob handouts jobs and thus it consumes unassigned jobs.
 2. HandleResponse consumes responses from workers.

 The two concurrent processes end when all the jobs are processed successfully by the works.

 @param completed_jobs: jobs that are completed
 @param unassigned_jobs: jobs waiting to be processed
 @param type:   Map or Reduce phase
 @param n_jobs: number of jobs of current phase
 @param other_phase_jobs: number of jobs of the other phases
 */
void Master::HandOutJobs(std::vector<int>& completed_jobs,
		std::vector<int>& unassigned_jobs, std::string type, int n_jobs,
		int other_phase_jobs) {
	// if there is no jobs, the function simply returns.
	if (unassigned_jobs.size() == 0)
		return;

	// 1. a thread handles worker's response
	std::thread thead_handelresponse(&Master::HandleResponse, this,
			std::ref(completed_jobs), std::ref(unassigned_jobs), n_jobs);

	// 2. a thread hands out jobs to works
	std::thread thread_processjbs(&Master::ProcessJob, this,
			std::ref(completed_jobs), std::ref(unassigned_jobs), type,
			other_phase_jobs);

	// wait for the two thread to finish
	thead_handelresponse.join();
	thread_processjbs.join();
}

//  Hand out jobs to the next available worker
void Master::ProcessJob(std::vector<int>& completed_jobs,
		std::vector<int>& unassigned_jobs, std::string type,
		int other_phase_jobs) {
	while (true) {
		// when all the jobs are done, break the loop (do not use return, because threads need to join)
		std::unique_lock < std::mutex > lk_completed(m_completed);
		if (completed) {
			break;
		}

		// when job size equals 0, we have to wait until either:
		// 1. all the jobs are completed, or
		// 2. any job is available.
		if (unassigned_jobs.size() == 0) {
			lk_completed.unlock();
			std::unique_lock < std::mutex > lk_ready(m_ready);
			ready = false;

			// sleep if no job to assign
			cv.wait(lk_ready, [this]
			{	return ready;});

			// all the jobs are done, end loop
			lk_completed.lock();
			if (completed) {
				break;
			}
		}

		// when there is a job to assign, pop out a job
		int jobnumber = unassigned_jobs.back();
		unassigned_jobs.pop_back();

		// we have to wait for the next available worker
		std::string worker_address = channel_available.get();

		// put the worker to do the job
		CallDoJob(worker_address, file, jobnumber, type, other_phase_jobs);
	}
}

// DoJob RPC to worker
void Master::CallDoJob(std::string worker_address, std::string file,
		int jobnumber, std::string type, int other_phase_jobs) {
	DoJobRequest request;
	request.set_file(file);
	request.set_jobtype(type);
	request.set_jobnumber(jobnumber);
	request.set_numotherphase(other_phase_jobs);

	// get worker's registered information
	RegisteredWorker* worker;
	std::unique_lock < std::mutex > lk(m_workermap);
	worker = map_workers[worker_address];
	lk.unlock();

	// start async call of DoJob
	AsyncClientCall* call = new AsyncClientCall;
	call->address = worker_address;
	call->response_reader = worker->stub_->AsyncDoJob(&call->context, request,
			&register_cq_);
	call->response_reader->Finish(&call->reply, &call->status, (void*) call);
}

// Loop while listening for completed responses.
void Master::AsyncCompleteRpcDoJob() {
	void* got_tag;
	bool ok = false;

	// Block until the next result is available in the completion queue "cq".
	while (register_cq_.Next(&got_tag, &ok)) {
		AsyncClientCall* call = static_cast<AsyncClientCall*>(got_tag);
		GPR_ASSERT(ok);
		// put worker's response to channel
		channel_response.put(std::make_pair(call->status.ok(), call->reply));

		// worker becomes available for the next job
		if (call->status.ok()) {
			channel_available.put(call->address);
		}
		delete call;
	}
}

//  Handles responses from workers. Be responsible for waking up ProcessJob thread.
void Master::HandleResponse(std::vector<int>& completed_jobs,
		std::vector<int>& unassigned_jobs, int n_jobs) {
	while (true) {
		// if all the jobs are completed, 1) wake up the prcoessjob thread, and 2) break
		if (completed_jobs.size() == n_jobs) {
			// set the completed flag
			std::unique_lock < std::mutex > lk_completed(m_completed);
			completed = true;

			// set the read flag
			std::unique_lock < std::mutex > lk_ready(m_ready);
			ready = true;

			// wake up ProcessJob
			cv.notify_one();
			break;
		}

		// if no job is assigned, just wait for responses from workers
		std::pair<bool, DoJobReply> response = channel_response.get();

		// handle response:
		// 1. if ok, then a job is completed
		// 2. otherwise  the master's RPC to the worker fails (e.g., due to network issue), reÂ­assign the job given to the failed worker to another worker.
		if (response.first) {
			// add job to the completed pool
			completed_jobs.push_back(response.second.jobnumber());
		} else {
			// unregister the worker due to failed RPC
			std::unique_lock < std::mutex > lk(m_workermap);
			delete map_workers[response.second.address()];
			map_workers.erase(response.second.address());
			lk.unlock();

			// push the job back to pool
			unassigned_jobs.push_back(response.second.jobnumber());

			// wake up ProcessJob thread, ProcessJob thread will reassgin the job
			std::unique_lock < std::mutex > lk_ready(m_ready);
			ready = true;
			cv.notify_one();

			std::cout << "failed job: " << response.second.jobnumber()
					<< std::endl;
		}
	}
}
/**
 The pipeline for master to send out mapreduce jobs to workers. There are three steps:
 1. registers workers
 2. handouts map phase jobs to workers, and waits for the workers
 3. handouts reduce phase jobs to workers, and waits for the workers.

 */
void Master::RunMaster() {
	// 0. Spawn dojob thread that loops indefinitely
	std::thread thread_dojob = std::thread(&Master::AsyncCompleteRpcDoJob,
			this);
	// 1. when a worker present, the master registers it, so that if a worker become available, the master put it into work.
	std::thread thread_regsiter(&Master::RegisterHandler, this);

	// 2. start to process map jobs
	std::cout << "start Map..." << std::endl;

	// 2.1 maintain a job list to assign to workers
	completed = false;
	std::vector<int> map_unassigned_jobs;
	for (int i = 0; i < n_map; i++) {
		map_unassigned_jobs.push_back(i);
	}
	std::cout << "number of map jobs to assign: " << n_map << std::endl;

	// 2.2 maintain a completed job list from workers, if all finished, start to process reduce jobs
	std::vector<int> map_completed_jobs;
	HandOutJobs(map_completed_jobs, map_unassigned_jobs, Map, n_map, n_reduce);
	std::cout << "end Map." << std::endl;

	// 3. finished map jobs, start to process reduce jobs
	std::cout << "start Reduce..." << std::endl;

	// 3.1 finished map jobs, start to process reduce jobs
	completed = false;
	std::vector<int> reduce_unassigned_jobs;
	for (int i = 0; i < n_reduce; i++) {
		reduce_unassigned_jobs.push_back(i);
	}
	std::cout << "number of reduce jobs to assign: " << n_reduce << std::endl;

	// 3.2 maintain a completed job list from workers
	std::vector<int> reduce_completed_jobs;
	HandOutJobs(reduce_completed_jobs, reduce_unassigned_jobs, Reduce, n_reduce,
			n_map);
	std::cout << "end Reduce." << std::endl;

	// wait for register thread
	thread_regsiter.join();
	thread_dojob.join();
}

void RunServer(std::string file, int n_map, int n_reduce) {
	std::string server_address("0.0.0.0:50051");
	Master service(file, n_map, n_reduce);

	ServerBuilder builder;
	// Listen on the given address without any authentication mechanism.
	builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());

	// Register "service" as the instance through which we'll communicate with
	// clients.
	builder.RegisterService(&service);

	// assemble the server.
	std::unique_ptr<Server> server(builder.BuildAndStart());
	std::cout << "Server listening on " << server_address << std::endl;

	// Wait for the server to shutdown.
	service.RunMaster();
	server->Wait();
}

int main(int argc, char** argv) {
	// 30 map jobs and 60 reduce jobs
	RunServer("", 30, 60);
	return 0;
}

