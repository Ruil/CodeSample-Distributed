/*
 * channel.h
 *
 *  Created on: Nov 24, 2016
 *      Author: rui
 */

#ifndef CHANNEL_H_
#define CHANNEL_H_

#include <list>
#include <thread>
#include <condition_variable>
#include <mutex>
template<class item>
class channel {
private:
	std::list<item> queue;
	std::mutex m;
	std::condition_variable cv;
public:
	void put(const item &i) {
		std::unique_lock < std::mutex > lock(m);
		queue.push_back(i);
		cv.notify_one();
	}
	item get() {
		std::unique_lock < std::mutex > lock(m);
		cv.wait(lock, [&]() {return !queue.empty();});
		item result = queue.front();
		queue.pop_front();
		return result;
	}
};

#endif /* CHANNEL_H_ */

