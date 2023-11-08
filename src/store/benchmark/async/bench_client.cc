// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * benchmark.cpp:
 *   simple replication benchmark client
 *
 * Copyright 2013 Dan R. K. Ports  <drkp@cs.washington.edu>
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use, copy,
 * modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 **********************************************************************/

#include "store/benchmark/async/bench_client.h"

#include "lib/latency.h"
#include "lib/message.h"
#include "lib/transport.h"
#include "lib/timeval.h"

#include <sys/time.h>
#include <string>
#include <sstream>
#include <algorithm>

DEFINE_LATENCY(op);

BenchmarkClient::BenchmarkClient(Transport &transport, uint64_t id,
		int numRequests, int expDuration, uint64_t delay, int warmupSec,
    int cooldownSec, int tputInterval, const std::string &latencyFilename) :
    id(id),
    tputInterval(tputInterval),
    transport(transport),
    rand(id),
    numRequests(numRequests), expDuration(expDuration),	delay(delay),
    warmupSec(warmupSec), cooldownSec(cooldownSec),
    latencyFilename(latencyFilename){
	if (delay != 0) {
		Notice("Delay between requests: %ld ms", delay);
	} else {
    Notice("No delay between requests.");
  }
	started = false;
	done = false;
  cooldownStarted = false;
  if (numRequests > 0) {
	  latencies.reserve(numRequests);
  }
  _Latency_Init(&latency, "txn");
}

BenchmarkClient::~BenchmarkClient() {
}

void BenchmarkClient::Start(bench_done_callback bdcb, bool batchOptimization, bool benchmark) {
  Debug("BenchmarkClient::Start\n");
  ycsb = benchmark;
	n = 0;
  curr_bdcb = bdcb;
  transport.Timer(warmupSec * 1000, std::bind(&BenchmarkClient::WarmupDone,
			this));
  gettimeofday(&startTime, NULL);
  if (tputInterval > 0) {
		msSinceStart = 0;
		opLastInterval = n;
		transport.Timer(tputInterval, std::bind(&BenchmarkClient::TimeInterval,
				this));
  }
  //ここがレイテンシ測定のstart
  Latency_Start(&latency);
  //この行き先は、async_transaction_bench_client
  if (batchOptimization){
    SendNext_batch();
  }
  else if (ycsb){
    SendNext_ycsb();
  }
  else{
    SendNext();
  }
  
}

void BenchmarkClient::TimeInterval() {
  if (done) {
	  return;
  }

  struct timeval tv;
  gettimeofday(&tv, NULL);
  msSinceStart += tputInterval;
  Notice("Completed %d requests at %lu ms", n-opLastInterval,
      (((tv.tv_sec*1000000+tv.tv_usec)/1000)/10)*10);
  opLastInterval = n;
  transport.Timer(tputInterval, std::bind(&BenchmarkClient::TimeInterval,
      this));
}

void BenchmarkClient::WarmupDone() {
  started = true;
  Notice("Completed warmup period of %d seconds with %d requests", warmupSec,
      n);
  n = 0;
}

void BenchmarkClient::CooldownDone() {
  done = true;

  char buf[1024];
  Notice("Finished cooldown period.");
  std::sort(latencies.begin(), latencies.end());

	Debug("crashing because latencies of size %d", latencies.size());

	if(latencies.size()>0){
		uint64_t ns = latencies[latencies.size()/2];
		LatencyFmtNS(ns, buf);
		Debug("Median latency is %ld ns (%s)", ns, buf);

		ns = 0;
		for (auto latency : latencies) {
			ns += latency;
		}
		ns = ns / latencies.size();
		LatencyFmtNS(ns, buf);
		Notice("Average latency is %ld ns (%s)", ns, buf);

		ns = latencies[latencies.size()*90/100];
		LatencyFmtNS(ns, buf);
		Debug("90th percentile latency is %ld ns (%s)", ns, buf);

		ns = latencies[latencies.size()*95/100];
		LatencyFmtNS(ns, buf);
		Debug("95th percentile latency is %ld ns (%s)", ns, buf);

		ns = latencies[latencies.size()*99/100];
		LatencyFmtNS(ns, buf);
		Debug("99th percentile latency is %ld ns (%s)", ns, buf);
	}

	curr_bdcb();
}

void BenchmarkClient::OnReply(int result) {
  Debug("BenchmarkClient::OnReply");
  IncrementSent(result);

  ycsb = 1;

  if (done) {
    return;
  }

  if (delay == 0) { 
    Latency_Start(&latency);
    if (ycsb){
      SendNext_ycsb();
    }
    else{
      SendNext();
    }
    
  } else {
    uint64_t rdelay = rand() % delay*2;
    if (ycsb){
      transport.Timer(rdelay, std::bind(&BenchmarkClient::SendNext_ycsb, this));
    }
    else{
      transport.Timer(rdelay, std::bind(&BenchmarkClient::SendNext, this));
    }
  }
}


void BenchmarkClient::OnReply_batch(std::vector<transaction_status_t> results) {
  Debug("BenchmarkClient::OnReply");

  IncrementSent_batch(results);

  if (done) {
    return;
  }

  if (delay == 0) { 
    Latency_Start(&latency);
    SendNext_batch();
  } else {
    uint64_t rdelay = rand() % delay*2;
    transport.Timer(rdelay, std::bind(&BenchmarkClient::SendNext_batch, this));
  }
}

void BenchmarkClient::StartLatency() {
  Debug("StartLatency is called \n");
  Latency_Start(&latency);
}

void BenchmarkClient::IncrementSent(int result) {
  if (started) {
    Notice("IncrementSent is called \n");
    // record latency
    if (!cooldownStarted) {
      uint64_t ns = Latency_End(&latency);
      // TODO: use standard definitions across all clients for success/commit and failure/abort
      if (result == 0) { // only record result if success
        struct timespec curr;
        clock_gettime(CLOCK_MONOTONIC, &curr);
        if (latencies.size() == 0UL) {
          gettimeofday(&startMeasureTime, NULL);
          startMeasureTime.tv_sec -= ns / 1000000000ULL;
          startMeasureTime.tv_usec -= (ns % 1000000000ULL) / 1000ULL;
          Debug("startMeasureTime : %d, %d \n", startMeasureTime.tv_sec, startMeasureTime.tv_usec);
          //std::cout << "#start," << startMeasureTime.tv_sec << "," << startMeasureTime.tv_usec << std::endl;
        }
        uint64_t currNanos = curr.tv_sec * 1000000000ULL + curr.tv_nsec;
        latencies.push_back(ns);
      }
    }

    if (numRequests == -1) {
      struct timeval currTime;
      gettimeofday(&currTime, NULL);

      struct timeval diff = timeval_sub(currTime, startTime);
      if (diff.tv_sec >= expDuration - cooldownSec && !cooldownStarted) {
        //diff.tv_sec() > expDuration(configで設定した時間) - cooldownSec()
        Notice("Starting cooldown after %ld seconds.", diff.tv_sec);
        Notice("expDuration : %ld", expDuration);
        Notice("Cooldown time : %ld", cooldownSec);
        Finish();
      } else if (diff.tv_sec > expDuration) {
        Notice("Finished cooldown after %ld seconds.", diff.tv_sec);
        CooldownDone();
      } else {
        Debug("Not done after %ld seconds.", diff.tv_sec);
      }
    } else if (n >= numRequests){
      CooldownDone();
    }
  }

  n++;
}

void BenchmarkClient::IncrementSent_batch(std::vector<transaction_status_t> results) {
  int commit_num = 0;
  if (started) {
    Debug("IncrementSent is called \n");
    int batchSize = results.size();
    // record latency
    if (!cooldownStarted) {
      uint64_t ns = Latency_End(&latency);
      // TODO: use standard definitions across all clients for success/commit and failure/abort
      for(int i = 0; i < batchSize; i++){
        if (results[i] == 0) { // only record result if success
          Debug("commit");
          commit_num++;
          struct timespec curr;
          clock_gettime(CLOCK_MONOTONIC, &curr);
          if (latencies.size() == 0UL) {
            gettimeofday(&startMeasureTime, NULL);
            startMeasureTime.tv_sec -= ns / 1000000000ULL;
            startMeasureTime.tv_usec -= (ns % 1000000000ULL) / 1000ULL;
            Debug("startMeasureTime : %d, %d \n", startMeasureTime.tv_sec, startMeasureTime.tv_usec);
            //std::cout << "#start," << startMeasureTime.tv_sec << "," << startMeasureTime.tv_usec << std::endl;
          }
          uint64_t currNanos = curr.tv_sec * 1000000000ULL + curr.tv_nsec;
          latencies.push_back(ns);
        }
      }
    }

    if (numRequests == -1) {
      struct timeval currTime;
      gettimeofday(&currTime, NULL);

      struct timeval diff = timeval_sub(currTime, startTime);
      if (diff.tv_sec >= expDuration - cooldownSec && !cooldownStarted) {
        //diff.tv_sec() > expDuration(configで設定した時間) - cooldownSec()
        Debug("Starting cooldown after %ld seconds.", diff.tv_sec);
        Debug("expDuration : %ld", expDuration);
        Debug("Cooldown time : %ld", cooldownSec);
        Finish();
      } else if (diff.tv_sec > expDuration) {
        Debug("Finished cooldown after %ld seconds.", diff.tv_sec);
        CooldownDone();
      } else {
        Debug("Not done after %ld seconds.", diff.tv_sec);
      }
    } else if (n >= numRequests){
      CooldownDone();
    }
  }

  n += commit_num;
}

void BenchmarkClient::Finish() {
  gettimeofday(&endTime, NULL);

  Notice("endTime : %d, %d \n", endTime.tv_sec, endTime.tv_usec);

  // endTime(Finishが呼ばれたタイミング)からstartMeasureTime()を引いている。
  struct timeval diff = timeval_sub(endTime, startMeasureTime);

  Notice("#end:, %d, %d< %d \n", diff.tv_sec, diff.tv_usec, id);

  Notice("Completed %d requests in " FMT_TIMEVAL_DIFF " seconds", n,
      VA_TIMEVAL_DIFF(diff));

  double total_time, latency, throughput;

  total_time = diff.tv_sec * 1000000 + diff.tv_usec;

  //std::cout << "VA_TIMEVAL_DIFF(diff)" << VA_TIMEVAL_DIFF(diff) << std::endl;

  latency = total_time / n;

  throughput = n / total_time; 

  Notice("Latency : %lf (second), %lf (millisecond), %lf (microsecond) \n", latency / 1000000, latency / 1000, latency);

  Notice("Throughput : %lf (transaction/second) \n", throughput * 1000000); 

  if (latencyFilename.size() > 0) {
      Latency_FlushTo(latencyFilename.c_str());
  }

  if (numRequests == -1) {
    cooldownStarted = true;
  } else {
    CooldownDone();
  }
}
