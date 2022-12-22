#include "MapReduceFramework.h"
#include <pthread.h>
#include <map>
#include <stdlib.h>
#include <string>
#include <iostream>
#include <atomic>
#include "Barrier.h"

#define PRINT_ERROR(s) fprintf(stdout, "system error: %s\n", s)
#define EXIT exit(1);

class Global;
class Comparator {
 public:
  bool operator()(const K2* c1, const K2* c2){
    return *c1 < *c2;
  }
};

struct ThreadContext{
    int id;
    size_t out_vec_size;
    IntermediateVec* vec;
    Global* global_context;
};

// global job handle
class Global{
 public:
  Global(const MapReduceClient& client,
         const InputVec& inputVec, OutputVec& outputVec,
         int multiThreadLevel):client(client), inputVec(inputVec),
         outputVec(outputVec),multiThreadLevel(multiThreadLevel),
         initial_size(inputVec.size()), barrier (multiThreadLevel),mutex(PTHREAD_MUTEX_INITIALIZER),
         reduced(0),total_shuffled(0), shuffled(0), finished(false)
    {
      std::atomic_init(&call_wait_job, false);
      std::atomic_init(&stage, 0);
       contexts = (ThreadContext**) malloc (multiThreadLevel * sizeof (*contexts));
       threads = (pthread_t**) malloc (multiThreadLevel * sizeof (*threads));

       // creae threads:
       for (int i = 0; i < multiThreadLevel; ++i) {
         contexts[i] = (ThreadContext*)calloc (1, sizeof (ThreadContext));
         contexts[i]->id = i;
         contexts[i]->vec = new IntermediateVec();
         contexts[i]->global_context = this;
         threads[i] = (pthread_t *) malloc (1 * sizeof (pthread_t));
         if(pthread_create (threads[i], nullptr, &map_reduce_phase, contexts[i]) != 0)
         {
             PRINT_ERROR("error in thread");
             exit(1);
         }
       }
    }

  void lock_mutex()
  {
     if (pthread_mutex_lock(&mutex) != 0)
     {
       PRINT_ERROR("can't lock mutex");
       EXIT
     }
  }
  void unlock_mutex()
  {
   if (pthread_mutex_unlock(&mutex) != 0)
   {
     PRINT_ERROR("can't unlock mutex");
     EXIT
   }
  }
  ~Global() {
   if(contexts)
   {
     for (int i = 0; i < multiThreadLevel; ++i) {
       delete contexts[i]->vec;
       free(contexts[i]);
     }
     free(contexts);
     contexts = nullptr;
   }
   if(threads)
   {
     for (int i = 0; i < multiThreadLevel; ++i) {
       free(threads[i]);
     }
     free(threads);
     threads = nullptr;
   }
  if (pthread_mutex_destroy(&mutex) != 0)
  {
      PRINT_ERROR("error on pthread_mutex_destroy");
      EXIT
  }
 }

 // threads handler
  static void* map_reduce_phase(void* context)
  {
   Global* gb = ((ThreadContext*) context)->global_context ;
   ThreadContext* thread = (ThreadContext*) context;
   gb->stage = MAP_STAGE;
  // map:
  gb->lock_mutex();
  while (!gb->inputVec.empty())
  {
    InputPair pair = gb->inputVec.back();
    gb->inputVec.erase (gb->inputVec.cend());
    gb->unlock_mutex();
    gb->client.map (pair.first, pair.second, context);
    gb->lock_mutex();

  }
  gb->total_shuffled += (int)(*thread->vec).size();
  gb->unlock_mutex();

  // shuffle:
  gb->barrier.barrier();
  if (thread->id == 0)
  {
    gb->stage = SHUFFLE_STAGE;
    for (int i = 0; i < gb->multiThreadLevel; ++i) {
      for (auto& pair : *gb->contexts[i]->vec) {
        if (gb->map.find(pair.first) == gb->map.end())
        {
          gb->map[pair.first] = IntermediateVec();
        }
        gb->map[pair.first].push_back (pair);
        ++gb->shuffled;
      }

    }

  }
  // wait:
  gb->barrier.barrier();
  // reduce:
  gb->stage = REDUCE_STAGE;

  gb->lock_mutex();
  while (!gb->map.empty())
  {
    IntermediateVec vec = gb->map.begin()->second;
    thread->out_vec_size = vec.size();
    gb->map.erase (gb->map.begin());
    gb->client.reduce(&vec, context);

    gb->unlock_mutex();
    // cs:
    gb->lock_mutex();

  }
  gb->unlock_mutex();
  gb->barrier.barrier();
  gb->finished = true;

  return 0;
  }

  std::atomic<int> stage;
  const MapReduceClient& client;
  InputVec inputVec;
  OutputVec& outputVec;
  int multiThreadLevel;
  pthread_t **threads;
  ThreadContext** contexts;
  std::atomic<bool> call_wait_job;
  size_t initial_size;
  Barrier barrier;
  pthread_mutex_t mutex;
  std::map<K2*, IntermediateVec, Comparator> map;
  int reduced, total_shuffled, shuffled;
  bool finished;

};


void emit2 (K2* key, V2* value, void* context)
{
  ThreadContext* t = (ThreadContext*) context;
  (*t->vec).push_back (IntermediatePair (key, value));

}


void emit3 (K3* key, V3* value, void* context)
{

  ThreadContext* t = (ThreadContext*) context;
  t->global_context->outputVec.push_back (OutputPair(key, value));
  // update how much reduced to percentage:
  t->global_context->reduced += (int) t->out_vec_size;

}

JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel){
  return new Global(client, inputVec, outputVec, multiThreadLevel);
}

void waitForJob(JobHandle job)
{
  Global* gb = (Global*) job;
  if (!gb->call_wait_job)
  {
    gb->call_wait_job = true;
    for (int i = 0; i < gb->multiThreadLevel; ++i) {
      if(pthread_join(*(gb->threads[i]), nullptr))
      {
          fprintf(stdout, "error in joining thread %d\n", i);
          EXIT
      }
    }
  }
}

void getJobState(JobHandle job, JobState* state)
{
  Global* gb = (Global*) job;
  state->stage =(stage_t)(gb->stage.load ());
  if(gb->finished)
  {
      state->percentage = 100;
      return;
  }
  if (state->stage == UNDEFINED_STAGE)
  {
    state->percentage = 0;
  }
  else if (state->stage == MAP_STAGE)
  {
    state->percentage =  100 * (1 -  ((float)gb->inputVec.size() / (float)gb->initial_size));
  }
  else if (state->stage == SHUFFLE_STAGE) {
    state->percentage = 100 * (((float)gb->shuffled/ (float)gb->total_shuffled));
  }
  else if (state->stage == REDUCE_STAGE) {
    state->percentage = 100 * (((float)gb->reduced/ (float)gb->total_shuffled));
  }

}

void closeJobHandle(JobHandle job)
{
  Global* gb =  static_cast<Global*>(job);
  waitForJob (gb);
  delete gb;
}

