---
title: Redis as Mutex
description: Using redis as a resource mutex in distributed systems.
date: 2026-09-12
lang: en
translationId: redis-semaphores-mutex
tags:
  - data
  - cache
  - concurrency
  - mutex
  - nodejs
draft: false
appUrl: https://api.redis-semaphores.devman.cc
---
# Where do we start?
We can start by learning what a **thread** is—the smallest unit of execution within a process, allowing a program to perform multiple operations simultaneously.

Threads share memory and resources (files and data) with other threads within the same process, allowing efficient communication between them; but it also adds complexity to resource management to avoid conflicts. That's where *Mutex* comes into action.

*Mutex (Mutual Exclusion)*—a synchronization primitive used in concurrent programming to protect shared resources from being accessed by multiple threads at the same time. In simple terms, a resource gatekeeper.
## How does it work?
![[Pasted image 20260914165322.png]]

A thread wants to use a resource, asks the mutex if it is available, locks it until the thread is finished, and then releases it so another thread can use it. If another thread wants to use that same resource while still being used in another thread, it waits until it is available.

In a distributed system, where we want to lock a resource to be accessed by multiple instances, normal mutex would not be enough as these resources will be locked only within each process, and we want them locked for multiple processes. To satisfy this requirement we need a shared store that multiple processes can access, and Redis looks like a nice solution because it provides an atomic operation (`SET key value NX PX ttl`) to acquire the lock only if no other process already holds it.

The request travels through a load balancer which sends it to the available process and then it checks with Redis whether the resource is available. When the process gets a locked request, it will wait until it is available so it can be locked by the new process.

---
# Use Case

Let's imagine a coffee shop managed by Marty McFly and Dr. Emmett Brown to get some money to fuel the DeLorean, since they came to a very expensive future. But each of them has a specialty in preparing coffee (even though it's an espresso machine), Marty is good at preparing cappuccinos and flat whites, while the doc is good at preparing macchiato and cortados. Each one of them has two espresso machines, so they are able to make two coffees at a time.

The clients make the order through a screen and this will enqueue the order in its respective queue depending on the coffee they ordered. As there are multiple screens to take the orders and send them at the same time, there has to be a way of sharing the resources across the screens.

![[Pasted image 20260914170649.png]]

---
# Implementation

## Prerequisites
- NodeJS 
- Redis
### Libraries
- ioredis
- redis-semaphore

As a first step, let's configure our client by initializing `Redis` with the host and port where it will be exposed. Also, define the resources that are going to be shared across the processes.

```typescript
import Redis from 'ioredis'

export const RedisClient = new Redis({
	host: 'host',
	port: 'port',
});

const COFFEE_MACHINES: Record<string, string[]> = {
	'Marty': ['Machine 1', 'Machine 2'],
	'Emmett': ['Machine 3', 'Machine 4']
};
```

Then, evaluate the barista you requested, and create an array of promises that will be in charge to fetch the first machine available for the required barista. Once you got the array of promises resolve them with `Promise.race` to resolve just the first it got.

```typescript
import { Mutex } from 'redis-semaphore';

const coffeeMachines = COFFEE_MACHINES[baristaName];
let acquiredMutex: Mutex | null = null;
let acquiredMachineId: string | null = null;

const immediateAcquirePromises = coffeeMachines.map(async (machineId) => {
  // The baristaName normally would come from the request	
  const mutex = new Mutex(RedisClient, `${baristaName}:${machineId}`, { retryInterval: 0 });
  try {
    await mutex.acquire();
    return { machineId, mutex, success: true };
  } catch (err: any) {
    return { machineId, mutex: null, success: false };
  }
});

try {
  const raceResult = await Promise.race(immediateAcquirePromises);
  if (raceResult.success && raceResult.mutex) {
    acquiredMutex = raceResult.mutex;
    acquiredMachineId = raceResult.machineId;

    for (const promise of immediateAcquirePromises) {
      promise.then((result) => {
        if (result.success && result.mutex && result.machineId !== acquiredMachineId) {
          result.mutex.release().catch(() => {
          });
        }
      }).catch(() => {
      });
    }
  }
} catch (err) {}

if (!acquiredMutex) {
  const waitPromises = coffeeMachines.map(async (terminalId) => {
    const mutex = new Mutex(RedisClient, `${baristaName}:${terminalId}`, {
      retryInterval: 100,
      acquireTimeout: 60000
    });
    try {
      await mutex.acquire();
      return { terminalId, mutex, success: true };
    } catch (err: any) {
      return { terminalId, mutex: null, success: false };
    }
  });

  const raceResult = await Promise.race(waitPromises);

  if (raceResult.success && raceResult.mutex) {
    acquiredMutex = raceResult.mutex;
    acquiredMachineId = raceResult.terminalId;

    for (const promise of waitPromises) {
      promise.then((result) => {
        if (result.success && result.mutex && result.terminalId !== acquiredMachineId) {
          result.mutex.release().catch(() => {});
        }
      }).catch(() => {});
    }
  }
}

if (!acquiredMutex || !acquiredMachineId) {
  throw new Error("Could not acquire...");
}
```

After processing the resource, you can release it so it becomes available for another process to use.

```typescript
try {
  // ... your process here
} catch (err: any) {
  throw err;
} finally {
  await acquiredMutex.release();
}
```

**One caveat worth mentioning:** this approach relies on a single Redis instance to coordinate the lock, which means that instance becomes a single point of failure. If it goes down, no process can acquire or release locks until it's back up. For a toy example or a low-stakes use case this is usually fine, but if you're protecting something critical in production, you'll want to look into [Redlock](https://redis.io/docs/latest/develop/use/patterns/distributed-locks/), Redis's own algorithm for distributed locking across multiple independent Redis nodes, which trades a bit of complexity for much better fault tolerance.

This way, you can work with Redis as a *Mutex* to control usage of resources in a distributed system.