import os
from mpi4py import MPI
import random
import time
import csv
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def compute_monte_carlo(total_throws):
    # Initialiser la graine aléatoire pour chaque processus
    seed = MPI.COMM_WORLD.Get_rank() + int(MPI.Wtime() * 1000)
    random.seed(seed)

    count_inside = 0
    for _ in range(int(total_throws)):
        x, y = random.random(), random.random()
        if x * x + y * y <= 1.0:
            count_inside += 1
    return count_inside

def master(scalability_tests, output_file):
    comm = MPI.COMM_WORLD

    try:
        with open(output_file, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['PI', 'Difference', 'Error', 'Ntot', 'AvailableProcessors', 'TimeDuration(ms)'])

            for test in scalability_tests:
                total_count = test['points']
                num_workers = test['workers']
                start_time = time.time()

                # Adjust points for different types of workers
                if num_workers == 8:
                    for i in range(1, num_workers + 1):
                        if i <= 4:  # First 4 workers (Pi 4)
                            comm.send(total_count * 0.2, dest=i, tag=0)
                        else:  # Next 4 workers (Pi Zero)
                            comm.send(total_count * 0.05, dest=i, tag=0)
                elif num_workers == 16:
                    for i in range(1, num_workers + 1):
                        if i <= 4 or (7 <= i <= 11):  # First 4 and workers 7 to 11 (Pi 4)
                            comm.send(total_count * 0.1, dest=i, tag=0)
                        else:  # Workers 5-6 and 12-16 (Pi Zero)
                            comm.send(total_count * 0.025, dest=i, tag=0)
                else:
                    for i in range(1, num_workers + 1):
                        comm.send(total_count // num_workers, dest=i, tag=0)  # Default distribution

                # Collect results from each worker
                total_inside = sum(comm.recv(source=i, tag=1) for i in range(1, num_workers + 1))
                pi_estimate = 4.0 * total_inside / total_count
                duration = time.time() - start_time
                error = abs(pi_estimate - 3.141592653589793)
                difference = pi_estimate - 3.141592653589793
                time_duration_ms = duration * 1000

                # Write to CSV
                writer.writerow([pi_estimate, difference, error, total_count, num_workers, time_duration_ms])
                logging.info(f'Written results for {num_workers} workers and {total_count} points to {output_file}')

    except Exception as e:
        logging.error(f'Failed to write to CSV file: {e}')

def worker():
    comm = MPI.COMM_WORLD
    while True:
        total_count = comm.recv(source=0, tag=0)
        total_inside = compute_monte_carlo(total_count)
        comm.send(total_inside, dest=0, tag=1)

if __name__ == "__main__":
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()  # Total number of processes

    # Define your scalability tests here
    scalability_tests_strong = [
        {'points': 50000, 'workers': 1},
        {'points': 50000, 'workers': 1},
        {'points': 50000, 'workers': 1},
        {'points': 50000, 'workers': 1},
        {'points': 50000, 'workers': 1},
        {'points': 50000, 'workers': 1},
        {'points': 50000, 'workers': 1},
        {'points': 50000, 'workers': 1},
        {'points': 50000, 'workers': 1},
        {'points': 50000, 'workers': 1},
        {'points': 50000, 'workers': 2},
        {'points': 50000, 'workers': 2},
        {'points': 50000, 'workers': 2},
        {'points': 50000, 'workers': 2},
        {'points': 50000, 'workers': 2},
        {'points': 50000, 'workers': 2},
        {'points': 50000, 'workers': 2},
        {'points': 50000, 'workers': 2},
        {'points': 50000, 'workers': 2},
        {'points': 50000, 'workers': 2},
        {'points': 50000, 'workers': 4},
        {'points': 50000, 'workers': 4},
        {'points': 50000, 'workers': 4},
        {'points': 50000, 'workers': 4},
        {'points': 50000, 'workers': 4},
        {'points': 50000, 'workers': 4},
        {'points': 50000, 'workers': 4},
        {'points': 50000, 'workers': 4},
        {'points': 50000, 'workers': 4},
        {'points': 50000, 'workers': 4},
        {'points': 50000, 'workers': 8},
        {'points': 50000, 'workers': 8},
        {'points': 50000, 'workers': 8},
        {'points': 50000, 'workers': 8},
        {'points': 50000, 'workers': 8},
        {'points': 50000, 'workers': 8},
        {'points': 50000, 'workers': 8},
        {'points': 50000, 'workers': 8},
        {'points': 50000, 'workers': 8},
        {'points': 50000, 'workers': 8},
        {'points': 50000, 'workers': 16},
        {'points': 50000, 'workers': 16},
        {'points': 50000, 'workers': 16},
        {'points': 50000, 'workers': 16},
        {'points': 50000, 'workers': 16},
        {'points': 50000, 'workers': 16},
        {'points': 50000, 'workers': 16},
        {'points': 50000, 'workers': 16},
        {'points': 50000, 'workers': 16},
        {'points': 50000, 'workers': 16},
        {'points': 500000, 'workers': 1},
        {'points': 500000, 'workers': 1},
        {'points': 500000, 'workers': 1},
        {'points': 500000, 'workers': 1},
        {'points': 500000, 'workers': 1},
        {'points': 500000, 'workers': 1},
        {'points': 500000, 'workers': 1},
        {'points': 500000, 'workers': 1},
        {'points': 500000, 'workers': 1},
        {'points': 500000, 'workers': 1},
        {'points': 500000, 'workers': 2},
        {'points': 500000, 'workers': 2},
        {'points': 500000, 'workers': 2},
        {'points': 500000, 'workers': 2},
        {'points': 500000, 'workers': 2},
        {'points': 500000, 'workers': 2},
        {'points': 500000, 'workers': 2},
        {'points': 500000, 'workers': 2},
        {'points': 500000, 'workers': 2},
        {'points': 500000, 'workers': 2},
        {'points': 500000, 'workers': 4},
        {'points': 500000, 'workers': 4},
        {'points': 500000, 'workers': 4},
        {'points': 500000, 'workers': 4},
        {'points': 500000, 'workers': 4},
        {'points': 500000, 'workers': 4},
        {'points': 500000, 'workers': 4},
        {'points': 500000, 'workers': 4},
        {'points': 500000, 'workers': 4},
        {'points': 500000, 'workers': 4},
        {'points': 500000, 'workers': 8},
        {'points': 500000, 'workers': 8},
        {'points': 500000, 'workers': 8},
        {'points': 500000, 'workers': 8},
        {'points': 500000, 'workers': 8},
        {'points': 500000, 'workers': 8},
        {'points': 500000, 'workers': 8},
        {'points': 500000, 'workers': 8},
        {'points': 500000, 'workers': 8},
        {'points': 500000, 'workers': 8},
        {'points': 500000, 'workers': 16},
        {'points': 500000, 'workers': 16},
        {'points': 500000, 'workers': 16},
        {'points': 500000, 'workers': 16},
        {'points': 500000, 'workers': 16},
        {'points': 500000, 'workers': 16},
        {'points': 500000, 'workers': 16},
        {'points': 500000, 'workers': 16},
        {'points': 500000, 'workers': 16},
        {'points': 500000, 'workers': 16},
        {'points': 5000000, 'workers': 1},
        {'points': 5000000, 'workers': 1},
        {'points': 5000000, 'workers': 1},
        {'points': 5000000, 'workers': 1},
        {'points': 5000000, 'workers': 1},
        {'points': 5000000, 'workers': 1},
        {'points': 5000000, 'workers': 1},
        {'points': 5000000, 'workers': 1},
        {'points': 5000000, 'workers': 1},
        {'points': 5000000, 'workers': 1},
        {'points': 5000000, 'workers': 2},
        {'points': 5000000, 'workers': 2},
        {'points': 5000000, 'workers': 2},
        {'points': 5000000, 'workers': 2},
        {'points': 5000000, 'workers': 2},
        {'points': 5000000, 'workers': 2},
        {'points': 5000000, 'workers': 2},
        {'points': 5000000, 'workers': 2},
        {'points': 5000000, 'workers': 2},
        {'points': 5000000, 'workers': 2},
        {'points': 5000000, 'workers': 4},
        {'points': 5000000, 'workers': 4},
        {'points': 5000000, 'workers': 4},
        {'points': 5000000, 'workers': 4},
        {'points': 5000000, 'workers': 4},
        {'points': 5000000, 'workers': 4},
        {'points': 5000000, 'workers': 4},
        {'points': 5000000, 'workers': 4},
        {'points': 5000000, 'workers': 4},
        {'points': 5000000, 'workers': 4},
        {'points': 5000000, 'workers': 8},
        {'points': 5000000, 'workers': 8},
        {'points': 5000000, 'workers': 8},
        {'points': 5000000, 'workers': 8},
        {'points': 5000000, 'workers': 8},
        {'points': 5000000, 'workers': 8},
        {'points': 5000000, 'workers': 8},
        {'points': 5000000, 'workers': 8},
        {'points': 5000000, 'workers': 8},
        {'points': 5000000, 'workers': 8},
        {'points': 5000000, 'workers': 16},
        {'points': 5000000, 'workers': 16},
        {'points': 5000000, 'workers': 16},
        {'points': 5000000, 'workers': 16},
        {'points': 5000000, 'workers': 16},
        {'points': 5000000, 'workers': 16},
        {'points': 5000000, 'workers': 16},
        {'points': 5000000, 'workers': 16},
        {'points': 5000000, 'workers': 16},
        {'points': 5000000, 'workers': 16},
        {'points': 50000000, 'workers': 1},
        {'points': 50000000, 'workers': 1},
        {'points': 50000000, 'workers': 1},
        {'points': 50000000, 'workers': 1},
        {'points': 50000000, 'workers': 1},
        {'points': 50000000, 'workers': 1},
        {'points': 50000000, 'workers': 1},
        {'points': 50000000, 'workers': 1},
        {'points': 50000000, 'workers': 1},
        {'points': 50000000, 'workers': 1},
        {'points': 50000000, 'workers': 2},
        {'points': 50000000, 'workers': 2},
        {'points': 50000000, 'workers': 2},
        {'points': 50000000, 'workers': 2},
        {'points': 50000000, 'workers': 2},
        {'points': 50000000, 'workers': 2},
        {'points': 50000000, 'workers': 2},
        {'points': 50000000, 'workers': 2},
        {'points': 50000000, 'workers': 2},
        {'points': 50000000, 'workers': 2},
        {'points': 50000000, 'workers': 4},
        {'points': 50000000, 'workers': 4},
        {'points': 50000000, 'workers': 4},
        {'points': 50000000, 'workers': 4},
        {'points': 50000000, 'workers': 4},
        {'points': 50000000, 'workers': 4},
        {'points': 50000000, 'workers': 4},
        {'points': 50000000, 'workers': 4},
        {'points': 50000000, 'workers': 4},
        {'points': 50000000, 'workers': 4},
        {'points': 50000000, 'workers': 4},
        {'points': 50000000, 'workers': 8},
        {'points': 50000000, 'workers': 8},
        {'points': 50000000, 'workers': 8},
        {'points': 50000000, 'workers': 8},
        {'points': 50000000, 'workers': 8},
        {'points': 50000000, 'workers': 8},
        {'points': 50000000, 'workers': 8},
        {'points': 50000000, 'workers': 8},
        {'points': 50000000, 'workers': 8},
        {'points': 50000000, 'workers': 8},
        {'points': 50000000, 'workers': 16},
        {'points': 50000000, 'workers': 16},
        {'points': 50000000, 'workers': 16},
        {'points': 50000000, 'workers': 16},
        {'points': 50000000, 'workers': 16},
        {'points': 50000000, 'workers': 16},
        {'points': 50000000, 'workers': 16},
        {'points': 50000000, 'workers': 16},
        {'points': 50000000, 'workers': 16},
        {'points': 50000000, 'workers': 16}
    ]

    scalability_tests_weak = [
        {'points': 50000, 'workers': 1},
        {'points': 50000, 'workers': 1},
        {'points': 50000, 'workers': 1},
        {'points': 50000, 'workers': 1},
        {'points': 50000, 'workers': 1},
        {'points': 50000, 'workers': 1},
        {'points': 50000, 'workers': 1},
        {'points': 50000, 'workers': 1},
        {'points': 50000, 'workers': 1},
        {'points': 50000, 'workers': 1},
        {'points': 100000, 'workers': 2},
        {'points': 100000, 'workers': 2},
        {'points': 100000, 'workers': 2},
        {'points': 100000, 'workers': 2},
        {'points': 100000, 'workers': 2},
        {'points': 100000, 'workers': 2},
        {'points': 100000, 'workers': 2},
        {'points': 100000, 'workers': 2},
        {'points': 100000, 'workers': 2},
        {'points': 100000, 'workers': 2},
        {'points': 200000, 'workers': 4},
        {'points': 200000, 'workers': 4},
        {'points': 200000, 'workers': 4},
        {'points': 200000, 'workers': 4},
        {'points': 200000, 'workers': 4},
        {'points': 200000, 'workers': 4},
        {'points': 200000, 'workers': 4},
        {'points': 200000, 'workers': 4},
        {'points': 200000, 'workers': 4},
        {'points': 200000, 'workers': 4},
        {'points': 400000, 'workers': 8},
        {'points': 400000, 'workers': 8},
        {'points': 400000, 'workers': 8},
        {'points': 400000, 'workers': 8},
        {'points': 400000, 'workers': 8},
        {'points': 400000, 'workers': 8},
        {'points': 400000, 'workers': 8},
        {'points': 400000, 'workers': 8},
        {'points': 400000, 'workers': 8},
        {'points': 400000, 'workers': 8},
        {'points': 800000, 'workers': 16},
        {'points': 800000, 'workers': 16},
        {'points': 800000, 'workers': 16},
        {'points': 800000, 'workers': 16},
        {'points': 800000, 'workers': 16},
        {'points': 800000, 'workers': 16},
        {'points': 800000, 'workers': 16},
        {'points': 800000, 'workers': 16},
        {'points': 800000, 'workers': 16},
        {'points': 800000, 'workers': 16},
        {'points': 500000, 'workers': 1},
        {'points': 500000, 'workers': 1},
        {'points': 500000, 'workers': 1},
        {'points': 500000, 'workers': 1},
        {'points': 500000, 'workers': 1},
        {'points': 500000, 'workers': 1},
        {'points': 500000, 'workers': 1},
        {'points': 500000, 'workers': 1},
        {'points': 500000, 'workers': 1},
        {'points': 500000, 'workers': 1},
        {'points': 1000000, 'workers': 2},
        {'points': 1000000, 'workers': 2},
        {'points': 1000000, 'workers': 2},
        {'points': 1000000, 'workers': 2},
        {'points': 1000000, 'workers': 2},
        {'points': 1000000, 'workers': 2},
        {'points': 1000000, 'workers': 2},
        {'points': 1000000, 'workers': 2},
        {'points': 1000000, 'workers': 2},
        {'points': 1000000, 'workers': 2},
        {'points': 2000000, 'workers': 4},
        {'points': 2000000, 'workers': 4},
        {'points': 2000000, 'workers': 4},
        {'points': 2000000, 'workers': 4},
        {'points': 2000000, 'workers': 4},
        {'points': 2000000, 'workers': 4},
        {'points': 2000000, 'workers': 4},
        {'points': 2000000, 'workers': 4},
        {'points': 2000000, 'workers': 4},
        {'points': 2000000, 'workers': 4},
        {'points': 4000000, 'workers': 8},
        {'points': 4000000, 'workers': 8},
        {'points': 4000000, 'workers': 8},
        {'points': 4000000, 'workers': 8},
        {'points': 4000000, 'workers': 8},
        {'points': 4000000, 'workers': 8},
        {'points': 4000000, 'workers': 8},
        {'points': 4000000, 'workers': 8},
        {'points': 4000000, 'workers': 8},
        {'points': 4000000, 'workers': 8},
        {'points': 8000000, 'workers': 16},
        {'points': 8000000, 'workers': 16},
        {'points': 8000000, 'workers': 16},
        {'points': 8000000, 'workers': 16},
        {'points': 8000000, 'workers': 16},
        {'points': 8000000, 'workers': 16},
        {'points': 8000000, 'workers': 16},
        {'points': 8000000, 'workers': 16},
        {'points': 8000000, 'workers': 16},
        {'points': 8000000, 'workers': 16},
        {'points': 5000000, 'workers': 1},
        {'points': 5000000, 'workers': 1},
        {'points': 5000000, 'workers': 1},
        {'points': 5000000, 'workers': 1},
        {'points': 5000000, 'workers': 1},
        {'points': 5000000, 'workers': 1},
        {'points': 5000000, 'workers': 1},
        {'points': 5000000, 'workers': 1},
        {'points': 5000000, 'workers': 1},
        {'points': 5000000, 'workers': 1},
        {'points': 10000000, 'workers': 2},
        {'points': 10000000, 'workers': 2},
        {'points': 10000000, 'workers': 2},
        {'points': 10000000, 'workers': 2},
        {'points': 10000000, 'workers': 2},
        {'points': 10000000, 'workers': 2},
        {'points': 10000000, 'workers': 2},
        {'points': 10000000, 'workers': 2},
        {'points': 10000000, 'workers': 2},
        {'points': 10000000, 'workers': 2},
        {'points': 20000000, 'workers': 4},
        {'points': 20000000, 'workers': 4},
        {'points': 20000000, 'workers': 4},
        {'points': 20000000, 'workers': 4},
        {'points': 20000000, 'workers': 4},
        {'points': 20000000, 'workers': 4},
        {'points': 20000000, 'workers': 4},
        {'points': 20000000, 'workers': 4},
        {'points': 20000000, 'workers': 4},
        {'points': 20000000, 'workers': 4},
        {'points': 40000000, 'workers': 8},
        {'points': 40000000, 'workers': 8},
        {'points': 40000000, 'workers': 8},
        {'points': 40000000, 'workers': 8},
        {'points': 40000000, 'workers': 8},
        {'points': 40000000, 'workers': 8},
        {'points': 40000000, 'workers': 8},
        {'points': 40000000, 'workers': 8},
        {'points': 40000000, 'workers': 8},
        {'points': 40000000, 'workers': 8},
        {'points': 80000000, 'workers': 16},
        {'points': 80000000, 'workers': 16},
        {'points': 80000000, 'workers': 16},
        {'points': 80000000, 'workers': 16},
        {'points': 80000000, 'workers': 16},
        {'points': 80000000, 'workers': 16},
        {'points': 80000000, 'workers': 16},
        {'points': 80000000, 'workers': 16},
        {'points': 80000000, 'workers': 16},
        {'points': 80000000, 'workers': 16}
    ]

    output_dir = '/home/moi/output'
    output_file_strong = os.path.join(output_dir, 'strong_scalability_results.csv')
    output_file_weak = os.path.join(output_dir, 'weak_scalability_results.csv')

    if rank == 0:
        logging.info("I am the master, coordinating the work.")
        #master(scalability_tests_strong, output_file_strong)
        master(scalability_tests_weak, output_file_weak)
    else:
        logging.info(f"I am worker {rank}, performing computations.")
        worker()
