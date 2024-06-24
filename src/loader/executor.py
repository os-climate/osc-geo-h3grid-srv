# Copyright 2024 Broda Group Software Inc.
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.
#
# Created: 2024-03-08 by davis.broda@brodagroupsoftware.com
#####
# Simple multi-processor function
#####

import concurrent.futures
import logging

# Set up logging
LOGGING_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=LOGGING_FORMAT)
logger = logging.getLogger(__name__)

class Executor:
    def __init__(self, interpolate_function, max_processes):
        self.interpolate_function = interpolate_function
        self.max_processes = max_processes
        logger.info(f"Using interpolate_function:{interpolate_function} max_processes:{max_processes}")

    def process_data(self, data):
        with concurrent.futures.ProcessPoolExecutor(max_workers=self.max_processes) as executor:
            futures = [executor.submit(self.interpolate_function, **params) for params in data]

            results = []
            for future in concurrent.futures.as_completed(futures):
                try:
                    result = future.result()
                    if isinstance(result, list):
                        # Extend the main list with the elements of the returned list
                        results.extend(result)
                    else:
                        # Append non-list results directly
                        results.append(result)
                except Exception as e:
                    print(f"Generated an exception: {e}")

            return results

# Example of an interpolation function
def my_interpolate(**kwargs):
    print(f"kwargs:{kwargs}")
    try:
        # Implement your interpolation logic here
        # Example calculation (modify according to your logic)
        result = sum(kwargs.values())
        return result
    except Exception as e:
        # Return error information, or handle it as needed
        return {'error': str(e)}


# Example usage
if __name__ == "__main__":

    num_entries = 10  # Number of data entries
    num_params = 20   # Number of parameters in each entry
    data = []
    for i in range(num_entries):
        entry = {f'param{j}': j + i for j in range(1, num_params + 1)}
        print(f"entry: {entry}")
        data.append(entry)

    max_processes = 4  # Set the number of processes
    interpolator = Executor(my_interpolate, max_processes)
    results = interpolator.process_data(data)

    print(f"results:{results}")
