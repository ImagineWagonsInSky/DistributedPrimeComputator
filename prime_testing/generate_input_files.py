import os
import numpy as np
import time

def generate64bit(file_amount, number_amount):
    dir_path = os.path.dirname(os.path.realpath(__file__)) + r"\prime_input"
    number_of_existing_files = len( [entry for entry in os.listdir(dir_path) if os.path.isfile(os.path.join(dir_path, entry))] )
    n = number_of_existing_files
    print("Generating 64 bit numbers")
    for i in range(file_amount):
        generated_numbers = np.random.randint(9223372036854775807,18446744073709551615, number_amount, dtype=np.uint64)

        n += 1
        fname = f"input_dataset_{n}.txt"
        file = open(os.path.join(dir_path, fname), "a")

        for i in generated_numbers:
            file.write(str(i) + "\n")

        file.close()


amount_files = 1
amount_nums  = 1000000
start = time.time()
generate64bit(amount_files, amount_nums)
end = time.time()
print(f"Generating {amount_files} files, each containing {amount_nums} 64-bit unsigned integers:", end - start)
