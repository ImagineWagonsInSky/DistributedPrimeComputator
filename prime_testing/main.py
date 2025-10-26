import prime_testing as pt
import logging
import time

def main():
    input_path  = r"\prime_input"
    output_path = r"\prime_output"

    test1 = 13515832593838893853 #is a prime
    test2 = 13515832593838893849 #not a prime
    k = 15

    start = time.time()
    print(pt.miller_rabin_probability(test1, k))
    print(pt.miller_rabin_probability(test2, k))
    end = time.time()
    print("Probabilistic version:", end - start)

    start = time.time()
    print(pt.miller_rabin_deterministic(test1))
    print(pt.miller_rabin_deterministic(test2))
    end = time.time()
    print("Deterministic version:", end - start)

if __name__ == "__main__":
    main()
