import random

def read_input_files(fname):
    number_list = []
    with open(fname) as file:
        for line in file:
            number_list.append(int(line))
    return number_list

def bin_power(base, e, mod):
    result = 1
    base %= mod
    while e:
        if e & 1:
            result = result * base % mod
        base = base * base % mod
        e >>= 1
    return result

def check_composite(n, a, d, s):
    x = bin_power(a, d, n)
    if x == 1 or x == n - 1:
        return False
    for r in range(s): #r = 1; r < s; r++
        x = x * x % n
        if x == n - 1:
            return False
    return True

#Miller-Rabin implementation
def miller_rabin_probability(n, k):
    if n == 2 or n == 3:
        return True

    if n % 2 == 0:
        return False

    r, s = 0, n - 1
    while s % 2 == 0:
        r += 1
        s //= 2
    for _ in range(k):
        a = random.randrange(2, n - 1)
        x = pow(a, s, n)
        if x == 1 or x == n - 1:
            continue
        for _ in range(r - 1):
            x = pow(x, 2, n)
            if x == n - 1:
                break
        else:
            return False
    return True


def miller_rabin_deterministic(n):
    if n < 2: return False

    r = 0
    d = n-1
    while(d & 1) == 0:
        d = d >> 1
        r += 1

    bases = {2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37}
    for a in bases:
        if n == a:
            return True
        if check_composite(n, a, d, r):
            return False

    return True



#Testing entire sets of numbers
def test_prime_probability(number_list):
    probably_primes = []
    for i in number_list:
        if miller_rabin_probability(i, 40): probably_primes.append(i)

    return probably_primes.sort()

def test_prime_deterministic(number_list):
    definitely_primes = []
    for i in number_list:
        if miller_rabin_deterministic(i): definitely_primes.append(i)

    return definitely_primes.sort()