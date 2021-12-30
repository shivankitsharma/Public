def fizzbuzz_recursive(n):
    if n == 0:
        return
    fizzbuzz_recursive(n - 1)
    if n % 3 == 0 and n % 5 == 0:
        print('FizzBuzz')
    elif n % 3 == 0:
        print('Fizz')
    elif n % 5 == 0:
        print('Buzz')
    else:
        print(n)
        
fizzbuzz_recursive(300)
