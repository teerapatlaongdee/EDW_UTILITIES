import time

def print_by_letter(text, delay):
    for char in text:
        print(char, end="", flush=True)
        time.sleep(delay)

# Call the function with "Hello, World!" and a delay of 0.5 seconds between each letter
print_by_letter("Every Document was Generated Successfully :)", 0.05)
