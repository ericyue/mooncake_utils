import os,sys

def process(line):
  print(line)

if __name__ == "__main__":
    while True:
        try:
            line = raw_input().strip()
            process(line)
        except EOFError:
            break
