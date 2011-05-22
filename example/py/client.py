#!/usr/bin/python

from gearman import libgearman

def main():
    client = libgearman.Client()
    client.add_server("127.0.0.1", 4730)
    r = client.do("ToUpper", "arbitrary binary data")
    print r

if __name__ == '__main__':
    main()

