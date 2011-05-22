#!/usr/bin/env python

import os, sys
from curses.wrapper import wrapper
from screen import Screen
from logger import flush

def main(s):
    f = file(sys.argv[1], 'r')
    screen = Screen(s)
    screen.append(f.readlines())
    f.close()
    screen.run(screen)

if __name__ == '__main__':
    try:
        wrapper(main)
    finally:
        flush()
