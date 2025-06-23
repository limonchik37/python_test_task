# Middleware Message Routing

This repository contains two implementations of a multithreaded **middleware system** that routes messages between multiple simulated clients and a server.

## Files

- `middleware_mainloop.py`  
  A procedural version using a single `main_loop` function.  
  Suitable for understanding the basic logic of message passing and tag-based routing.

- `middleware_class.py`  
  An object-oriented version based on a `Middleware` class.  
  Includes:
  - Cleaner modular design
  - Built-in debug logging via `--debug` CLI flag
  - Easier to maintain and extend
