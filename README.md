# TCP over UDP

## Problem Statement:
A UDP server is hosted by course coordinator on vayu.iitd.ac.in. Server might drop requests randomly.
### Checkpoints:
  1) Receive data reliably.
  2) Try utilizing the constant(from server side) bandwidth.
  3) Try utilizing the variable(from server side) bandwidth.

### Files: 
  - Checkpoint<check=num>.py holds our solution for the checkpoints with reports .pdf files.
  TCP.py contains both AIMD and AIAD implementation where mode = 0 for AIAD and mode = 1 for AIMD.

### Improvements:
  - Parallel execution in real scenario and better RTT evaluation methods and predicting model for wait time and window size.
