import socket
import struct
import time
import random

HOST ="127.0.0.1"
PORT = 9000
NUM_SAMPLES = 4

def build_packet():
  timestamp_ns = time.time_ns()
  voltages = [random.uniform(-5.0, 5.0) for _ in range(NUM_SAMPLES)]
  header = struct.pack(">QI", timestamp_ns, NUM_SAMPLES)
  payload = struct.pack(f">{NUM_SAMPLES}f", *voltages)
  return header + payload

def main():
  num_packets = 2000
  with socket.create_connection((HOST, PORT)) as sock:
    for _ in range(num_packets):
      packet = build_packet()
      sock.sendall(packet)
    print(f"Sent {num_packets} packets")


if __name__ == "__main__":
  main()

