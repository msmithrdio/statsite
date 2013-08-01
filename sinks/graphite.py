"""
Supports flushing metrics to graphite
"""
import sys
import socket
import logging
import time

class GraphiteStoreStats(object):
  def __init__(self):
    self._timers = {}
    self._stats = {}

  def get_stats(self):
    return self._stats

  def clear_stats(self):
    self._stats = {}

  def start_timer(self, timer):
    self._timers[timer] = time.time()
    
  def stop_timer(self, timer):
    if timer in self._timers:
      self._stats[timer] = time.time() - self._timers[timer] 

  def add_stat(self, stat, value):
    self._stats[stat] = value

class GraphiteStore(object):
    def __init__(self, host="localhost", port=2003, prefix="statsite", attempts=3, flush_interval=10000):
        """
        Implements an interface that allows metrics to be persisted to Graphite.
        Raises a :class:`ValueError` on bad arguments.

        :Parameters:
            - `host` : The hostname of the graphite server.
            - `port` : The port of the graphite server
            - `prefix` (optional) : A prefix to add to the keys. Defaults to 'statsite'
            - `attempts` (optional) : The number of re-connect retries before failing.
        """
        # Convert the port to an int since its coming from a configuration file
        port = int(port)
        attempts = int(attempts)
        flush_interval = int(flush_interval)

        if port <= 0:
            raise ValueError("Port must be positive!")
        if attempts <= 1:
            raise ValueError("Must have at least 1 attempt!")

        self.host = host
        self.port = port
        self.prefix = prefix
        self.attempts = attempts
        self.flush_interval = flush_interval
        self.sock = self._create_socket()
        self.logger = logging.getLogger("statsite.graphitestore")
        self.stats = GraphiteStoreStats()

    def _process_datapoint(self, k, v):
        """Post-process datapoints"""
        # Convert raw counters into per-second counters; it's more useful!
        if k.endswith(".count"):
          return float(v) / (self.flush_interval / 1000)
        return v

    def flush(self, metrics):
        """
        Flushes the metrics provided to Graphite.

       :Parameters:
        - `metrics` : A list of (key,value,timestamp) tuples.
        """
        self.stats.add_stat("num_stats", len(metrics),)
        # Construct the output
        self.stats.start_timer("format_time")
        metrics = [m.split("|") for m in metrics if m]
        self.logger.info("Outputting %d metrics" % len(metrics))
        if not self.prefix:
            lines = ["%s %s %s" % (k, self._process_datapoint(k,v), ts) for k, v, ts in metrics]
        else:
            lines = ["%s.%s %s %s" % (self.prefix, k, self._process_datapoint(k,v), ts) for k, v, ts in metrics]
        data = "\n".join(lines) + "\n"
        self.stats.stop_timer("format_time")

        # Serialize writes to the socket
        self.stats.start_timer("send_time")
        try:
            self._write_metric(data)
        except:
            self.logger.exception("Failed to write out the metrics!")
        self.stats.stop_timer("send_time")

        ts = int(time.time())
        stats = self.stats.get_stats()
        lines = ["%s %s %d" % ("stats.statsite."+stat, stats[stat], ts) for stat in stats]
        data = "\n".join(lines) + "\n"
        try:
            self._write_metric(data)
        except:
            self.logger.exception("Failed to write out the stats about the metrics!")
        self.stats.clear_stats()

    def close(self):
        """
        Closes the connection. The socket will be recreated on the next
        flush.
        """
        self.sock.close()

    def _create_socket(self):
        """Creates a socket and connects to the graphite server"""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((self.host, self.port))
        return sock

    def _write_metric(self, metric):
        """Tries to write a string to the socket, reconnecting on any errors"""
        for attempt in xrange(self.attempts):
            try:
                self.sock.sendall(metric)
                return
            except socket.error:
                self.logger.exception("Error while flushing to graphite. Reattempting...")
                self.sock = self._create_socket()

        self.logger.critical("Failed to flush to Graphite! Gave up after %d attempts." % self.attempts)


if __name__ == "__main__":
    # Initialize the logger
    logging.basicConfig()

    # Intialize from our arguments
    graphite = GraphiteStore(*sys.argv[1:])

    # Get all the inputs
    graphite.stats.start_timer("read_time")
    metrics = sys.stdin.read()
    graphite.stats.stop_timer("read_time")

    # Flush
    graphite.flush(metrics.split("\n"))
    graphite.close()
