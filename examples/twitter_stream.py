"""Stream Tweets to a socket."""
import socket
import yaml
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener


class StdOutListener(StreamListener):
    """This listener prints data to stdout."""
    def on_data(self, data):
        """Actions when data is received."""
        print(data)

    def on_error(self, status):
        """Actions when an error occurs."""
        print(status)


class SocketListener(StreamListener):
    """This listener sends data to the specified socket.

    Parameters
    ----------
    host : str
        The host ip for the socket connection
    port : int
        The port for the socket connection
    
    Attributes
    ----------
    host : str
        The host ip for the socket connection
    port : int
        The port for the socket connection
    client_socket : socket
        The socket to send data to
    connection : connection
        The connection to the client
    """
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.client_socket = socket.socket()
        self.connection = self.connect()

    def connect(self):
        """Bind to the socket and connect to the client."""
        self.client_socket.bind((self.host, self.port))  # Bind to port
        print("Listening on port {}".format(self.port))
        self.client_socket.listen(5)  # Wait for client connection
        connection, address = self.client_socket.accept()  # Establish client connection
        print("Received request from {}".format(address))

        return connection

    def on_data(self, data):
        """Actions when data is received."""
        self.connection.send(data.encode('utf-8'))

    def on_error(self, status):
        """Actions when an error occurs."""
        print(status)


class TwitterStream:
    """This defines a Twitter stream with the given listener and topic.
    
    Parameters
    ----------
    config : dict
        A YAML config file that specifies credentials and topics for Twitter
    listener : subclass of StreamListener from tweepy
        The listener tells the Twitter stream what to do with the data as it 
        streams in.

    Attributes
    ----------
    auth : type OAuthHandler from tweepy
        This creates the authorization for the Twitter app
    stream : type Stream from tweepy
        This is the initialized twitter stream.
    """
    def __init__(self, config, listener=StdOutListener()):
        self.config = config
        self.listener = listener

        # Complete authorization
        self.auth = OAuthHandler(self.config['consumer.key'], self.config['consumer.secret'])
        self.auth.set_access_token(self.config['access.token'], self.config['access.token.secret'])

        # Create stream
        self.stream = Stream(self.auth, self.listener, async=True)

    def start_twitter_stream(self):
        """Start Twitter pull on specified topic(s)."""
        self.stream.filter(track=self.config['twitter.topic'])


if __name__ == "__main__":
    # Load configs from yaml
    twitter_config = yaml.load(open('config.yaml'))

    # Create a Twitter stream with listener
    socket_listener = SocketListener('localhost', 5555)
    twitter_stream = TwitterStream(config=twitter_config, listener=socket_listener)
    twitter_stream.start_twitter_stream()
