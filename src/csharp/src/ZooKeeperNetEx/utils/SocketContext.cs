using System;
using System.Net;
using System.Net.Sockets;

namespace ZooKeeperNetEx.utils
{
    // https://github.com/dotnet/corefx/blob/master/src/System.Net.Sockets/src/System/Net/Sockets/Socket.Tasks.cs
    internal class SocketContext : IDisposable
    {
        private readonly SocketAsyncEventArgs _socketAsyncEventArgs;

        private readonly AwaitableSignal _awaitableSignal;
        private readonly Socket _socket;

        private readonly VolatileInt _state = new VolatileInt(IDLE);

        private readonly VolatileInt _disposed = new VolatileInt(0);

        private static readonly byte[] Buffer = new byte[0];

        private const int IDLE = 0;
        private const int PENDING = 1;
        public static SocketContext StartConnectAsync(AwaitableSignal awaitableSignal, Socket socket, EndPoint remoteEndPoint)
        {
            return new SocketContext(awaitableSignal, socket, remoteEndPoint);
        }

        private SocketContext(AwaitableSignal awaitableSignal, Socket socket, EndPoint remoteEndPoint)
        {
            _socketAsyncEventArgs = new SocketAsyncEventArgs();
            _socketAsyncEventArgs.SetBuffer(Buffer, 0, 0);
            _socketAsyncEventArgs.Completed += delegate { Complete(); };
            _socketAsyncEventArgs.RemoteEndPoint = remoteEndPoint;
            _awaitableSignal = awaitableSignal;
            _socket = socket;

            StartSocketAction(_socket.ConnectAsync);
        }
        
        public void StartReceiveAsync()
        {
            StartSocketAction(_socket.ReceiveAsync);
        }

        private void StartSocketAction(Func<SocketAsyncEventArgs, bool> socketAction)
        {
            _state.SetValue(IDLE, PENDING);
            if (socketAction(_socketAsyncEventArgs) == false)
            {
                Complete();
            }
        }

        private void Complete()
        {
            _state.SetValue(PENDING, IDLE);
            _awaitableSignal.TrySignal();
        }

        public EndPoint RemoteEndPoint
        {
            get
            {
                ThrowIfDisposed();
                return _socketAsyncEventArgs.RemoteEndPoint;
            }
        }
        
        public SocketAsyncOperation GetResult()
        {
            ThrowIfDisposed();

            if (_state.Value == PENDING) return SocketAsyncOperation.None;

            var socketAsyncEventArgs = _socketAsyncEventArgs; 

            if (socketAsyncEventArgs.LastOperation == SocketAsyncOperation.Receive && _socket.Available == 0 && _socket.Poll(1000, SelectMode.SelectRead))
            {
                socketAsyncEventArgs.SocketError = SocketError.ConnectionReset;
            }

            if (socketAsyncEventArgs.SocketError != SocketError.Success)
            {
                throw new SocketException((int) socketAsyncEventArgs.SocketError);
            }
            return socketAsyncEventArgs.LastOperation;
        }
      
        private void ThrowIfDisposed()
        {
            if (_disposed.Value == 1)
            {
                throw new ObjectDisposedException(nameof(SocketContext));
            }
        }

        public void Dispose()
        {
            if (_disposed.TrySetValue(0, 1))
            {
                _socketAsyncEventArgs?.Dispose();
            }
        }
    }
}
