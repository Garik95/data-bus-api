const app = require('express')();
const server = require('http').createServer(app);
const io = require('socket.io')(server);
const { connect } = require('socket.io-client');

io.on('connection', socket => {
    console.log(socket);
})
server.listen(9000, () => {
    console.log('listening');
});