// const io = require("socket.io-client");
const socket = require("socket.io-client")("http://localhost:3000");
// socket.connect()
socket.on('messageChannel',(data) => {console.log(data);})
socket.on('ping',(data) => {console.log(data);})
setInterval(()=> {
    socket.emit('messageChannel', { foo: "bar" })
},1000)
