const app = require('express')();
const server = require('http').createServer(app);
const options = { };
const io = require('socket.io')(server, options);

// const express = require('express')
var cors = require('cors');
const sql = require("mssql");
var bodyParser = require('body-parser');

// const io = require('socket.io')();

// const app = express()
const port = 7777

app.use(cors())
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({
    extended: true
}));

var config = {
    user: 'sa',
    password: 'Passw0rd!',
    server: 'HDESKNEW',
    database: 'DataBus',
    "options": {
        "encrypt": false,
        "enableArithAbort": true
    },
};

sql.connect(config, function (err) {
    if (err) {
        console.log(err);
    } else {
        io.on('connection', client => {
            console.log(client.client.conn.remoteAddress);
            io.emit('ping','pingiiiiiiing!');
            // setInterval(() => {client.broadcast.emit('connect','pingiiiiiiing!')},1000)
            // client.on('', (item) => { console.log(item); });
            client.on('ping', (item) => { console.log(item); });
        });

        app.get('/',(req,res) => {
            res.send('ok')
        })
    }
});

// server.listen(3000);

io.listen(7778,function(){
    console.log('Socket server running on 7778');
});

var server = app.listen(port, function () {
    console.log('Server is running on port: ', port);
});