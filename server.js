const app = require('express')();
const server = require('http').createServer(app);
const options = {};
const io = require('socket.io')(server, options);


var cors = require('cors');
const sql = require("mssql");
var bodyParser = require('body-parser');


const {
    Base64
} = require('js-base64');
var windows1251 = require('windows-1251');

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


sql.connect(config);
const db = new sql.Request();

app.get('/rules', (req, res) => {
    db.query('select a.*,b.name as emitname,guid from rules a left join emitters b on a.id = b.ruleid', (err, result) => {
        if (err) console.log(err);
        else {
            res.send(result.recordset);
        }
    })
});

app.get('/logs', (req, res) => {
    db.query('select top 100 a.*,DATEDIFF(second,a.requestat,a.responseat) as elapsed from rulesLog a where ruleid=' + req.query.id + ' order by id desc ', (err, result) => {
        if (err) console.log(err);
        else {
            res.send(result.recordset);
        }
    })
});

app.get('/emit/get/:guid/', (req, res) => {
    // res.send(req.query);
    db.query(`select * from emitters where guid = '` + req.params.guid + `'`, (err, result) => {
        if (err) res.send('nok');
        else {
            if (result.recordset.length != 0) {
                req.body['ruleid'] = result.recordset[0].ruleid
                req.body['id'] = result.recordset[0].id
                req.body['source'] = req.query.type
                if (req.query.type == "1") {
                    req.body.params = req.query.params
                    
                }
                runJob(req, res);
            } else {
                res.send(`invalid token`)
            }
        }
    })
});

app.post('/addEmitter', (req, res) => {
    db.query(`insert into emitters(name,ruleid) OUTPUT Inserted.guid values(N'` + req.body.emitname + `',${req.body.ruleid})`, (err, result) => {
        if (err) console.log(err);
        else {
            res.send(result.recordset[0]);
        }
    })
})

io.on('connection', socket => {
    socket.join(socket.id)

    socket.on('id', (item) => {
        io.to(socket.id).emit('id', socket.id)
    })

    socket.on('client', (item) => {
        console.log(item);
        io.to(socket.id).emit('serverMessage', 'you are connected!')
    })

    socket.on("ping", (item) => {
        console.log('ping ', item);
        io.to(socket.id).emit('ping', 'ok')
    })



    app.post('/run', runJob);

});




io.listen(7778, function () {
    console.log('Socket server running on 7778');
});

var srv = app.listen(7777, function () {
    console.log('Server is running on port: 7777');
});


function runJob(req, res) {
    if (req.body.id && req.body.id.length != 0) {
        var spawn = require('child_process').spawn;

        db.query('select * from rules where id=' + req.body.ruleid, (err, result) => {
            if (err) console.log(err);
            else {
                if (req.body.source == "1") {
                    var process = spawn(result.recordset[0].command, JSON.parse(result.recordset[0].content).concat(req.body.params));
                } else {
                    var process = spawn(result.recordset[0].command, JSON.parse(result.recordset[0].content));
                }

                if (process) {
                    db.query("insert into rulesLog(ruleid,request) OUTPUT Inserted.ID values(" + req.body.ruleid + ",N'" + result.recordset[0].content + "')", (err, result) => {
                        
                        process.stdout.on('data', function (data) {
                            db.query("update rulesLog set outTxt=N'" + decodeMsg(data) + "' where id=" + result.recordset[0].ID, (err, result) => {
                                if (err) throw err;
                                else
                                    finishJob(req.body.id, decodeMsg(data))
                            })
                        })

                        process.stderr.on('data', function (data) {
                            db.query("update rulesLog set errTxt=N'" + decodeMsg(data) + "' where id=" + result.recordset[0].ID, (err, result) => {
                                if (err) throw err;
                                else
                                    finishJob(req.body.id, decodeMsg(data))
                            })
                        })

                        process.on('close', (code) => {
                            db.query("update rulesLog set exitCode=" + code + ",responseat=GETDATE() where id=" + result.recordset[0].ID, (err, result) => {
                                console.log('close with code: ', code);
                            })
                        })

                        res.send('ok')
                    })
                }
            }
        })

    } else {
        res.send('nok')
    }
}

function finishJob(sender, data) {
    io.to(sender).emit('message', data)
}

function decodeMsg(data) {
    var d = ''
    for (i = 0; i < data.length; i++) {
        if (data[i] > 127) {
            d += String.fromCharCode(parseInt(data[i]) + 848);
        } else {
            d += String.fromCharCode(data[i])
        }
    }
    return Base64.encode(d);
}