const app = require('express')();
const server = require('http').createServer(app);
const options = {};
const io = require('socket.io')(server, options);
var multer  = require('multer')
var path = require('path')
fs = require('fs')

var storage = multer.diskStorage({
    destination: function (req, file, cb) {
      cb(null, __dirname + '/scripts')
    },
    filename: function (req, file, cb) {
      const uniqueSuffix = Date.now() + '-' + Math.round(Math.random() * 1E9) + path.extname(file.originalname)
      cb(null, uniqueSuffix)
    }
  })
  
  var upload = multer({ storage: storage })

var cors = require('cors');
const sql = require("mssql");
var bodyParser = require('body-parser');
var spawn = require('child_process').spawn;


const {
    Base64
} = require('js-base64');

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
    db.query('select a.*,b.name as emitname,guid,c.id as scheduleid,schedule,c.status as scheduleStatus from rules a left join emitters b on a.id = b.ruleid left join jobs c on a.id = c.ruleid', (err, result) => {
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

app.post('/addRule',upload.single('file'),(req,res) => {
    var data = JSON.parse(req.body.data);
    db.query(`insert into rules(name,type,command,filename,content) values (N'` + data.name + `',${data.type},N'`+data.program+`',N'`+ req.file.filename +`',N'`+ JSON.stringify(data.args) +`')`,(err,result) => {
        if(err) throw err;
        else{
            console.log(req.file.filename);
            res.send('ok')
        }
    })
})

app.post('/addEmitter', (req, res) => {
    db.query(`insert into emitters(name,ruleid) OUTPUT Inserted.guid values(N'` + req.body.emitname + `',${req.body.ruleid})`, (err, result) => {
        if (err) console.log(err);
        else {
            res.send(result.recordset[0]);
        }
    })
})

app.get('/services', (req,res) => {
    db.query(`select * from services`,(err,result) => {
        if(err) throw err;
        else {
            res.send(result.recordset)
        }
    })
});

app.get('/getRuleExecutable', (req,res) => {
    db.query(`select filename from rules where id = ${req.query.id}`,(err,result) => {
        if(err) throw err;
        else {
            fs.readFile('scripts/' + result.recordset[0].filename, 'utf8', function (err,data) {
                if (err) {
                  return console.log(err);
                }
                res.send(Base64.encode(data))
              });

        }
    })
})

app.post('/addSchedule',(req,res) => {
    if(typeof req.body.ruleid != 'undefined' && !req.body.id){
        db.query(`insert into jobs(ruleid,schedule) OUTPUT inserted.ID values(${req.body.ruleid},N'` + req.body.schedule + `')`, (err,result) => {
            if(err) throw err;
            else {
                // console.log(result.recordset[0].ID);
                req.body.id = result.recordset[0].ID
                runSchedule(req,res)
                // res.send('ok')
            }
        })
    } else if(typeof req.body.ruleid != 'undefined' && req.body.id) {
        db.query(`update jobs set schedule = N'` + req.body.schedule + `',status=${req.body.status} OUTPUT INSERTED.pid where id = ${req.body.id}`, (err,result) => {
            if(err) throw err;
            else {
                if(req.body.status == 1)
                    runSchedule(req,res)
                else {
                    try {
                        process.kill(result.recordset[0].pid)
                    }
                    catch{

                    }
                    finally{
                        res.send('ok')
                    }
                }
            }
        })
    }
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


app.post('/scheduler',runChildProcess)
app.post('/killScheduler',killChildProcess)


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
                    var args = JSON.parse(result.recordset[0].content);
                    args.unshift('scripts/' + result.recordset[0].filename);
                    var process = spawn(result.recordset[0].command, args);
                }

                if (process) {
                    db.query("insert into rulesLog(ruleid,request,pid) OUTPUT Inserted.ID values(" + req.body.ruleid + ",N'" + result.recordset[0].content + "',"+ process.pid +")", (err, result) => {
                        
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

function runSchedule(req, res) {
    if (req.body.id && req.body.id.length != 0) {
        db.query('select a.*,b.schedule from rules a left join jobs b on a.id = b.ruleid where b.status = 1 and a.id=' + req.body.ruleid, (err, result) => {
            if (err) console.log(err);
            else {
                    var process = spawn('node',  ["scheduler.js",result.recordset[0].command,'scripts/' + result.recordset[0].filename,Base64.decode(result.recordset[0].schedule),result.recordset[0].content]);

                if (process) {
                    db.query(`update jobs set pid=${process.pid} where id=${req.body.id}`, (err) => {
                            
                        db.query("insert into rulesLog(ruleid,request,pid) OUTPUT Inserted.ID values(" + req.body.ruleid + ",N'" + result.recordset[0].content + "',"+ process.pid +")", (err, result) => {
                        

                            process.stdout.on('data', function (data) {
                                db.query("update rulesLog set outTxt=N'" + decodeMsg(data) + "' where id=" + result.recordset[0].ID, (err, result) => {
                                    if (err) throw err;
                                    else
                                    console.log(decodeMsg(data));
                                        finishJob(req.body.id, decodeMsg(data))
                                })
                            })

                            process.stderr.on('data', function (data) {
                                db.query("update rulesLog set errTxt=N'" + decodeMsg(data) + "' where id=" + result.recordset[0].ID, (err, result) => {
                                    if (err) throw err;
                                    else
                                    console.log(decodeMsg(data));
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
                    })
                }
            }
        })

    } else {
        res.send('nok')
    }
}

function runChildProcess(req, res){
    var spawn = require('child_process').spawn;
    var process = spawn('node', ['scheduler.js']);
    
    if(process) {
        db.query(`update services set pid=${process.pid}`,(err,result) => {
            if(err) throw err;
            else {
                process.stdout.on('data', function (data) {
                    console.log('data ',process.pid, data);
                })
                process.stderr.on('data', function (data) {
                    console.log('err ',process.pid, data);
                })
                process.on('close', (code) => {
                    console.log('exit ',process.pid, code);
                })
            
                res.send({status:'ok',pid:process.pid})
            }
        })
    }

}

function killChildProcess(req,res) {
    var spawn = require('child_process').spawn;
    var process = spawn('taskkill',['/F','/PID',req.body.pid]);

    if(process) {
        db.query(`update services set pid=NULL where pid=${req.body.pid}`,(err,result) => {
            if(err) throw err;
            else {
                process.stdout.on('data', function (data) {
                    console.log('data ',process.pid, data);
                })
                process.stderr.on('data', function (data) {
                    console.log('err ',process.pid, data);
                })
                process.on('close', (code) => {
                    console.log('exit ',process.pid, code);
                })
            
                res.send({status:'ok',pid:req.body.pid})
            }
        })
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