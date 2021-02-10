const schedule = require('node-schedule');
var spawn = require('child_process').spawn;
const {Base64} = require('js-base64');

console.log(process.argv);

var args = process.argv.slice(2);

// Выполняемая программа python, node и т.д.
var exe = args[0];
// Путь к файлу
var loc = args[1];
// интервал запуска
var interval = args[2];

// Остальные аргументы
if(args.length > 3){
  var args = process.argv.slice(5);
  args.unshift(loc);
}
else {
  args = loc;
}

const job = schedule.scheduleJob(interval, function(){
  var process = spawn(exe, args);
  var log = ''
  var err = ''
  process.stdout.on('data', function (data) {
    log += decodeMsg(data) + "\n";
  });
  process.stderr.on('data', function (data) {
    err += decodeMsg(data) + "\n";
    // console.log('from scheduler err',decodeMsg(data));
  });
  process.on('close', function(data) {
    // console.log({  });
    if(err.length != 0 || data == 1)
      console.log({pid:process.pid,data:data,error:err,exit:data})
  })
});

function decodeMsg(data) {
  var d = ''
  for (i = 0; i < data.length; i++) {
      if (data[i] > 127) {
          d += String.fromCharCode(parseInt(data[i]) + 848);
      } else {
          d += String.fromCharCode(data[i])
      }
  }
  return d;
}