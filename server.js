import express from 'express';
import { createServer } from 'http';
import { WebSocketServer } from 'ws';
import crypto from 'crypto';
import fs from 'fs';
import path from 'path';

const app = express();
const server = createServer(app);
const wss = new WebSocketServer({ server });
const rooms = new Map();
const PORT = process.env.PORT || 10000;
const client = fs.readFileSync(path.join(process.cwd(), 'public', 'index.html'), 'utf8');

app.get('/', (_, res) => res.type('html').send(client));
app.get('/health', (_, res) => res.json({ ok: true, rooms: rooms.size }));

const newId = () => crypto.randomBytes(4).toString('hex');
function deck(){ const a=[]; for(let n=1;n<=104;n++) a.push(n); for(let i=a.length-1;i>0;i--){const j=Math.floor(Math.random()*(i+1));[a[i],a[j]]=[a[j],a[i]];} return a; }
function points(n){ return n===55?7:n%11===0?5:n%10===0?3:n%5===0?2:1; }
function publicState(r){return {code:r.code,host:r.host,phase:r.phase,players:[...r.players.values()].map(p=>({id:p.id,name:p.name,score:p.score,hand:p.hand,submitted:p.submitted,host:p.id===r.host})),rows:r.rows,message:r.message,alert:r.alert,pending:r.pending};}
function broadcast(r){const msg=JSON.stringify({type:'state',state:publicState(r)});for(const p of r.players.values())if(p.ws.readyState===1)p.ws.send(msg);}
function start(r){
 r.deck=deck(); r.rows=[r.deck.splice(0,1),r.deck.splice(0,1),r.deck.splice(0,1),r.deck.splice(0,1)];
 for(const p of r.players.values()){p.hand=r.deck.splice(0,10).sort((a,b)=>a-b);p.score=0;p.submitted=null;}
 r.phase='playing';r.pending=null;r.resolution=[];r.message='Chọn 1 lá. Khi tất cả đã chọn, các lá được lật và xử lý từ nhỏ đến lớn.';r.alert=false;
}
function nextRound(r){
 r.pending=null;r.resolution=[];for(const p of r.players.values())p.submitted=null;
 if(r.deck.length<r.players.size){r.phase='finished';r.message='Hết bài. Ván kết thúc! Người có ít điểm nhất thắng.';r.alert=false;broadcast(r);return;}
 for(const p of r.players.values())p.hand.push(r.deck.shift());
 for(const p of r.players.values())p.hand.sort((a,b)=>a-b);
 r.message='Chọn 1 lá.';r.alert=false;broadcast(r);
}
function eligibleRows(r,card){const out=[];for(let i=0;i<4;i++){const last=r.rows[i][r.rows[i].length-1];if(last<card)out.push(i);}return out;}
function beginResolution(r){r.resolution=[...r.players.values()].map(p=>({pid:p.id,card:p.submitted})).sort((a,b)=>a.card-b.card);processNext(r);}
function processNext(r){
 if(r.pending)return;
 if(!r.resolution.length){nextRound(r);return;}
 const x=r.resolution.shift();const p=r.players.get(x.pid);if(!p)return processNext(r);const card=x.card;
 const choices=eligibleRows(r,card);
 if(choices.length===0){r.pending={type:'forced',player:p.id,card,choices:[0,1,2,3]};r.message=p.name+' đánh '+card+' — lá này nhỏ hơn tất cả 4 hàng. Hãy chọn hàng để THU BÀI.';r.alert=true;broadcast(r);return;}
 const row=choices.reduce((best,i)=>r.rows[i][r.rows[i].length-1]>r.rows[best][r.rows[best].length-1]?i:best,choices[0]);
 if(r.rows[row].length===5){r.pending={type:'sixth',player:p.id,card,row,choices:[row]};r.message=p.name+' đánh '+card+' → LÁ THỨ 6 của Hàng '+(row+1)+'! Nhấn THU BÀI.';r.alert=true;broadcast(r);return;}
 p.hand.splice(p.hand.indexOf(card),1);r.rows[row].push(card);r.message=p.name+' đánh '+card+' → Hàng '+(row+1)+'.';r.alert=false;broadcast(r);setTimeout(()=>processNext(r),450);
}
function collect(r,p,row){
 if(!r.pending||r.pending.player!==p.id)return false;
 if(r.pending.type==='sixth'&&row!==r.pending.row)return false;
 if(r.pending.type==='forced'&&(row<0||row>3))return false;
 const card=r.pending.card;const taken=r.rows[row].splice(0);p.hand.splice(p.hand.indexOf(card),1);p.score+=taken.reduce((s,n)=>s+points(n),0);r.rows[row].push(card);
 r.message=p.name+' thu Hàng '+(row+1)+': '+taken.join(' · ')+' = '+taken.reduce((s,n)=>s+points(n),0)+' 🐮. Lá '+card+' là lá đầu tiên của hàng.';
 r.alert=true;r.pending=null;broadcast(r);setTimeout(()=>processNext(r),450);return true;
}

wss.on('connection',ws=>{
 let room=null, player=null;
 ws.on('message',raw=>{
  let m;try{m=JSON.parse(raw)}catch{return;}
  if(m.type==='create'||m.type==='join'){
   if(room)return;
   if(m.type==='create'){
    let code;do{code=newId().slice(0,6).toUpperCase();}while(rooms.has(code));
    room={code,host:null,players:new Map(),phase:'lobby',rows:[],deck:[],resolution:[],pending:null,message:'',alert:false};rooms.set(code,room);
   } else {
    room=rooms.get(String(m.code||'').toUpperCase());
    if(!room)return ws.send(JSON.stringify({type:'error',message:'Không tìm thấy phòng.'}));
    if(room.phase!=='lobby')return ws.send(JSON.stringify({type:'error',message:'Ván đã bắt đầu, không thể vào.'}));
   }
   if(room.players.size>=10)return ws.send(JSON.stringify({type:'error',message:'Phòng đã đủ 10 người.'}));
   player={id:newId(),name:String(m.name||'Người chơi').slice(0,18),score:0,hand:[],submitted:null,ws};room.players.set(player.id,player);if(!room.host)room.host=player.id;
   ws.send(JSON.stringify({type:'hello',id:player.id}));broadcast(room);return;
  }
  if(!room||!player)return;
  if(m.type==='start'&&room.host===player.id&&room.players.size>=2&&room.phase==='lobby'){start(room);broadcast(room);return;}
  if(m.type==='play'&&room.phase==='playing'&&room.pending===null&&player.submitted===null){
   const card=Number(m.card);if(!Number.isInteger(card)||!player.hand.includes(card))return;
   player.submitted=card;const all=[...room.players.values()].every(p=>p.submitted!==null);room.message=all?'Đang xử lý…':'Đã chọn. Chờ người chơi còn lại…';broadcast(room);if(all)setTimeout(()=>beginResolution(room),300);return;
  }
  if(m.type==='collect'&&room.phase==='playing'){const row=Number(m.row);if(collect(room,player,row))return;}
  if(m.type==='restart'&&room.phase==='finished'&&room.host===player.id){start(room);broadcast(room);return;}
 });
 ws.on('close',()=>{if(!room||!player)return;room.players.delete(player.id);if(room.host===player.id)room.host=room.players.keys().next().value;if(room.players.size===0){rooms.delete(room.code);return;}if(room.pending?.player===player.id)room.pending=null;broadcast(room);});
});

server.listen(PORT,()=>console.log('6-nimmt online listening on '+PORT));
