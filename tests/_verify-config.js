#!/usr/bin/env node
const http = require('http');
http.get('http://localhost:3000/api/config', res => {
  let d = '';
  res.on('data', c => d += c);
  res.on('end', () => {
    const c = JSON.parse(d);
    const working = Object.values(c.states).filter(s => s.working);
    const nonw = Object.values(c.states).filter(s => !s.working);
    console.log('Total states in API:', Object.keys(c.states).length);
    console.log('Working:', working.length);
    console.log('Non-working:', nonw.length);
    console.log('Working list:', working.map(s => s.stateCode).sort().join(', '));
  });
}).on('error', e => console.error('Server not ready:', e.message));
