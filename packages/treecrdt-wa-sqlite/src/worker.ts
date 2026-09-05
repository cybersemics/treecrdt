/// <reference lib="webworker" />
import * as Comlink from 'comlink';
import createExclusiveConnection from './connection.js';
import createTreecrdtSession from './session.js';
import { openTreecrdtDb } from './open.js';

Comlink.expose(createExclusiveConnection(createTreecrdtSession(openTreecrdtDb)));
