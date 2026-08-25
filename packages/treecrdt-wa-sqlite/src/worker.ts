/// <reference lib="webworker" />
import * as Comlink from 'comlink';
import { TreecrdtBackend } from './backend.js';
import { openTreecrdtDb } from './open.js';

const backend = new TreecrdtBackend(openTreecrdtDb);
Comlink.expose(backend);
