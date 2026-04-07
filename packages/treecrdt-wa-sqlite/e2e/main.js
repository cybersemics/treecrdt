import { jsx as _jsx } from "react/jsx-runtime";
import React from 'react';
import { createRoot } from 'react-dom/client';
import App from './src/App.js';
import './src/bench';
import './src/conformance';
import './src/closed-client';
import './src/drop-opfs';
import './src/sync';
const container = document.getElementById('root');
if (!container) {
    throw new Error('Missing root element');
}
createRoot(container).render(_jsx(React.StrictMode, { children: _jsx(App, {}) }));
