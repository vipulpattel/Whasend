const { app, BrowserWindow, ipcMain } = require("electron");
const { shell, dialog } = require('electron');
const path = require("path");
const fs = require("fs");
const { Client, LocalAuth, MessageMedia } = require('whatsapp-web.js');
const QRCode = require("qrcode");
const XLSX = require('xlsx');
const Database = require("better-sqlite3");
const dayjs = require('dayjs');
const customParseFormat = require('dayjs/plugin/customParseFormat');
dayjs.extend(customParseFormat); // ⬅️ Activate the plugin
const si = require("systeminformation");
const axios = require("axios");
// Auto-update (electron-updater)
let autoUpdater;
try {
  const { autoUpdater: _autoUpdater } = require('electron-updater');
  autoUpdater = _autoUpdater;
} catch (e) {
  // electron-updater not installed or not available in dev environment
  autoUpdater = null;
}
const log = require('electron-log');

// Handle unhandled promise rejections to prevent app crashes
process.on('unhandledRejection', (reason, promise) => {
  console.warn('⚠️ Unhandled Promise Rejection:', reason);
  // Don't exit the process, just log the error
});

// Handle uncaught exceptions
process.on('uncaughtException', (error) => {
  console.error('❌ Uncaught Exception:', error);
  // Don't exit the process for minor errors
});

// Helper function to get local system time in ISO format
function getLocalISOString() {
  const now = new Date();
  const offset = now.getTimezoneOffset() * 60000; // offset in milliseconds
  const localTime = new Date(now.getTime() - offset);
  return localTime.toISOString();
}

// Internet connectivity check function
async function checkInternetConnectivity() {
  try {
    //console.log('🔍 Checking internet connectivity...');
    
    // Try multiple endpoints for reliability
    const endpoints = [
      'https://www.google.com',
      'https://8.8.8.8', // Google DNS
      'https://1.1.1.1' // Cloudflare DNS
    ];
    
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 8000); // 8 second timeout
    
    try {
      // Try the first endpoint
      const response = await fetch(endpoints[0], {
        method: 'HEAD',
        mode: 'no-cors',
        signal: controller.signal,
        cache: 'no-cache'
      });
      
      clearTimeout(timeoutId);
      // console.log('✅ Internet connectivity confirmed');
      return { connected: true, message: 'Internet connection available' };
      
    } catch (firstError) {
      //console.log('⚠️ First endpoint failed, trying backup...');
      
      // Try axios as backup method
      try {
        await axios.get('https://www.google.com', { 
          timeout: 5000,
          headers: { 'Cache-Control': 'no-cache' }
        });
        clearTimeout(timeoutId);
       // console.log('✅ Internet connectivity confirmed (backup method)');
        return { connected: true, message: 'Internet connection available' };
      } catch (backupError) {
        clearTimeout(timeoutId);
        console.log('❌ Internet connectivity check failed');
        return { 
          connected: false, 
          message: 'No internet connection detected. Please check your network connection and try again.' 
        };
      }
    }
    
  } catch (error) {
    console.log('❌ Internet connectivity check error:', error.message);
    return { 
      connected: false, 
      message: `Internet connectivity check failed: ${error.message}` 
    };
  }
}

// Periodic connectivity monitoring
let connectivityInterval = null;
let lastConnectivityState = true;

function startPeriodicConnectivityCheck() {
  // Check connectivity every 2 minutes
  connectivityInterval = setInterval(async () => {
    const result = await checkInternetConnectivity();
    
    // Only alert on state changes to avoid spam
    if (result.connected !== lastConnectivityState) {
      lastConnectivityState = result.connected;
      
      if (!result.connected) {
        console.warn('🌐 ❌ Internet connection lost!');
        // Send alert to all windows
        if (mainWin) {
          mainWin.webContents.send('connectivity-alert', {
            type: 'lost',
            message: 'Internet connection lost. Message sending may be affected.'
          });
        }
      } else {
        console.log('🌐 ✅ Internet connection restored!');
        // Send alert to all windows
        if (mainWin) {
          mainWin.webContents.send('connectivity-alert', {
            type: 'restored',
            message: 'Internet connection restored.'
          });
        }
      }
    }
  }, 120000); // Check every 2 minutes
}

function stopPeriodicConnectivityCheck() {
  if (connectivityInterval) {
    clearInterval(connectivityInterval);
    connectivityInterval = null;
  }
}

// =============================================================================
// ANTI-DETECTION & SAFETY HELPER FUNCTIONS
// =============================================================================

// Human-like behavior simulation for anti-detection (optimized for speed)
function getHumanLikeDelay(baseDelay = 2000, variance = 0.3) {
  const variationRange = baseDelay * variance;
  const minDelay = baseDelay - variationRange;
  const maxDelay = baseDelay + variationRange;
  const calculatedDelay = Math.floor(Math.random() * (maxDelay - minDelay + 1)) + minDelay;
  
  // Ensure minimum 8 seconds (8000ms) but cap maximum at 12 seconds for speed
  return Math.max(calculatedDelay, 2000);
}

// Adaptive rate limiting based on time patterns
function getAdaptiveDelay(profileName, messageCount) {
  const now = new Date();
  const hour = now.getHours();
  
  // Base delays with human patterns - minimum 8 seconds
  let baseDelay = 5000; // 8 seconds base minimum
  
  // Slower during peak hours (9 AM - 6 PM) to avoid detection
  if (hour >= 9 && hour <= 18) {
    baseDelay = 7000; // 9 seconds during peak hours
  }
  
  // Extra caution during very early/late hours
  if (hour < 7 || hour > 22) {
    baseDelay = 80000; // 10 seconds during off hours
  }
  
  // Progressive slowdown for multiple messages
  if (messageCount > 10) {
    baseDelay += Math.floor(messageCount / 10) * 1000;
  }
  
  // Add randomization to avoid pattern detection
  return getHumanLikeDelay(baseDelay, 0.4);
}

// Simulate human typing patterns
async function simulateTyping(client, jid, message) {
  try {
    // Calculate realistic typing time but limit to maximum 3-5 seconds
    const words = message.split(' ').length;
    const baseTypingTime = Math.max(1000, (words / 40) * 60 * 1000 * 0.1); // Reduced to 10% instead of 30%
    const typingTimeMs = Math.min(baseTypingTime, 4000); // Cap at 3 seconds maximum
    
    // Start typing indicator
    await client.sendPresenceAvailable();
    await client.sendPresenceUnavailable();
    
    // Random delay during "typing" - much shorter now
    await new Promise(resolve => setTimeout(resolve, getHumanLikeDelay(typingTimeMs, 0.3)));
    
  } catch (error) {
    // Silent fail - typing simulation is optional
    console.log(`[Typing simulation] ${error.message}`);
  }
}

// Enhanced profile validation - check if client is properly loaded and ready
async function validateProfileReadiness(profileName, client = null) {
  try {
    console.log(`🔍 Validating profile readiness: ${profileName}`);
    
    // Step 1: Check if profile exists in activeClients
    const activeClient = client || activeClients[profileName];
    if (!activeClient) {
      return {
        ready: false,
        error: `Profile ${profileName} not found in active clients`,
        code: 'PROFILE_NOT_ACTIVE'
      };
    }
    
    // Step 2: Check client state
    try {
      const state = await activeClient.getState();
      console.log(`📱 Profile ${profileName} state: ${state}`);
      
      if (state !== 'CONNECTED') {
        return {
          ready: false,
          error: `Profile ${profileName} not connected. Current state: ${state}`,
          code: 'CLIENT_NOT_CONNECTED',
          state: state
        };
      }
    } catch (stateError) {
      return {
        ready: false,
        error: `Failed to get client state for ${profileName}: ${stateError.message}`,
        code: 'STATE_CHECK_FAILED'
      };
    }
    
    // Step 3: Check if client info is available (indicates successful WhatsApp connection)
    try {
      const info = activeClient.info;
      if (!info || !info.wid) {
        return {
          ready: false,
          error: `Profile ${profileName} client info not available - may still be initializing`,
          code: 'CLIENT_INFO_MISSING'
        };
      }
      
      console.log(`✅ Profile ${profileName} ready - WhatsApp Number: ${info.wid.user}`);
    } catch (infoError) {
      return {
        ready: false,
        error: `Failed to get client info for ${profileName}: ${infoError.message}`,
        code: 'CLIENT_INFO_FAILED'
      };
    }
    
    // Step 4: Test basic client functionality with a simple ping
    try {
      const chatId = await activeClient.getNumberId(activeClient.info.wid.user);
      if (!chatId) {
        return {
          ready: false,
          error: `Profile ${profileName} cannot validate own number - client may not be fully ready`,
          code: 'SELF_VALIDATION_FAILED'
        };
      }
    } catch (pingError) {
      console.warn(`⚠️ Profile ${profileName} ping test failed (non-critical): ${pingError.message}`);
      // Don't fail validation for ping errors as they might be API limitations
    }
    
    return {
      ready: true,
      info: activeClient.info,
      state: 'CONNECTED',
      message: `Profile ${profileName} is ready and connected`
    };
    
  } catch (error) {
    return {
      ready: false,
      error: `Profile validation failed for ${profileName}: ${error.message}`,
      code: 'VALIDATION_EXCEPTION'
    };
  }
}

// Anti-detection message sending wrapper with WhatsApp number validation
async function sendMessageSafely(client, jid, content, options = {}) {
  try {
    console.log(`📤 Attempting to send message to: ${jid}`);
    
    // Step 0: Enhanced client validation
    if (!client) {
      throw new Error('Client is not initialized');
    }
    
    // Find profile name for this client
    let profileName = null;
    for (const [name, activeClient] of Object.entries(activeClients)) {
      if (activeClient === client) {
        profileName = name;
        break;
      }
    }
    
    if (!profileName) {
      throw new Error('Profile not found for this client');
    }
    
    // Step 1: Validate profile readiness before sending
    const validation = await validateProfileReadiness(profileName, client);
    if (!validation.ready) {
      throw new Error(`Profile not ready: ${validation.error}`);
    }
    
    console.log(`✅ Profile ${profileName} validated and ready`);
    
    // Step 2: Simulate human typing for text messages (optimized)
    if (typeof content === 'string' && content.length > 10) {
      await simulateTyping(client, jid, content);
    }
    
    // Step 3: Add small random delay before actual sending (reduced)
    await new Promise(resolve => setTimeout(resolve, getHumanLikeDelay(200, 0.3)));
    
    // Step 4: Send the message directly (let WhatsApp handle validation during send)
    try {
      const result = await client.sendMessage(jid, content, options);
      console.log(`✅ Message sent successfully to ${jid} via profile ${profileName}`);

      // Step 5: Simulate reading confirmation delay (reduced)
      await new Promise(resolve => setTimeout(resolve, getHumanLikeDelay(100, 0.2)));

      return result;
    } catch (sendError) {
      const msg = String(sendError && sendError.message || '').toLowerCase();

      // Check if sending fails due to invalid WhatsApp number
      if (msg.includes('not registered') || 
          msg.includes('invalid user') ||
          msg.includes('user not found') ||
          msg.includes('phone number not registered') ||
          msg.includes('recipient not found')) {
        throw new Error(`Number not registered on WhatsApp`);
      }

      // Special-case: whatsapp-web may throw an internal 'findChat: new chat not found' error
      // which can be transient (race when creating a new chat). Retry once after a short pause.
      if (msg.includes('findchat') || msg.includes('new chat not found')) {
        // Only retry once to avoid infinite loops
        if (!options || !options.__findChatRetried) {
          console.warn(`⚠️ findChat error detected for ${jid}. Retrying once after short delay...`);
          await new Promise(resolve => setTimeout(resolve, 1000));
          try {
            const retryOpts = Object.assign({}, options || {}, { __findChatRetried: true });
            const retryResult = await client.sendMessage(jid, content, retryOpts);
            console.log(`✅ Retry succeeded for ${jid} after findChat error`);
            await new Promise(resolve => setTimeout(resolve, getHumanLikeDelay(100, 0.2)));
            return retryResult;
          } catch (retryErr) {
            console.warn(`⚠️ Retry after findChat error failed for ${jid}:`, retryErr && retryErr.message ? retryErr.message : retryErr);
            // fall through to rethrow original error below
          }
        }
      }

      // Re-throw other send errors with original message
      throw sendError;
    }
    
  } catch (error) {
    // Re-throw with context
    throw new Error(`Safe send failed: ${error.message}`);
  }
}

// Bulk WhatsApp number validation function
async function validateWhatsAppNumbers(client, phoneNumbers, maxConcurrent = 5) {
  const results = { valid: [], invalid: [], errors: [] };
  
  console.log(`🔍 Starting validation for ${phoneNumbers.length} numbers (simplified mode)`);
  
  // Instead of pre-validation, we'll mark all properly formatted numbers as valid
  // and let the actual sending process determine if they're truly valid
  for (const phone of phoneNumbers) {
    try {
      const jid = formatNumber(phone);
      if (jid && jid.includes('@c.us') && jid.length >= 12) {
        results.valid.push(phone);
        console.log(`✅ ${phone} appears to be a valid format`);
      } else {
        results.invalid.push({ phone, reason: 'Invalid phone number format' });
        console.log(`❌ ${phone} has invalid format`);
      }
    } catch (error) {
      results.errors.push({ phone, error: error.message });
      console.warn(`⚠️ Format validation error for ${phone}: ${error.message}`);
    }
  }
  
  console.log(`📊 Validation complete: ${results.valid.length} valid format, ${results.invalid.length} invalid format, ${results.errors.length} errors`);
  return results;
}

// Session health monitoring
const sessionHealth = new Map(); // profileName -> health metrics

function updateSessionHealth(profileName, metric, value) {
  if (!sessionHealth.has(profileName)) {
    sessionHealth.set(profileName, {
      messagesSent: 0,
      lastMessageTime: null,
      consecutiveFailures: 0,
      sessionStartTime: Date.now(),
      rateLimitHits: 0,
      lastHealthCheck: Date.now()
    });
  }
  
  const health = sessionHealth.get(profileName);
  health[metric] = value;
  health.lastHealthCheck = Date.now();
  
  // Check for potential issues
  if (health.consecutiveFailures > 5) {
    console.warn(`[${profileName}] High failure rate detected - consider profile rotation`);
  }
  
  if (health.rateLimitHits > 3) {
    console.warn(`[${profileName}] Multiple rate limit hits - slowing down`);
  }
}

function getSessionHealth(profileName) {
  return sessionHealth.get(profileName) || null;
}

// Advanced rate limiting with backoff
const profileCooldowns = new Map(); // profileName -> cooldown end time

function isProfileInCooldown(profileName) {
  const cooldownEnd = profileCooldowns.get(profileName);
  return cooldownEnd && Date.now() < cooldownEnd;
}

function setCooldown(profileName, minutes = 10) {
  const cooldownEnd = Date.now() + (minutes * 60 * 1000);
  profileCooldowns.set(profileName, cooldownEnd);
  console.log(`[${profileName}] Cooldown activated for ${minutes} minutes`);
}

// Safety status reporting
function logSafetyStatus() {
  console.log('\n🛡️ SAFETY STATUS REPORT');
  console.log('========================');
  
  for (const [profileName, health] of sessionHealth) {
    const cooldownEnd = profileCooldowns.get(profileName);
    const inCooldown = cooldownEnd && Date.now() < cooldownEnd;
    
    let status = '🟢 SAFE';
    if (health.consecutiveFailures >= 5 || health.rateLimitHits >= 3 || inCooldown) {
      status = '🔴 RISK';
    } else if (health.consecutiveFailures >= 3 || health.rateLimitHits >= 1) {
      status = '🟡 CAUTION';
    }
    
    console.log(`[${profileName}] ${status}`);
    console.log(`  Messages: ${health.messagesSent}, Failures: ${health.consecutiveFailures}, Rate hits: ${health.rateLimitHits}`);
    
    if (inCooldown) {
      const remaining = Math.ceil((cooldownEnd - Date.now()) / 60000);
      console.log(`  ⏳ Cooldown: ${remaining} minutes remaining`);
    }
  }
  console.log('========================\n');
}

// Advanced stealth detection and countermeasures
function addStealthCountermeasures(page) {
  return page.evaluateOnNewDocument(() => {
    // CRITICAL: Block all automation detection methods
    const originalDefineProperty = Object.defineProperty;
    Object.defineProperty = function(obj, prop, descriptor) {
      if (prop === 'webdriver' && descriptor && descriptor.get) {
        // Block attempts to redefine webdriver
        return obj;
      }
      return originalDefineProperty.apply(this, arguments);
    };
    
    // Block common automation detection scripts
    const blockedScripts = [
      'webdriver-manager',
      'selenium',
      'phantomjs',
      'automation',
      'bot-detection'
    ];
    
    const originalAppendChild = Element.prototype.appendChild;
    Element.prototype.appendChild = function(child) {
      if (child.tagName === 'SCRIPT' && child.src) {
        for (const blocked of blockedScripts) {
          if (child.src.includes(blocked)) {
            console.log('[STEALTH] Blocked automation detection script:', child.src);
            return child;
          }
        }
      }
      return originalAppendChild.call(this, child);
    };
    
    // Override console methods to hide automation logs
    const originalLog = console.log;
    console.log = function() {
      const message = Array.from(arguments).join(' ');
      if (message.includes('webdriver') || message.includes('automation') || message.includes('bot')) {
        return; // Suppress automation-related logs
      }
      return originalLog.apply(this, arguments);
    };
    
    // Spoof browser performance characteristics
    Object.defineProperty(performance, 'now', {
      value: function() {
        // Add slight randomness to performance timing to avoid fingerprinting
        return Date.now() + Math.random() * 0.1;
      },
      configurable: false
    });
    
    // Override automation-specific APIs
    if (window.chrome && window.chrome.runtime) {
      delete window.chrome.runtime.onConnect;
      delete window.chrome.runtime.onMessage;
      delete window.chrome.runtime.connect;
      delete window.chrome.runtime.sendMessage;
    }
  });
}

// Network traffic pattern normalization
function setupNetworkMasking(page) {
  return page.setRequestInterception(true).then(() => {
    page.on('request', (request) => {
      const headers = request.headers();
      
      // Remove automation-specific headers
      delete headers['accept-language'];
      delete headers['sec-fetch-site'];
      delete headers['sec-fetch-mode'];
      delete headers['sec-fetch-dest'];
      
      // Add realistic browser headers
      const newHeaders = {
        ...headers,
        'accept-language': 'en-US,en;q=0.9,es;q=0.8',
        'sec-ch-ua': '"Google Chrome";v="120", "Chromium";v="120", "Not:A-Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'none',
        'upgrade-insecure-requests': '1'
      };
      
      // Continue with modified headers
      request.continue({ headers: newHeaders });
    });
  });
}

// Browser extension simulation to appear like normal user browser
function simulateBrowserExtensions(page) {
  return page.evaluateOnNewDocument(() => {
    // Simulate common browser extensions
    window.chrome = window.chrome || {};
    window.chrome.runtime = window.chrome.runtime || {};
    
    // Simulate AdBlock extension
    window.chrome.runtime.getManifest = () => ({
      name: "AdBlock",
      version: "4.44.0",
      manifest_version: 3
    });
    
    // Simulate extension message passing (but don't actually function)
    window.chrome.runtime.sendMessage = function(extensionId, message, options, responseCallback) {
      if (responseCallback) {
        setTimeout(() => responseCallback({}), Math.random() * 100);
      }
    };
    
    // Simulate extension storage
    window.chrome.storage = {
      local: {
        get: function(keys, callback) {
          setTimeout(() => callback({}), Math.random() * 50);
        },
        set: function(items, callback) {
          if (callback) setTimeout(callback, Math.random() * 50);
        }
      }
    };
    
    // Add realistic extension IDs to chrome object
    Object.defineProperty(window.chrome, 'extensions', {
      value: {
        getAll: function(callback) {
          callback([
            { id: 'gighmmpiobklfepjocnamgkkbiglidom', name: 'AdBlock' },
            { id: 'cjpalhdlnbpafiamejdnhcphjbkeiagm', name: 'uBlock Origin' }
          ]);
        }
      },
      configurable: false
    });
    
    // Simulate realistic browser vendor information
    Object.defineProperty(navigator, 'vendor', {
      get: () => 'Google Inc.',
      configurable: false
    });
    
    Object.defineProperty(navigator, 'vendorSub', {
      get: () => '',
      configurable: false
    });
    
    // Simulate hardware concurrency realistically
    Object.defineProperty(navigator, 'hardwareConcurrency', {
      get: () => 8,
      configurable: false
    });
    
    // Simulate platform information
    Object.defineProperty(navigator, 'platform', {
      get: () => 'Win32',
      configurable: false
    });
    
    console.log('[STEALTH] Browser extension simulation activated');
  });
}

// Controllers for jobs
const jobControllers = {}; // { jobId: { paused: false, cancelled: false } }

// Helper function to cleanup stale job controllers on app start
function cleanupStaleJobControllers() {
  try {
    const reports = readJsonLog();
    const activeJobIds = new Set();
    
    // Find jobs that are actually still active
    for (const job of reports) {
      const status = (job.status || '').toLowerCase();
      if (status === 'in_progress' || status === 'scheduled') {
        activeJobIds.add(job.id);
      }
    }
    
    // Remove controllers for jobs that are no longer active
    let cleaned = 0;
    for (const jobId of Object.keys(jobControllers)) {
      if (!activeJobIds.has(jobId)) {
        delete jobControllers[jobId];
        cleaned++;
      }
    }
    
    console.log(`🧹 Cleaned up ${cleaned} stale job controllers, ${Object.keys(jobControllers).length} remain active`);
  } catch (e) {
    console.warn('Failed to cleanup stale job controllers:', e.message);
  }
}

// 🚀 Global cache for clients
//const activeClients = {};
let loginWindow;
let mainWin;
let templateWin;

function getUserDataDir() {
  try {
    return app.getPath('userData');
  } catch (e) {
    // fallback to __dirname if app not ready (very unlikely)
    return __dirname;
  }
}

function getProfilesFile() {
  const dir = getUserDataDir();
  try { fs.mkdirSync(dir, { recursive: true }); } catch (e) {}
  return path.join(dir, 'profiles.json');
}

// Comprehensive profile name validation function
function validateProfileName(name) {
  const errors = [];
  
  // Check if empty or only whitespace
  if (!name || name.trim() === '') {
    errors.push('Profile name cannot be empty');
  }
  
  // Check length (minimum 2, maximum 50 characters)
  if (name.length < 2) {
    errors.push('Profile name must be at least 2 characters long');
  }
  
  if (name.length > 50) {
    errors.push('Profile name cannot exceed 50 characters');
  }
  
  // Check for spaces
  if (/\s/.test(name)) {
    errors.push('Profile name cannot contain spaces');
  }
  
  // Check for invalid characters (only allow letters, numbers, underscore, hyphen)
  if (!/^[a-zA-Z0-9_-]+$/.test(name)) {
    errors.push('Profile name can only contain letters, numbers, underscore (_), and hyphen (-)');
  }
  
  // Check if starts with number
  if (/^[0-9]/.test(name)) {
    errors.push('Profile name cannot start with a number');
  }
  
  // Check for reserved names
  const reservedNames = ['all', 'none', 'null', 'undefined', 'admin', 'system', 'default'];
  if (reservedNames.includes(name.toLowerCase())) {
    errors.push('Profile name cannot be a reserved word');
  }
  
  return {
    isValid: errors.length === 0,
    errors: errors
  };
}

function loadProfiles() {
  // Prefer DB-backed profiles when DB is available
  try {
    if (typeof db !== 'undefined' && db) {
      const rows = db.prepare('SELECT * FROM profiles WHERE is_delete=0').all();
      return rows.map(r => ({
        name: r.name,
        number: r.number,
        pushname: r.pushname,
        session: r.session,
        is_active: r.is_active,
        is_delete: r.is_delete,
        created: r.created,
        modified: r.modified,
        last_connected: r.last_connected
      }));
    }
  } catch (e) {
    console.error('Failed to load profiles from DB:', e.message);
  }

  // Fallback to JSON file for legacy compatibility
  const profilesFile = getProfilesFile();
  if (!fs.existsSync(profilesFile)) return [];
  try { return JSON.parse(fs.readFileSync(profilesFile)); } catch (e) { console.error('Failed to parse profiles.json:', e.message); return []; }
}

function saveProfiles(profiles) {
  // If DB is available, persist profiles there (upsert by name)
  try {
    if (typeof db !== 'undefined' && db) {
      const upsert = db.prepare(`
        INSERT INTO profiles (name, number, pushname, session, is_active, is_delete, created, modified, last_connected)
        VALUES (@name,@number,@pushname,@session,@is_active,@is_delete,@created,@modified,@last_connected)
        ON CONFLICT(name) DO UPDATE SET
          number=excluded.number,
          pushname=excluded.pushname,
          session=excluded.session,
          is_active=excluded.is_active,
          is_delete=excluded.is_delete,
          modified=excluded.modified,
          last_connected=excluded.last_connected
      `);

      const insertMany = db.transaction((rows) => {
        for (const p of rows) {
          upsert.run({
            name: p.name,
            number: p.number || "",
            pushname: p.pushname || "",
            session: p.session || "",
            is_active: p.is_active === undefined ? 1 : p.is_active,
            is_delete: p.is_delete === undefined ? 0 : p.is_delete,
            created: p.created ? String(p.created) : getLocalISOString(),
            modified: p.modified ? String(p.modified) : getLocalISOString(),
            last_connected: p.last_connected ? String(p.last_connected) : getLocalISOString()
          });
        }
      });

      insertMany(profiles || []);
      return;
    }
  } catch (e) {
    console.error('Failed to save profiles to DB:', e.message);
  }

  // Fallback: write JSON file
  const profilesFile = getProfilesFile();
  try { fs.writeFileSync(profilesFile, JSON.stringify(profiles, null, 2)); }
  catch (e) { console.error('Failed to write profiles.json:', e.message); }
}

function loadTemplates() {
  // DB-only: return templates from DB or empty array if DB not initialized
  try {
    if (typeof db !== 'undefined' && db) {
      const rows = db.prepare("SELECT * FROM templates WHERE is_delete=0").all();
      return rows.map(r => ({
        name: r.name,
        type: r.type || 'Text',
        message: r.message || r.Message || "",
        media_path: r.media_path || null,
        media_filename: r.media_filename || null,
        status: r.is_active ? 'active' : 'inactive',
        // sendOption: r.sendOption || 'instant', // temporarily disabled
        // afterDays: r.afterDays || 0, // temporarily disabled
        is_delete: r.is_delete || 0,
        created: r.created,
        modified: r.modified
      }));
    }
  } catch (e) {
    console.error('Failed to load templates from DB:', e.message);
  }

  // DB not available yet
  return [];
}

function saveTemplates(templates) {
  // If DB is ready, upsert into DB, otherwise write JSON file
  try {
    if (typeof db !== 'undefined' && db) {
      const upsert = db.prepare(`
        INSERT INTO templates (name, message, sendOption, afterDays, is_active, is_delete, created, modified)
        VALUES (@name,@message,@sendOption,@afterDays,@is_active,@is_delete,@created,@modified)
        ON CONFLICT(name) DO UPDATE SET
          message=excluded.message,
          sendOption=excluded.sendOption,
          afterDays=excluded.afterDays,
          is_active=excluded.is_active,
          is_delete=excluded.is_delete,
          modified=excluded.modified
      `);

      const insertMany = db.transaction((rows) => {
        for (const t of rows) {
          const now = getLocalISOString();
          const item = {
            name: t.name,
            message: t.message || t.Message || "",
            // sendOption: t.sendOption || 'instant', // temporarily disabled
            afterDays: Number(t.afterDays) || 0, // keep default value for DB upsert
            sendOption: t.sendOption || 'instant', // keep default for DB upsert
            is_active: t.is_active === undefined ? 1 : t.is_active,
            is_delete: t.is_delete === undefined ? 0 : t.is_delete,
            created: t.created ? String(t.created) : now,
            modified: now
          };
          upsert.run(item);
        }
      });

      insertMany(templates);
      // Do NOT write templates.json here anymore; persist only to DB
      return;
    }
  } catch (e) {
    console.error('Failed to save templates to DB:', e.message);
  }

  // If we reach here, DB is not available or upsert failed. We do not write to templates.json
  // by design (templates are stored only in the DB). Log for troubleshooting.
  console.error('saveTemplates aborted: no DB available to persist templates.');
}

function addOrUpdateProfile(profileName, sessionPath, clientInfo = null) {
  const now = getLocalISOString();

  // If DB is available, upsert into profiles table
  try {
    if (typeof db !== 'undefined' && db) {
      const upsert = db.prepare(`
        INSERT INTO profiles (name, number, pushname, session, is_active, is_delete, created, modified, last_connected)
        VALUES (@name,@number,@pushname,@session,@is_active,@is_delete,@created,@modified,@last_connected)
        ON CONFLICT(name) DO UPDATE SET
          number=excluded.number,
          pushname=excluded.pushname,
          session=excluded.session,
          is_active=excluded.is_active,
          is_delete=excluded.is_delete,
          modified=excluded.modified,
          last_connected=excluded.last_connected
      `);

      upsert.run({
        name: profileName,
        number: clientInfo ? clientInfo.wid.user : "",
        pushname: clientInfo ? clientInfo.pushname : "",
        session: sessionPath || "",
        is_active: 1,
        is_delete: 0,
        created: now,
        modified: now,
        last_connected: now
      });
      return;
    }
  } catch (e) {
    console.error('addOrUpdateProfile DB error:', e.message);
  }

  // Fallback to JSON file behavior
  let profiles = loadProfiles();
  let existing = profiles.find(p => p.name === profileName);
  if (existing) {
    existing.is_active = 1;
    existing.is_delete = 0;
    existing.modified = now;
    existing.last_connected = now;
    existing.session = sessionPath;
    if (clientInfo) {
      existing.number = clientInfo.wid.user;
      existing.pushname = clientInfo.pushname;
    }
  } else {
    profiles.push({
      name: profileName,
      number: clientInfo ? clientInfo.wid.user : "",
      pushname: clientInfo ? clientInfo.pushname : "",
      is_active: 1,
      is_delete: 0,
      created: now,
      modified: now,
      last_connected: now,
      session: sessionPath
    });
  }

  saveProfiles(profiles);
}

function getJsonLogsPath() {
  const dir = getUserDataDir();
  const userJsonDir = path.join(dir, 'Jsonlogs');
  const userPath = path.join(userJsonDir, 'sent_message_logs.json');

  // Ensure user Jsonlogs folder exists
  try { fs.mkdirSync(userJsonDir, { recursive: true }); } catch (e) {}

  // If user copy exists, prefer it
  if (fs.existsSync(userPath)) return userPath;

  // Try several developer / packaged locations for a seed copy and copy it to userData
  const candidates = [];
  try {
    // Project-level during development
    candidates.push(path.join(__dirname, 'Jsonlogs', 'sent_message_logs.json'));
  } catch (e) {}
  try {
    // Common packaged locations
    if (process && process.resourcesPath) {
      candidates.push(path.join(process.resourcesPath, 'Jsonlogs', 'sent_message_logs.json'));
      candidates.push(path.join(process.resourcesPath, 'app', 'Jsonlogs', 'sent_message_logs.json'));
      candidates.push(path.join(process.resourcesPath, 'assets', 'Jsonlogs', 'sent_message_logs.json'));
    }
  } catch (e) {}

  for (const c of candidates) {
    try {
      if (c && fs.existsSync(c)) {
        try { fs.copyFileSync(c, userPath); } catch (e) { /* ignore copy error */ }
        return userPath;
      }
    } catch (e) {}
  }

  // Fallback: return user path (empty file will be created when written)
  return userPath;
}

// Convert alerts, stats and settings storage to use DB to avoid JSON files in packaged app
function ensureAuxTables() {
  try {
    db.prepare(`
      CREATE TABLE IF NOT EXISTS alerts (
        id TEXT PRIMARY KEY,
        timestamp TEXT,
        profile TEXT,
        level TEXT,
        type TEXT,
        message TEXT
      )
    `).run();

    db.prepare(`
      CREATE TABLE IF NOT EXISTS daily_stats (
        day TEXT PRIMARY KEY,
        total INTEGER DEFAULT 0,
        per_profile TEXT DEFAULT '{}'
      )
    `).run();

    db.prepare(`
      CREATE TABLE IF NOT EXISTS app_settings (
        key TEXT PRIMARY KEY,
        value TEXT
      )
    `).run();
  } catch (e) {
    console.error('Failed to ensure aux tables:', e.message);
  }
}

function logAlert(alert) {
  try {
    const id = `alert_${getLocalISOString().replace(/[:.]/g,'-')}`;
    const ts = getLocalISOString();
    const stmt = db.prepare('INSERT INTO alerts (id,timestamp,profile,level,type,message) VALUES (?,?,?,?,?,?)');
    stmt.run(id, ts, alert.profile || '', alert.level || '', alert.type || '', String(alert.message || ''));
    const row = db.prepare('SELECT * FROM alerts WHERE id = ?').get(id);
    try { if (mainWin) mainWin.webContents.send('alert-created', row); } catch (e) {}
    return row;
  } catch (e) { console.error('logAlert DB failed', e.message); return null; }
}

function getAlerts(limit = 200) {
  try { return db.prepare('SELECT * FROM alerts ORDER BY timestamp DESC LIMIT ?').all(limit); } catch (e) { console.error('getAlerts failed', e.message); return []; }
}

function incStat(profileName) {
  try {
    const day = getLocalISOString().slice(0,10);
    const row = db.prepare('SELECT total, per_profile FROM daily_stats WHERE day = ?').get(day);
    if (row) {
      const per = JSON.parse(row.per_profile || '{}');
      per[profileName] = (per[profileName] || 0) + 1;
      const total = (row.total || 0) + 1;
      db.prepare('UPDATE daily_stats SET total = ?, per_profile = ? WHERE day = ?').run(total, JSON.stringify(per), day);
    } else {
      const per = {};
      per[profileName] = 1;
      db.prepare('INSERT INTO daily_stats (day, total, per_profile) VALUES (?, ?, ?)').run(day, 1, JSON.stringify(per));
    }
  } catch (e) { console.error('incStat DB failed', e.message); }
}

function getDailyStats(days = 7) {
  try {
    const out = [];
    for (let i = 0; i < days; i++) {
      const day = new Date(Date.now() - i * 24 * 3600 * 1000).toISOString().slice(0,10);
      const row = db.prepare('SELECT * FROM daily_stats WHERE day = ?').get(day);
      if (row) {
        out.push({ day, data: { total: row.total || 0, perProfile: JSON.parse(row.per_profile || '{}') } });
      } else {
        out.push({ day, data: { total: 0, perProfile: {} } });
      }
    }
    return out;
  } catch (e) { console.error('getDailyStats failed', e.message); return []; }
}

// Collect all daily_stats rows and send them to Digitalsolutions API for the logged-in user
async function sendDailyStatsToDigitalsolutions(userInfo) {
  try {
    if (!db) {
      console.warn('sendDailyStatsToDigitalsolutions: DB not initialized');
      return { success: false, error: 'DB not initialized' };
    }

    // Read all rows from daily_stats
    const rows = db.prepare('SELECT day, total, per_profile FROM daily_stats ORDER BY day DESC').all();
    const stats = (rows || []).map(r => ({ day: r.day, total: r.total || 0, per_profile: (() => { try { return JSON.parse(r.per_profile || '{}'); } catch (e) { return {}; } })() }));

    // Allow endpoint override via settings; keep a sensible default
    const settings = loadSettings();
    const url = (settings && settings.digitalsolutionsApiUrl) ? settings.digitalsolutionsApiUrl : 'https://ticket.digitalsolutions.co.in/SyncDailyStats';

    const payload = {
      user: {
        email: userInfo && userInfo.email ? userInfo.email : null,
        name: userInfo && userInfo.name ? userInfo.name : null,
        id: userInfo && (userInfo.id || userInfo.mudId) ? (userInfo.id || userInfo.mudId) : null
      },
      stats
    };
    //console.log('sendDailyStatsToDigitalsolutions: preparing to send payload to', payload); 
    // If no internet connection, skip attempt (non-blocking)
    try {
      const conn = await checkInternetConnectivity();
      if (!conn || !conn.connected) {
        console.warn('sendDailyStatsToDigitalsolutions: no internet connection, will skip sync');
        return { success: false, error: 'No internet' };
      }
    } catch (e) {
      // continue and attempt axios post
    }

    // POST to remote API (timeout 10s)
    const res = await axios.post(url, payload, { timeout: 10000 });
    console.log('sendDailyStatsToDigitalsolutions: sync result', res && res.status ? res.status : 'no-status');
    return { success: true, response: res && res.data ? res.data : null };
  } catch (e) {
    console.warn('sendDailyStatsToDigitalsolutions failed:', e && e.message ? e.message : e);
    return { success: false, error: e && e.message ? e.message : String(e) };
  }
}

function loadSettings() {
  try {
    const rows = db.prepare('SELECT key, value FROM app_settings').all();
    const s = {};
    for (const r of rows) {
      try { s[r.key] = JSON.parse(r.value); } catch (e) { s[r.key] = r.value; }
    }
    const defaults = {
      rateLimitPerMinute: 10,
      perProfileLimitPerMinute: 5,
      maxConsecutiveFailuresBeforePause: 3,
      minWaitPeriodDays: 7,
      maxPerProfilePerDay: 50,
      dryRun: false, // Always disabled for security
      safeMode: true // Always enabled for security
    };
    const result = { ...defaults, ...s };
    
    // Force security settings regardless of stored values
    result.safeMode = true; // Always force safe mode ON
    result.dryRun = false;  // Always force dry run OFF
    
    return result;
  } catch (e) { console.error('loadSettings DB failed', e.message); return { rateLimitPerMinute:10, perProfileLimitPerMinute:5, maxConsecutiveFailuresBeforePause:3, minWaitPeriodDays:7, maxPerProfilePerDay:50, dryRun:false, safeMode:true }; }
}

function saveSettings(settings) {
  try {
    // Force security settings regardless of user input
    settings.safeMode = true; // Always force safe mode ON
    settings.dryRun = false;  // Always force dry run OFF
    
    const insert = db.prepare('INSERT INTO app_settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value');
    for (const k of Object.keys(settings)) {
      insert.run(k, JSON.stringify(settings[k]));
    }
    return true;
  } catch (e) { console.error('saveSettings DB failed', e.message); return false; }
}


function addOrUpdateTemplate(templateName, TemplateMessage) {
  // Atomic DB upsert using prepared statement
  try {
    const now = getLocalISOString();
    const item = {
      name: templateName,
      message: TemplateMessage,
      sendOption: 'instant',
      afterDays: 0,
      is_active: 1,
      is_delete: 0,
      created: now,
      modified: now
    };
    upsertTemplate.run(item);
  } catch (e) {
    console.error('addOrUpdateTemplate DB error:', e.message);
  }
}

function createMainWindow() {
    mainWin = new BrowserWindow({
        width: 1400,
        height: 900,
        webPreferences: {
            nodeIntegration: true,
            contextIsolation: false
        }
    });
    mainWin.removeMenu();
    mainWin.loadFile("index.html");

    if (loginWindow) {
    loginWindow.close();
    loginWindow = null;
  }

    mainWin.on("close", (e) => {
    // Check for active jobs with in_progress, paused states
    const activeJobs = Object.entries(jobControllers)
      .filter(([id, ctrl]) => !ctrl.cancelled && !ctrl.completed);

    // Also check database for jobs with active statuses
    let dbActiveJobs = [];
    try {
      const jsonLogPath = path.join(getUserDataDir(), "Jsonlogs", "sent_message_logs.json");
      if (fs.existsSync(jsonLogPath)) {
        const logs = JSON.parse(fs.readFileSync(jsonLogPath, "utf8"));
        dbActiveJobs = logs.filter(job => {
          const status = (job.status || '').toLowerCase();
          return status === 'in_progress' || status === 'paused' || status === 'scheduled';
        });
      }
    } catch (error) {
      console.log("Error checking job statuses:", error.message);
    }

    const totalActiveJobs = [...activeJobs, ...dbActiveJobs];
    const hasInProgressOrPaused = dbActiveJobs.some(job => {
      const status = (job.status || '').toLowerCase();
      return status === 'in_progress' || status === 'paused';
    });

    // Always prevent close and ask for confirmation
    e.preventDefault();

    if (totalActiveJobs.length > 0) {
      // Send specific data about job types
      mainWin.webContents.send("confirm-exit", {
        jobIds: activeJobs.map(([id]) => id),
        dbJobs: dbActiveJobs,
        hasActiveMessages: hasInProgressOrPaused
      });
    }
    else{
      mainWin.webContents.send("confirmeasy-exit");
    }
  });

}


function createLoginWindow() {
    loginWindow = new BrowserWindow({
        width: 1000,
        height: 700,
        webPreferences: {
            nodeIntegration: true,
            contextIsolation: false
        }
    });
    loginWindow.removeMenu();
    loginWindow.loadFile("login.html");

    
}

// Auto-start all active profiles when the app is ready so activeClients is populated
app.whenReady().then(() => {
  try {
    // Cleanup stale job controllers first
    cleanupStaleJobControllers();
    
    // Start periodic connectivity monitoring
    //console.log('🌐 Starting periodic internet connectivity monitoring...');
    startPeriodicConnectivityCheck();
    
    const profiles = loadProfiles();
    const activeProfiles = (profiles || []).filter(p => p.is_active === 1 || p.is_active === true || p.is_active === undefined);
    //console.log(`Auto-starting ${activeProfiles.length} active profiles...`);
    for (const p of activeProfiles) {
      try {
        //console.log(`Attempting to start client for profile: ${p.name}`);
        startWhatsAppClient(p.name);
      } catch (e) {
        console.error(`Failed to start client for ${p.name}:`, e.message);
      }
    }
  } catch (e) {
    console.error('Error auto-starting profiles:', e.message);
  }
});

// ✅ Create new Add Profile popup
ipcMain.on("open-add-profile", () => {
    const popup = new BrowserWindow({
        width: 400,
        height: 500,
        parent: mainWin,
        modal: true,
        webPreferences: {
            nodeIntegration: true,
            contextIsolation: false
        }
    });
    popup.removeMenu();
    popup.loadFile("addProfile.html");
});

// ✅ Create new Template page
ipcMain.on("open-template-window", () => {
  templateWin = new BrowserWindow({
    width: 1000,
    height: 700,
    parent: mainWin,
    modal: true,
    webPreferences: {
      nodeIntegration: true,
      contextIsolation: false
    }
  });
  templateWin.removeMenu();
  templateWin.loadFile("template.html");
  
  // Clear reference when window is closed
  templateWin.on('closed', () => {
    templateWin = null;
  });
});


// ✅ Create new Template page
ipcMain.on("open-Reports-window", () => {
  const templateWin = new BrowserWindow({
    width: 1000,
    height: 700,
    parent: mainWin,
    modal: true,
    webPreferences: {
      nodeIntegration: true,
      contextIsolation: false
    }
  });
  templateWin.removeMenu();
  templateWin.loadFile("reports.html");
});

// ✅ Create new Template page
ipcMain.on("open-Patients-window", () => {
  const templateWin = new BrowserWindow({
    width: 1000,
    height: 700,
    parent: mainWin,
    modal: true,
    webPreferences: {
      nodeIntegration: true,
      contextIsolation: false
    }
  });
  templateWin.removeMenu();
  templateWin.loadFile("patient.html");
});

// Open settings window
ipcMain.on("open-settings-window", () => {
    const settingsWin = new BrowserWindow({
        width: 520,
        height: 600,
        parent: mainWin,
        modal: false,
        webPreferences: {
            nodeIntegration: true,
            contextIsolation: false
        }
    });
    settingsWin.removeMenu();
    settingsWin.loadFile("settings.html");
});

// Add template window
ipcMain.on("open-add-template", () => {
  addTemplateWin = new BrowserWindow({
    width: 600,
    height: 500,
    parent: templateWin || mainWin, // Use templateWin as parent if it exists
    modal: true,
    webPreferences: {
      nodeIntegration: true,
      contextIsolation: false
    }
  });
  addTemplateWin.removeMenu();
  addTemplateWin.loadFile("addTemplate.html");
  
  // Clear reference when window is closed
  addTemplateWin.on('closed', () => {
    addTemplateWin = null;
  });
});

// Open Add Schedule popup
ipcMain.on('open-add-schedule', () => {
  const popup = new BrowserWindow({
    width: 500,
    height: 600,
    parent: mainWin,
    modal: true,
    webPreferences: {
      nodeIntegration: true,
      contextIsolation: false
    }
  });
  popup.removeMenu();
  popup.loadFile('addSchedule.html');
});

// Open full Schedules window (grid view)
ipcMain.on('open-schedules-window', () => {
  const win = new BrowserWindow({
    width: 1000,
    height: 700,
    parent: mainWin,
    webPreferences: {
      nodeIntegration: true,
      contextIsolation: false
    }
  });
  win.removeMenu();
  win.loadFile('schedules.html');
});

// Open Schedule Sequence window (template sequencing)
ipcMain.on('open-schedule-sequence-window', () => {
  const win = new BrowserWindow({
    width: 1200,
    height: 800,
    parent: mainWin,
    webPreferences: {
      nodeIntegration: true,
      contextIsolation: false
    }
  });
  win.removeMenu();
  win.loadFile('scheduleSequence.html');
});

// Broadcast schedule-saved to all renderer windows so grids can refresh
ipcMain.on('schedule-saved', () => {
  BrowserWindow.getAllWindows().forEach(w => {
    try { w.webContents.send('schedule-saved'); } catch (e) {}
  });
});

// Forward prefill-schedule to the addSchedule popup if it's open
ipcMain.on('prefill-schedule', (event, data) => {
  const addWin = BrowserWindow.getAllWindows().find(w => w.getTitle && w.getTitle().includes('Add Schedule'));
  // fallback: find by file path check
  if (!addWin) {
    const candidate = BrowserWindow.getAllWindows().find(w => {
      try {
        return w.webContents.getURL().endsWith('addSchedule.html');
      } catch (e) { return false; }
    });
    if (candidate) candidate.webContents.send('prefill-schedule', data);
  } else {
    addWin.webContents.send('prefill-schedule', data);
  }
});

// Return all schedules
ipcMain.handle('get-schedules', async () => {
  try {
    const rows = db.prepare('SELECT * FROM schedules WHERE is_active = 1').all();
    return rows.map(r => ({
      id: r.id,
      name: r.name,
      templateName: r.templateName,
      days: r.days,
      profiles: (() => { try { return JSON.parse(r.profiles || '[]'); } catch { return []; } })(),
      created: r.created,
      modified: r.modified
    }));
  } catch (e) {
    console.error('get-schedules error:', e.message);
    return [];
  }
});

// Get single schedule by name
ipcMain.handle('get-schedule', async (event, name) => {
  try {
    const row = db.prepare('SELECT * FROM schedules WHERE name = ?').get(name);
    if (!row) return null;
    return {
      id: row.id,
      name: row.name,
      templateName: row.templateName,
      days: row.days,
      profiles: (() => { try { return JSON.parse(row.profiles || '[]'); } catch { return []; } })(),
      created: row.created,
      modified: row.modified
    };
  } catch (e) {
    console.error('get-schedule error:', e.message);
    return null;
  }
});

// Soft-delete schedule
ipcMain.handle('delete-schedule', async (event, name) => {
  try {
    const now = getLocalISOString();
    const res = db.prepare('UPDATE schedules SET is_active=0, modified=? WHERE name = ?').run(now, name);
    return { success: res.changes > 0 };
  } catch (e) {
    console.error('delete-schedule error:', e.message);
    return { success: false, error: e.message };
  }
});

// Provide profiles list (from profiles.json)
ipcMain.handle('get-profiles-list', async () => {
  try {
    const profiles = loadProfiles();
    return profiles.filter(p => p.is_active === 1).map(p => p.name);
  } catch (e) {
    console.error('get-profiles-list error:', e.message);
    return [];
  }
});

// Return full profile objects (preferred by renderers that need number/pushname)
ipcMain.handle('get-profiles', async () => {
  try {
    const profiles = loadProfiles();
    // Ensure we return consistent fields
    return profiles.map(p => ({
      name: p.name,
      number: p.number || '',
      pushname: p.pushname || '',
      session: p.session || '',
      is_active: p.is_active === undefined ? 1 : p.is_active,
      last_connected: p.last_connected || ''
    }));
  } catch (e) {
    console.error('get-profiles error:', e.message);
    return [];
  }
});

// Get all patients from database
ipcMain.handle('get-all-patients', async () => {
  try {
    if (!db) {
      console.error('Database not initialized');
      return [];
    }
    const stmt = db.prepare('SELECT * FROM patients ORDER BY id DESC');
    const patients = stmt.all();
    return patients;
  } catch (e) {
    console.error('get-all-patients error:', e.message);
    return [];
  }
});

// Provide templates list (DB-backed)
ipcMain.handle('get-templates-list', async () => {
  try {
    const rows = db.prepare("SELECT name FROM templates WHERE is_delete=0").all();
    return rows.map(r => r.name);
  } catch (e) {
    console.error('get-templates-list error:', e.message);
    return [];
  }
});

// Save schedule
ipcMain.handle('save-schedule', async (event, schedule) => {
  try {
    const now = getLocalISOString();
    const payload = {
      name: schedule.name,
      templateName: schedule.template,
      days: Number(schedule.days) || 0,
      profiles: JSON.stringify(schedule.profiles || []),
      is_active: 1,
      created: now,
      modified: now
    };
    upsertSchedule.run(payload);
    return { success: true };
  } catch (e) {
    console.error('save-schedule error:', e.message);
    return { success: false, error: e.message };
  }
});

// Edit template window
ipcMain.on("open-edit-template", (event, name) => {
  const popup = new BrowserWindow({
    width: 600,
    height: 500,
    parent: mainWin,
    modal: true,
    webPreferences: {
      nodeIntegration: true,
      contextIsolation: false
    }
  });
  popup.removeMenu();
  const path = require("path");
    const templatePath = path.join(__dirname, "addTemplate.html");
    const url = `file://${templatePath}?name=${encodeURIComponent(name)}`;

    popup.loadURL(url);
});

ipcMain.on("get-template", (event, name) => {
  let templates = loadTemplates();
  const template = templates.find(t => t.name === name);
  if (template) event.sender.send("template-data", template);
});

// Return all templates (DB-backed when available)
ipcMain.handle('get-templates', async () => {
  try {
    return loadTemplates();
  } catch (e) {
    console.error('Failed to get templates:', e.message);
    return [];
  }
});

// Toggle template status by name
ipcMain.handle('toggle-template-status-by-name', async (event, templateName, newStatus) => {
  try {
    if (!db) throw new Error('Database not available');
    
    const stmt = db.prepare('UPDATE templates SET status = ? WHERE name = ?');
    const result = stmt.run(newStatus, templateName);
    
    if (result.changes > 0) {
      console.log(`Template "${templateName}" status updated to ${newStatus}`);
      return { success: true, message: `Template status updated to ${newStatus}` };
    } else {
      throw new Error(`Template "${templateName}" not found`);
    }
  } catch (e) {
    console.error('Failed to toggle template status:', e.message);
    throw e;
  }
});

// Template Sequence Handlers
ipcMain.handle('save-template-sequence', async (event, sequence) => {
  try {
    if (!db) throw new Error('Database not available');
    
    console.log('Received sequence to save:', sequence); // Debug
    
    // Create template_sequences table if it doesn't exist
    db.exec(`
      CREATE TABLE IF NOT EXISTS template_sequences (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        description TEXT,
        steps TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
      )
    `);
    
    // Ensure steps are properly ordered by sequenceNumber and assign correct stepNumber
    let orderedSteps = [];
    if (sequence.steps && Array.isArray(sequence.steps)) {
      // Sort by sequenceNumber first, then assign correct stepNumber
      orderedSteps = sequence.steps
        .sort((a, b) => (parseInt(a.sequenceNumber) || 0) - (parseInt(b.sequenceNumber) || 0))
        .map((step, index) => ({
          ...step,
          stepNumber: index + 1, // Assign sequential step numbers starting from 1
          sequenceNumber: parseInt(step.sequenceNumber) || (index + 1)
        }));
      
      console.log('Original steps:', sequence.steps);
      console.log('Ordered steps with corrected stepNumber:', orderedSteps);
    }
    
    const stepsJson = JSON.stringify(orderedSteps);
    console.log('Steps JSON to save:', stepsJson); // Debug
    
    const stmt = db.prepare(`
      INSERT INTO template_sequences (name, description, steps, created_at, updated_at)
      VALUES (?, ?, ?, ?, ?)
    `);
    
    const now = getLocalISOString();
    const result = stmt.run(
      sequence.name || 'Template Sequence',
      sequence.description || '',
      stepsJson,
      now,
      now
    );
    
    console.log('Database insert result:', result); // Debug
    console.log('✅ Template sequence saved with proper ordering');
    
    return { success: true, id: result.lastInsertRowid };
  } catch (e) {
    console.error('Failed to save template sequence:', e.message);
    return { success: false, error: e.message };
  }
});

ipcMain.handle('get-template-sequences', async () => {
  try {
    if (!db) return [];
    
    // Create table if it doesn't exist
    db.exec(`
      CREATE TABLE IF NOT EXISTS template_sequences (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        description TEXT,
        steps TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
      )
    `);
    
    const sequences = db.prepare('SELECT * FROM template_sequences ORDER BY created_at DESC').all();
    
    return sequences.map(seq => {
      let steps = [];
      try {
        steps = JSON.parse(seq.steps || '[]');
        // Ensure steps are ordered by stepNumber (not sequenceNumber)
        if (Array.isArray(steps)) {
          steps = steps
            .map((step, index) => ({
              ...step,
              stepNumber: step.stepNumber || (index + 1) // Use existing stepNumber or assign sequential
            }))
            .sort((a, b) => a.stepNumber - b.stepNumber);
        }
      } catch (e) {
        console.error('Failed to parse steps JSON:', e.message);
        steps = [];
      }
      
      return {
        id: seq.id,
        name: seq.name,
        description: seq.description,
        steps: steps,
        createdAt: seq.created_at,
        updatedAt: seq.updated_at
      };
    });
  } catch (e) {
    console.error('Failed to get template sequences:', e.message);
    return [];
  }
});

ipcMain.handle('delete-template-sequence', async (event, id) => {
  try {
    if (!db) throw new Error('Database not available');
    
    const stmt = db.prepare('DELETE FROM template_sequences WHERE id = ?');
    stmt.run(id);
    
    return { success: true };
  } catch (e) {
    console.error('Failed to delete template sequence:', e.message);
    return { success: false, error: e.message };
  }
});

// Get the active/current template sequence for messaging
ipcMain.handle('get-active-template-sequence', async () => {
  try {
    if (!db) return null;
    
    // Get the most recent sequence (you can modify this logic to have a specific "active" flag)
    const sequence = db.prepare('SELECT * FROM template_sequences ORDER BY created_at DESC LIMIT 1').get();
    
    if (!sequence) return null;
    
    return {
      id: sequence.id,
      name: sequence.name,
      description: sequence.description,
      steps: JSON.parse(sequence.steps || '[]'),
      createdAt: sequence.created_at
    };
  } catch (e) {
    console.error('Failed to get active template sequence:', e.message);
    return null;
  }
});

// Return templates scheduled 'after' with matching patients
/* scheduled-today handler temporarily disabled because sendOption/afterDays are commented out in UI
ipcMain.handle('scheduled-today', async () => {
  try {
    if (!db) return [];
    const templates = db.prepare("SELECT name, afterDays FROM templates WHERE sendOption = 'after' AND is_delete = 0").all();
    const results = [];
    for (const t of templates) {
      const days = Number(t.afterDays) || 0;
      const cutoff = dayjs().subtract(days, 'day').format('YYYY-MM-DD');
      const patients = db.prepare(`SELECT * FROM patients WHERE is_active=1 AND is_Deleted=0 AND last_visited IS NOT NULL AND date(last_visited) <= date(?)`).all(cutoff);
      results.push({ template: t.name, afterDays: days, patients });
    }
    return results;
  } catch (e) {
    console.error('scheduled-today error:', e.message);
    return [];
  }
});
*/

// Compute schedules and matching patients and open a results window
ipcMain.on('open-scheduled-results', async () => {
  try {
    if (!db) return;
    
    // First, read template_sequences table to get the sequence configuration
    let templateSequence = null;
    try {
      const sequenceRow = db.prepare('SELECT * FROM template_sequences ORDER BY created_at DESC LIMIT 1').get();
      if (sequenceRow) {
        templateSequence = {
          id: sequenceRow.id,
          name: sequenceRow.name,
          description: sequenceRow.description,
          steps: JSON.parse(sequenceRow.steps || '[]')
        };
        console.log(`[Template Sequence] Found active sequence: ${templateSequence.name} with ${templateSequence.steps.length} steps`);
        console.log(`[Template Sequence] Template order: ${templateSequence.steps.map(s => s.templateName).join(' → ')}`);
      } else {
        console.log('[Template Sequence] No template sequence found, using default logic');
      }
    } catch (e) {
      console.error('Failed to load template sequence:', e.message);
    }
    
    // Load all active schedules (ascending by days) and candidate patients once.
    const schedules = db.prepare('SELECT * FROM schedules WHERE is_active = 1 ORDER BY days ASC').all();
    
    if (!schedules || schedules.length === 0) {
      console.log('[Schedule Debug] No active schedules found');
      // Check if any schedules exist at all
      const allSchedules = db.prepare('SELECT * FROM schedules').all();
      console.log(`[Schedule Debug] Total schedules in DB: ${allSchedules.length}`);
      if (allSchedules.length > 0) {
        console.log('[Schedule Debug] All schedules:', allSchedules.map(s => `${s.name} (days: ${s.days}, active: ${s.is_active})`));
      }
      return;
    }
    
    // ================================
    // SCHEDULE LOOP CONFIGURATION
    // ================================
    // Set to true: After completing all schedules, loop back to first schedule
    // Set to false: After completing all schedules, stop sending (no more assignments)
    // 
    // Example scenarios:
    // true:  20→40→60→80→20→40... (infinite loop)
    // false: 20→40→60→80→STOP (one-time completion)
    const LOOP_SCHEDULES_AFTER_COMPLETION = true;
    
    // Calculate average gap between schedules for timing logic
    let totalGap = 0;
    let gapCount = 0;
    for (let i = 1; i < schedules.length; i++) {
      const gap = Number(schedules[i].days) - Number(schedules[i-1].days);
      totalGap += gap;
      gapCount++;
    }
    const averageGap = gapCount > 0 ? Math.ceil(totalGap / gapCount) : 7; // Default 7 days if no gaps
    
    console.log(`[Schedule Debug] Found ${schedules.length} schedules, average gap: ${averageGap} days`);
    
    // Helper function to get next template from sequence
    function getNextTemplateFromSequence(patient, templateSequence) {
      if (!templateSequence || !templateSequence.steps || templateSequence.steps.length === 0) {
        return null; // No sequence defined, use default logic
      }
      
      const lastTemplate = patient.last_template || patient.lastTemplate || '';
      const lastScheduleDays = Number(patient.last_schedule_days) || 0;
      
      // console.log(`🔍 [Sched Template Logic] Patient: ${patient.name || patient.unique_id}, Last Template: "${lastTemplate}", Last Step: ${lastScheduleDays}`);
      
      // Case 1: If last_schedule_days = 0 or last_template = 'welcome', start from sequence step 1
      if (lastScheduleDays === 0 || lastTemplate.toLowerCase() === 'welcome') {
        const firstStep = templateSequence.steps[0];
        if (firstStep) {
          const firstStepNumber = firstStep.stepNumber || 1;
          //console.log(`📋 [Sched Template Logic] Starting from first template: ${firstStep.templateName} (Step ${firstStepNumber})`);
          return {
            templateName: firstStep.templateName,
            sequenceStep: firstStepNumber
          };
        }
      }
      
      // Case 2: Based on last step number, find next available template
      if (lastScheduleDays > 0) {
        // Look for next step number that exists in sequence
        for (let stepNum = lastScheduleDays + 1; stepNum <= lastScheduleDays + 10; stepNum++) {
          const stepTemplate = templateSequence.steps.find(step => step.stepNumber === stepNum);
          if (stepTemplate) {
            console.log(`📋 [Sched Template Logic] Found next step ${stepNum}: ${stepTemplate.templateName} (skipped missing steps)`);
            return {
              templateName: stepTemplate.templateName,
              sequenceStep: stepNum
            };
          }
        }
        
        // If no higher step found, loop back to first (if looping enabled)
        if (LOOP_SCHEDULES_AFTER_COMPLETION) {
          const firstStep = templateSequence.steps[0];
          if (firstStep) {
            const firstStepNumber = firstStep.stepNumber || 1;
            console.log(`📋 [Sched Template Logic] No higher steps found, looping to first: ${firstStep.templateName} (Step ${firstStepNumber})`);
            return {
              templateName: firstStep.templateName,
              sequenceStep: firstStepNumber
            };
          }
        } else {
          console.log(`📋 [Sched Template Logic] No higher steps found, sequence completed`);
          return null; // Stop sending
        }
      }
      
      // Case 3: Find current template in sequence and move to next available
      const currentStepIndex = templateSequence.steps.findIndex(step => 
        step.templateName === lastTemplate
      );
      
      if (currentStepIndex >= 0) {
        const currentStep = templateSequence.steps[currentStepIndex];
        const currentStepNumber = currentStep.stepNumber || (currentStepIndex + 1);
        
        console.log(`📋 [Sched Template Logic] Found current template "${lastTemplate}" at position ${currentStepIndex}, step number ${currentStepNumber}`);
        
        // Look for next available step number
        for (let stepNum = currentStepNumber + 1; stepNum <= currentStepNumber + 10; stepNum++) {
          const nextStepTemplate = templateSequence.steps.find(step => step.stepNumber === stepNum);
          if (nextStepTemplate) {
            console.log(`📋 [Sched Template Logic] Found next available step ${stepNum}: ${nextStepTemplate.templateName}`);
            return {
              templateName: nextStepTemplate.templateName,
              sequenceStep: stepNum
            };
          }
        }
        
        // If no next step found, try next index in array
        const nextStepIndex = currentStepIndex + 1;
        if (nextStepIndex < templateSequence.steps.length) {
          const nextStep = templateSequence.steps[nextStepIndex];
          const nextStepNumber = nextStep.stepNumber || (nextStepIndex + 1);
          console.log(`📋 [Sched Template Logic] Using next array position: ${nextStep.templateName} (Step ${nextStepNumber})`);
          return {
            templateName: nextStep.templateName,
            sequenceStep: nextStepNumber
          };
        } else {
          // Reached end of sequence, loop back to first or stop
          if (LOOP_SCHEDULES_AFTER_COMPLETION) {
            const firstStep = templateSequence.steps[0];
            const firstStepNumber = firstStep.stepNumber || 1;
            console.log(`📋 [Sched Template Logic] End of sequence, looping to first: ${firstStep.templateName} (Step ${firstStepNumber})`);
            return {
              templateName: firstStep.templateName,
              sequenceStep: firstStepNumber
            };
          } else {
            console.log(`📋 [Sched Template Logic] Completed sequence, stopping`);
            return null; // Stop sending
          }
        }
      } else {
        // Current template not found in sequence, start from beginning
        const firstStep = templateSequence.steps[0];
        if (firstStep) {
          const firstStepNumber = firstStep.stepNumber || 1;
          console.log(`📋 [Sched Template Logic] Template not found in sequence, starting from first: ${firstStep.templateName} (Step ${firstStepNumber})`);
          return {
            templateName: firstStep.templateName,
            sequenceStep: firstStepNumber
          };
        }
      }
      
      return null;
    }
    
    const results = [];

    // Fetch candidate patients: active, not deleted, not DND, with a last_visited date
    // Fetch ALL candidate patients: active, not deleted, not DND, with a last_visited date
    // Order by Last_Msgsent_date ASC - oldest messages first (जिसका message भेजे हुए सबसे ज्यादा दिन हो गए)
    const patientsAll = db.prepare(`
      SELECT * FROM patients 
      WHERE is_active=1 AND is_Deleted=0 AND Is_DND = 0 AND last_visited IS NOT NULL 
      ORDER BY 
        CASE 
          WHEN Last_Msgsent_date IS NULL OR Last_Msgsent_date = '' THEN 1 
          ELSE 0 
        END ASC,
        Last_Msgsent_date ASC
    `).all();

    console.log(`[Schedule Debug] Found ${patientsAll.length} candidate patients`);
    if (patientsAll.length === 0) {
      // Check what patients exist
      const totalPatients = db.prepare('SELECT COUNT(*) as count FROM patients').get();
      const activePatients = db.prepare('SELECT COUNT(*) as count FROM patients WHERE is_active=1').get();
      const notDeletedPatients = db.prepare('SELECT COUNT(*) as count FROM patients WHERE is_active=1 AND is_Deleted=0').get();
      const notDndPatients = db.prepare('SELECT COUNT(*) as count FROM patients WHERE is_active=1 AND is_Deleted=0 AND Is_DND=0').get();
      const withVisitPatients = db.prepare('SELECT COUNT(*) as count FROM patients WHERE is_active=1 AND is_Deleted=0 AND Is_DND=0 AND last_visited IS NOT NULL').get();
      
      console.log(`[Schedule Debug] Patient counts:`);
      console.log(`  Total: ${totalPatients.count}`);
      console.log(`  Active: ${activePatients.count}`);
      console.log(`  Not deleted: ${notDeletedPatients.count}`);
      console.log(`  Not DND: ${notDndPatients.count}`);
      console.log(`  With visit date: ${withVisitPatients.count}`);
    }

    // Get list of active templates for validation
    const activeTemplates = new Set();
    try {
      const templateRows = db.prepare('SELECT name FROM templates WHERE is_delete = 0').all();
      templateRows.forEach(t => activeTemplates.add(t.name));
      console.log(`[Schedule Debug] Active templates: ${Array.from(activeTemplates).join(', ')}`);
    } catch (e) {
      // ignore template loading errors
    }

    // NEW LOGIC: Each patient should appear in only ONE schedule (the next applicable one)
    // Progressive order: must send 10-day before 20-day, etc.
    // Gap checking: ensure enough time has passed since last message
    const now = dayjs();
    
    // Initialize results for all schedules
    for (const s of schedules) {
      const thresholdDays = Number(s.days) || 0;
      results.push({ 
        schedule: s.name, 
        templateName: s.templateName, 
        days: thresholdDays, 
        profiles: (() => { try { return JSON.parse(s.profiles || '[]'); } catch { return []; } })(), 
        patients: [] 
      });
    }

    // Process each patient to determine which schedule they should receive next
    console.log(`[Schedule Debug] Starting to process ${patientsAll.length} patients...`);
    let processedCount = 0;
    let assignedCount = 0;
    
    for (const p of patientsAll) {
      processedCount++;
      // Reduced logging - only show every 1000 patients processed
      if (processedCount % 1000 === 0) {
        console.log(`[Schedule Debug] Processing patient ${processedCount}/${patientsAll.length}`);
      }
      try {
        const lv = p.last_visited;
        if (!lv) {
          continue;
        }
        
        const daysSinceVisit = now.diff(dayjs(lv), 'day');
        const lastMsgDate = p.Last_Msgsent_date || p.last_msgsent_date || p.lastMsgsentDate || null;
        const lastScheduleDays = Number(p.last_schedule_days) || 0;
        const lastTemplate = p.last_template || p.lastTemplate || '';
        
        // Check if we should use template sequence logic
        let nextTemplateInfo = null;
        if (templateSequence) {
          nextTemplateInfo = getNextTemplateFromSequence(p, templateSequence);
        }
        
        // Case 1: No previous message or no last_schedule_days - start from first applicable schedule
        if (!lastMsgDate || String(lastMsgDate).trim() === '' || lastScheduleDays === 0) {
          // If we have a template sequence and should use it
          if (nextTemplateInfo) {
            // Find a schedule that uses the next template from sequence
            for (let i = 0; i < schedules.length; i++) {
              const thresholdDays = Number(schedules[i].days) || 0;
              if (daysSinceVisit >= thresholdDays) {
                // Override the schedule's template with the sequence template
                const modifiedResult = { ...results[i] };
                modifiedResult.templateName = nextTemplateInfo.templateName;
                modifiedResult.sequenceStep = nextTemplateInfo.sequenceStep;
                modifiedResult.patients.push(p);
                
                // Replace the original result with modified one
                results[i] = modifiedResult;
                assignedCount++;
                break; // Only add to ONE schedule
              }
            }
          } else {
            // Use default logic
            for (let i = 0; i < schedules.length; i++) {
              const thresholdDays = Number(schedules[i].days) || 0;
              if (daysSinceVisit >= thresholdDays) {
                results[i].patients.push(p);
                assignedCount++;
                break; // Only add to ONE schedule
              }
            }
          }
          continue;
        }

        // Case 2: Patient has previous message history - check visit vs message timing
        try {
          const lastMsgDateStr = String(lastMsgDate).trim();
          let lastMsg;
          
          // Parse the last message date
          if (lastMsgDateStr.includes('T') || lastMsgDateStr.includes(' ')) {
            lastMsg = dayjs(lastMsgDateStr);
          } else if (lastMsgDateStr.match(/^\d{4}-\d{2}-\d{2}$/)) {
            lastMsg = dayjs(lastMsgDateStr, 'YYYY-MM-DD');
          } else {
            lastMsg = dayjs(lastMsgDateStr);
          }
          
          if (!lastMsg.isValid()) {
            // If we have a template sequence and should use it
            if (nextTemplateInfo) {
              // Find a schedule that uses the next template from sequence
              for (let i = 0; i < schedules.length; i++) {
                const thresholdDays = Number(schedules[i].days) || 0;
                if (daysSinceVisit >= thresholdDays) {
                  // Override the schedule's template with the sequence template
                  const modifiedResult = { ...results[i] };
                  modifiedResult.templateName = nextTemplateInfo.templateName;
                  modifiedResult.sequenceStep = nextTemplateInfo.sequenceStep;
                  modifiedResult.patients.push(p);
                  
                  // Replace the original result with modified one
                  results[i] = modifiedResult;
                  break;
                }
              }
            } else {
              // Use default logic
              for (let i = 0; i < schedules.length; i++) {
                const thresholdDays = Number(schedules[i].days) || 0;
                if (daysSinceVisit >= thresholdDays) {
                  results[i].patients.push(p);
                  break;
                }
              }
            }
            continue;
          }
          
          const lastVisit = dayjs(lv);
          const daysSinceLastMsg = now.diff(lastMsg, 'day');
          
          //console.log(`[Schedule Debug] ${p.name || p.unique_id} - LastMsg: ${lastMsg.format('YYYY-MM-DD')}, LastVisit: ${lastVisit.format('YYYY-MM-DD')}, DaysSinceLastMsg: ${daysSinceLastMsg}`);
          
          // Check if patient visited AFTER the last message was sent
          if (lastVisit.isAfter(lastMsg, 'day')) {
            // Patient visited after last message - RESTART from first schedule
            // Check if we should use template sequence logic
            if (nextTemplateInfo) {
              // Find a schedule that uses the next template from sequence
              for (let i = 0; i < schedules.length; i++) {
                const thresholdDays = Number(schedules[i].days) || 0;
                if (daysSinceVisit >= thresholdDays) {
                  // Override the schedule's template with the sequence template
                  const modifiedResult = { ...results[i] };
                  modifiedResult.templateName = nextTemplateInfo.templateName;
                  modifiedResult.sequenceStep = nextTemplateInfo.sequenceStep;
                  modifiedResult.patients.push(p);
                  
                  // Replace the original result with modified one
                  results[i] = modifiedResult;
                  assignedCount++;
                  break;
                }
              }
            } else {
              // Use default logic
              for (let i = 0; i < schedules.length; i++) {
                const thresholdDays = Number(schedules[i].days) || 0;
                if (daysSinceVisit >= thresholdDays) {
                  results[i].patients.push(p);
                  assignedCount++;
                  break;
                }
              }
            }
          } else {
            // Patient did NOT visit after last message - CONTINUE progression
            
            // Check if we should use template sequence logic for progression
            if (nextTemplateInfo) {
              // Find a schedule that can accommodate the next template from sequence
              for (let i = 0; i < schedules.length; i++) {
                const thresholdDays = Number(schedules[i].days) || 0;
                const daysSinceLastMsg = now.diff(lastMsg, 'day');
                
                // Check if patient qualifies and enough time has passed
                if (daysSinceVisit >= thresholdDays && daysSinceLastMsg >= averageGap) {
                  // Override the schedule's template with the sequence template
                  const modifiedResult = { ...results[i] };
                  modifiedResult.templateName = nextTemplateInfo.templateName;
                  modifiedResult.sequenceStep = nextTemplateInfo.sequenceStep;
                  modifiedResult.patients.push(p);
                  
                  // Replace the original result with modified one
                  results[i] = modifiedResult;
                  assignedCount++;
                  break;
                }
              }
            } else {
              // Use default schedule progression logic
              // Find the next schedule in progression based on last_schedule_days
              let nextScheduleIndex = -1;
              for (let i = 0; i < schedules.length; i++) {
                const scheduleDays = Number(schedules[i].days) || 0;
                if (scheduleDays > lastScheduleDays) {
                  nextScheduleIndex = i;
                  break;
                }
              }
              
              // If no next schedule found (completed all schedules), check configuration
              if (nextScheduleIndex === -1) {
                if (LOOP_SCHEDULES_AFTER_COMPLETION) {
                  // Loop back to first schedule
                  nextScheduleIndex = 0;
                  console.log(`[Schedule Debug] Patient completed all schedules, looping back to first (${schedules[0].name})`);
                } else {
                  // Stop here - patient has completed all schedules and looping is disabled
                  console.log(`[Schedule Debug] Patient completed all schedules, stopping (loop disabled)`);
                  continue; // Skip this patient - no more assignments
                }
              }
              
              const nextSchedule = schedules[nextScheduleIndex];
              const nextThresholdDays = Number(nextSchedule.days) || 0;
              
              // Check two conditions:
              // 1. Patient qualifies for the next schedule based on visit time (daysSinceVisit >= nextThresholdDays)
              // 2. Enough gap time has passed since last message (daysSinceLastMsg >= averageGap)
              
              const qualifiesForNext = daysSinceVisit >= nextThresholdDays;
              
              // Calculate gap requirement - handle loop-back scenario
              let gapRequired;
              if (nextScheduleIndex === 0 && lastScheduleDays > 0) {
                // Looping back to first schedule - use average gap as minimum wait time
                gapRequired = averageGap;
              } else {
                // Normal progression - gap is difference between current and last schedule
                gapRequired = averageGap; // Use consistent average gap for all transitions
              }
              
              const enoughGapTime = daysSinceLastMsg >= gapRequired;
              
              if (qualifiesForNext && enoughGapTime) {
                results[nextScheduleIndex].patients.push(p);
                assignedCount++;
              }
            }
          }
        } catch (e) {
          // Error parsing dates - start from first applicable schedule
          // If we have a template sequence and should use it
          if (nextTemplateInfo) {
            // Find a schedule that uses the next template from sequence
            for (let i = 0; i < schedules.length; i++) {
              const thresholdDays = Number(schedules[i].days) || 0;
              if (daysSinceVisit >= thresholdDays) {
                // Override the schedule's template with the sequence template
                const modifiedResult = { ...results[i] };
                modifiedResult.templateName = nextTemplateInfo.templateName;
                modifiedResult.sequenceStep = nextTemplateInfo.sequenceStep;
                modifiedResult.patients.push(p);
                
                // Replace the original result with modified one
                results[i] = modifiedResult;
                assignedCount++;
                break;
              }
            }
          } else {
            // Use default logic
            for (let i = 0; i < schedules.length; i++) {
              const thresholdDays = Number(schedules[i].days) || 0;
              if (daysSinceVisit >= thresholdDays) {
                results[i].patients.push(p);
                assignedCount++;
                break;
              }
            }
          }
        }
      } catch (e) {
        // ignore per-patient errors
      }
    }

    // Debug summary of schedule assignments and apply sorting/limiting
    console.log(`\n[Schedule Debug] Processing Summary:`);
    console.log(`  Processed: ${processedCount} patients`);
    console.log(`  Assigned: ${assignedCount} patients`);
    console.log('\n[Schedule Debug] Assignment Summary:');
    results.forEach((result, index) => {
      console.log(`  Schedule ${index} (${result.schedule}): ${result.patients.length} patients`);
      
      // Sort patients by Last_Msgsent_date ASC (oldest messages first) and limit to 500
      if (result.patients && result.patients.length > 0) {
        result.patients.sort((a, b) => {
          const lastMsgA = a.Last_Msgsent_date || a.last_msgsent_date || a.lastMsgsentDate || '';
          const lastMsgB = b.Last_Msgsent_date || b.last_msgsent_date || b.lastMsgsentDate || '';
          
          // Handle null/empty dates - put them at the beginning (they need messages first)
          if (!lastMsgA && !lastMsgB) return 0;
          if (!lastMsgA) return -1;  // A goes to beginning (no message = highest priority)
          if (!lastMsgB) return 1;   // B goes to beginning
          
          // Convert to timestamps for comparison
          try {
            const timestampA = new Date(lastMsgA).getTime();
            const timestampB = new Date(lastMsgB).getTime();
            
            // Handle invalid dates
            if (isNaN(timestampA) && isNaN(timestampB)) return 0;
            if (isNaN(timestampA)) return -1;  // A goes to beginning
            if (isNaN(timestampB)) return 1;   // B goes to beginning
            
            // Sort in ascending order (oldest messages first - जिसका message भेजे हुए ज्यादा दिन हो गए)
            return timestampA - timestampB;
          } catch (e) {
            return 0;
          }
        });
        
        // Limit to 500 patients per schedule
        if (result.patients.length > 500) {
          result.originalCount = result.patients.length;
          result.patients = result.patients.slice(0, 500);
          console.log(`    Limited to 500 patients (was ${result.originalCount})`);
        }
      }
    });
    console.log('');

    const win = new BrowserWindow({
      width: 1000,
      height: 700,
      parent: mainWin,
      webPreferences: {
        nodeIntegration: true,
        contextIsolation: false
      }
    });
    win.removeMenu();
    win.loadFile('scheduledResults.html');
    win.webContents.once('did-finish-load', () => {
      win.webContents.send('scheduled-results-data', results);
    });
  } catch (e) {
    console.error('open-scheduled-results error:', e.message);
  }
});

ipcMain.on("save-template", (event, { name, message, editing }) => {
  // Validate template name
  if (!name || name.trim() === '') {
    event.reply("template-error", "Template name cannot be empty");
    return;
  }

  const trimmedName = name.trim();
  
  // When sendOption/afterDays are disabled in the UI we keep template persistence simple.
  let templates = loadTemplates();
  let template = templates.find(t => t.name === trimmedName && !t.is_delete);

  if (editing && template) {
    // Editing existing template
    template.message = message;
    template.modified = getLocalISOString();
  } else if (editing && !template) {
    // Trying to edit a template that doesn't exist
    event.reply("template-error", "Template not found for editing");
    return;
  } else {
    // Adding new template - check for uniqueness (case-insensitive)
    const existingTemplate = templates.find(t => 
      t.name.toLowerCase() === trimmedName.toLowerCase() && !t.is_delete
    );
    
    if (existingTemplate) {
      // If the case differs, show both names in the error message for clarity
      if (existingTemplate.name !== trimmedName) {
        event.reply("template-error", `Template name "${trimmedName}" conflicts with existing template "${existingTemplate.name}" (case-insensitive match). Please choose a different name.`);
      } else {
        event.reply("template-error", `Template name "${trimmedName}" already exists. Please choose a different name.`);
      }
      return;
    }
    
    templates.push({
      name: trimmedName,
      message,
      // keep legacy fields at defaults in DB when upsert runs
      is_delete: 0,
      created: getLocalISOString(),
      modified: getLocalISOString()
    });
  }

  saveTemplates(templates);
  event.reply("template-success", trimmedName);
});

ipcMain.on("delete-template", (event, name) => {
  let templates = loadTemplates();
  let template = templates.find(t => t.name === name);
  if (template) {
    template.is_delete = 1;
    template.modified = getLocalISOString();
    saveTemplates(templates);
    event.reply("delete-template-success", name);
  }
  else {
    event.reply("delete-template-failed", name);
  }
});

// ✅ Start WhatsApp client
ipcMain.on("start-profile", (event, profileName) => {
  try {
    // Check saved license info for profile limit
    let settings = {};
    try { settings = loadSettings() || {}; } catch (e) { settings = {}; }
    const licenseInfo = settings.licenseInfo || null;
    let limit = null; // null -> no limit
    if (licenseInfo && licenseInfo.profileCount !== undefined && licenseInfo.profileCount !== null) {
      const pc = licenseInfo.profileCount;
      if (typeof pc === 'number' && isFinite(pc)) limit = Number(pc);
      else if (typeof pc === 'string') {
        const s = pc.trim().toLowerCase();
        if (s.includes('no')) limit = null;
        else if (!isNaN(Number(s))) limit = Number(s);
      }
    }

    // Count current active profiles
    let currentCount = 0;
    try {
      if (typeof db !== 'undefined' && db) {
        const row = db.prepare('SELECT COUNT(*) as cnt FROM profiles WHERE is_active = 1 AND is_delete = 0').get();
        currentCount = row ? (row.cnt || 0) : 0;
      } else {
        const profiles = loadProfiles();
        currentCount = (profiles || []).filter(p => p.is_active === 1 || p.is_active === true || p.is_active === undefined).length;
      }
    } catch (e) {
      console.warn('Failed to count profiles for limit check:', e && e.message ? e.message : e);
      currentCount = 0;
    }

    // Enforce limit when finite
    if (limit !== null && typeof limit === 'number' && isFinite(limit)) {
      if (currentCount >= limit) {
        try { event.reply('start-profile-failed', { message: `Profile limit reached (${currentCount}/${limit})` }); } catch (e) {}
        return;
      }
    }

    // Validate profile name before starting client
    const validation = validateProfileName(profileName);
    if (!validation.isValid) {
      try { 
        event.reply('start-profile-failed', { 
          message: `Invalid profile name: ${validation.errors.join(', ')}` 
        }); 
      } catch (e) {}
      return;
    }

    // Allowed: start client
    startWhatsAppClient(profileName, event);
  } catch (err) {
    console.error('start-profile handler error:', err && err.message ? err.message : err);
    try { event.reply('start-profile-failed', { message: 'Failed to start profile' }); } catch (e) {}
  }
});
// Keep all active clients in memory
const activeClients = {};  
// Map profileName -> Set of jobIds currently using that profile
const profileJobs = {};

// Debug IPC: return list of active client names and basic info
ipcMain.handle('get-active-clients', () => {
  try {
    const names = Object.keys(activeClients);
    const info = names.map(n => ({ name: n, info: activeClients[n]?.info || null }));
    return { count: names.length, clients: info };
  } catch (e) {
    console.error('get-active-clients error:', e.message);
    return { count: 0, clients: [] };
  }
});

// Open an Excel file (or any file) using the OS default program
ipcMain.handle('open-excel', async (event, filePath) => {
  try {
    if (!filePath || !fs.existsSync(filePath)) {
      return { success: false, error: 'File does not exist' };
    }
    const result = await shell.openPath(filePath);
    if (result) {
      // openPath returns an empty string on success, or an error string on failure
      return { success: false, error: result };
    }
    return { success: true };
  } catch (e) {
    console.error('open-excel error:', e.message);
    return { success: false, error: e.message };
  }
});

// Save the report (Excel file) to a user-selected path (Save As)
ipcMain.handle('save-excel-as', async (event, srcPath) => {
  try {
    if (!srcPath || !fs.existsSync(srcPath)) {
      return { success: false, error: 'Source file does not exist' };
    }

    const defaultName = path.basename(srcPath);
    const { canceled, filePath: destPath } = await dialog.showSaveDialog({
      title: 'Save report as',
      defaultPath: defaultName,
      buttonLabel: 'Save'
    });

    if (canceled || !destPath) {
      return { success: false, error: 'Save canceled by user' };
    }

    await fs.promises.copyFile(srcPath, destPath);
    return { success: true, path: destPath };
  } catch (e) {
    console.error('save-excel-as error:', e.message);
    return { success: false, error: e.message };
  }
});

ipcMain.on("delete-profile", async (event, profileName) => {
  try {
    // 1. Update DB row if DB present
    if (typeof db !== 'undefined' && db) {
      const now = getLocalISOString();
      const res = db.prepare('UPDATE profiles SET is_active=0, is_delete=1, modified=? WHERE name = ?').run(now, profileName);

      // Destroy active client if exists
      if (activeClients[profileName]) {
        try {
          await activeClients[profileName].destroy();
          delete activeClients[profileName];
          console.log(`❌ Client ${profileName} destroyed`);
        } catch (e) {
          console.warn(`⚠️ Error destroying client ${profileName}:`, e.message);
          delete activeClients[profileName]; // Still remove from activeClients
        }
      }

      // Delete LocalAuth session folder
      try {
        const authBase = path.join(app.getPath('userData'), '.wwebjs_auth');
        const sessionPath = path.join(authBase, `session-${profileName}`);
        if (fs.existsSync(sessionPath)) {
          fs.rmSync(sessionPath, { recursive: true, force: true });
          console.log(`🗑️ Session folder removed: ${sessionPath}`);
        }
      } catch (e) {
        console.error('Failed to remove session from userData path:', e.message);
      }

      if (res.changes > 0) event.reply('delete-profile-success', profileName);
      else event.reply('delete-profile-failed', profileName);
      return;
    }

    // Fallback: JSON-based deletion
    let profiles = loadProfiles();
    let profile = profiles.find(p => p.name === profileName);

    if (profile) {
      profile.is_active = 0;
      profile.is_delete = 1;
      profile.modified = getLocalISOString();
      saveProfiles(profiles);

      if (activeClients[profileName]) {
        try {
          await activeClients[profileName].destroy();
          delete activeClients[profileName];
          console.log(`❌ Client ${profileName} destroyed`);
        } catch (e) {
          console.warn(`⚠️ Error destroying client ${profileName}:`, e.message);
          delete activeClients[profileName]; // Still remove from activeClients
        }
      }

      try {
        const authBase = path.join(app.getPath('userData'), '.wwebjs_auth');
        const sessionPath = path.join(authBase, `session-${profileName}`);
        if (fs.existsSync(sessionPath)) {
          fs.rmSync(sessionPath, { recursive: true, force: true });
          console.log(`🗑️ Session folder removed: ${sessionPath}`);
        }
      } catch (e) {
        console.error('Failed to remove session from userData path:', e.message);
      }

      event.reply('delete-profile-success', profileName);
    } else {
      event.reply('delete-profile-failed', profileName);
    }
  } catch (e) {
    console.error('delete-profile error:', e.message);
    event.reply('delete-profile-failed', profileName);
  }
});

ipcMain.on("verify-profile", async (event, profileName) => {
  try {
    // First check internet connectivity
    console.log(`🔍 Checking internet connectivity before verifying profile: ${profileName}`);
    const connectivityResult = await checkInternetConnectivity();
    
    if (!connectivityResult.connected) {
      console.log(`❌ No internet connection - cannot verify profile: ${profileName}`);
      event.reply('verify-result', { 
        profileName, 
        status: 'no-internet', 
        error: connectivityResult.message 
      });
      return;
    }
    
    //console.log(`✅ Internet connectivity confirmed, proceeding with profile verification: ${profileName}`);

    // Prefer DB lookup
    let profile = null;
    if (typeof db !== 'undefined' && db) {
      profile = db.prepare('SELECT * FROM profiles WHERE name = ? AND is_delete = 0').get(profileName);
    } else {
      const profiles = loadProfiles();
      profile = profiles.find(p => p.name === profileName);
    }

    if (!profile || !profile.is_active) {
      event.reply('verify-result', { profileName, status: 'inactive' });
      return;
    }

    // Try reconnecting a lightweight client using userData LocalAuth
    const client = new Client({
      authStrategy: new LocalAuth({ clientId: profileName, dataPath: path.join(app.getPath('userData'), '.wwebjs_auth') })
    });

    let isReady = false;
    let isDestroyed = false;

    const cleanup = () => {
      if (!isDestroyed) {
        isDestroyed = true;
        try {
          client.destroy();
        } catch (e) {
          console.error('Error destroying verification client:', e.message);
        }
      }
    };

    client.on("ready", () => {
        if (!isDestroyed) {
          isReady = true;
          event.reply("verify-result", { profileName, status: "active" });
          cleanup();
        }
    });

    client.on("disconnected", () => {
        if (!isDestroyed) {
          event.reply("verify-result", { profileName, status: "disconnected" });
          cleanup();
        }
    });

    client.on("auth_failure", () => {
        if (!isDestroyed) {
          event.reply("verify-result", { profileName, status: "auth_failure" });
          cleanup();
        }
    });

    // Handle client errors
    client.on("error", (error) => {
        if (!isDestroyed) {
          console.error(`Verification client error for ${profileName}:`, error.message);
          event.reply("verify-result", { profileName, status: "error", error: error.message });
          cleanup();
        }
    });

    setTimeout(() => {
        if (!isReady && !isDestroyed) {
            event.reply("verify-result", { profileName, status: "timeout" });
            cleanup();
        }
    }, 5000);

    // Initialize with proper error handling
    try {
      await client.initialize();
    } catch (initError) {
      if (!isDestroyed) {
        console.error(`Failed to initialize verification client for ${profileName}:`, initError.message);
        event.reply("verify-result", { profileName, status: "error", error: initError.message });
        cleanup();
      }
    }
  } catch (e) {
    console.error('verify-profile error:', e.message);
    event.reply('verify-result', { profileName, status: 'error', error: e.message });
  }
});

// Validate WhatsApp numbers for a given profile
ipcMain.handle('validate-whatsapp-numbers', async (event, { profileName, phoneNumbers }) => {
  try {
    const client = activeClients[profileName];
    if (!client) {
      return { success: false, error: `Profile ${profileName} is not logged in` };
    }

    // Ensure phone numbers is an array
    const numbers = Array.isArray(phoneNumbers) ? phoneNumbers : [phoneNumbers];
    
    console.log(`🔍 Starting WhatsApp validation for ${numbers.length} numbers using profile: ${profileName}`);
    
    const results = await validateWhatsAppNumbers(client, numbers);
    
    console.log(`✅ Validation complete: ${results.valid.length} valid, ${results.invalid.length} invalid, ${results.errors.length} errors`);
    
    return {
      success: true,
      results: {
        valid: results.valid,
        invalid: results.invalid,
        errors: results.errors,
        summary: {
          total: numbers.length,
          validCount: results.valid.length,
          invalidCount: results.invalid.length,
          errorCount: results.errors.length
        }
      }
    };
    
  } catch (error) {
    console.error('validate-whatsapp-numbers error:', error.message);
    return { success: false, error: error.message };
  }
});

// Parse Excel file for wizard
ipcMain.on('parse-excel-file', (event, { fileName, buffer, columnMappings, headerRowIndex }) => {
  try {
    // Create uploadedexcel directory in user data directory
    const dir = getUserDataDir();
    const uploadDir = path.join(dir, 'uploadedexcel');
    if (!fs.existsSync(uploadDir)) {
      fs.mkdirSync(uploadDir, { recursive: true });
    }
    
    // Save original file
    const timestamp = Date.now();
    const originalFileName = `${timestamp}_${fileName}`;
    const originalFilePath = path.join(uploadDir, originalFileName);
    fs.writeFileSync(originalFilePath, buffer);
    
    // Parse Excel file using XLSX library
    const workbook = XLSX.read(buffer, { type: 'buffer' });
    
    // Get first worksheet
    const firstSheetName = workbook.SheetNames[0];
    const worksheet = workbook.Sheets[firstSheetName];
    
    // Convert to array of arrays
    const rawData = XLSX.utils.sheet_to_json(worksheet, { 
      header: 1,
      defval: '',
      blankrows: false 
    });
    
    // Filter out completely empty rows
    const filteredData = rawData.filter(row => 
      row.some(cell => cell !== null && cell !== undefined && cell.toString().trim() !== '')
    );
    
    // If columnMappings provided, create mapped data
    let mappedData = null;
    if (columnMappings && headerRowIndex) {
      const headers = Object.keys(columnMappings);
      mappedData = [];
      // Add headers as first row
      mappedData.push(headers);
      // Map data rows
      for (let i = headerRowIndex; i < filteredData.length; i++) {
        const mappedRow = [];
        headers.forEach(field => {
          const colIndex = columnMappings[field];
          mappedRow.push(filteredData[i][colIndex] || '');
        });
        mappedData.push(mappedRow);
      }
      // Deduplicate mappedData by Phone column (if present)
      const phoneIdx = headers.indexOf('Phone');
      if (phoneIdx !== -1) {
        const seen = new Set();
        const deduped = [headers];
        for (let i = 1; i < mappedData.length; i++) {
          let phone = mappedData[i][phoneIdx];
          if (phone) phone = phone.toString().trim();
          if (phone && !seen.has(phone)) {
            seen.add(phone);
            deduped.push(mappedData[i]);
          }
        }
        mappedData = deduped;
      }
      // Save mapped data as CSV for easy processing
      const mappedFileName = `${timestamp}_mapped.csv`;
      const mappedFilePath = path.join(uploadDir, mappedFileName);
      const csvContent = mappedData.map(row => 
        row.map(cell => `"${cell.toString().replace(/"/g, '""')}` ).join(',')
      ).join('\n');
      fs.writeFileSync(mappedFilePath, csvContent, 'utf-8');
      console.log(`💾 Saved mapped data: ${mappedFilePath}`);
    }
    
    console.log(`📊 Successfully parsed and saved Excel file: ${fileName}, ${filteredData.length} rows`);
    console.log(`💾 Original file saved: ${originalFilePath}`);
    
    event.reply('parse-excel-file-response', {
      success: true,
      data: filteredData,
      mappedData: mappedData,
      originalFilePath: originalFilePath,
      mappedFilePath: mappedData ? path.join(uploadDir, `${timestamp}_mapped.csv`) : null
    });
    
  } catch (error) {
    console.error('❌ Error parsing Excel file:', error);
    event.reply('parse-excel-file-response', {
      success: false,
      error: error.message
    });
  }
});

// Pre-validate WhatsApp numbers from Excel file
ipcMain.handle('validate-excel-numbers', async (event, { profileName, excelPath }) => {
  try {
    const client = activeClients[profileName];
    if (!client) {
      return { success: false, error: `Profile ${profileName} is not logged in` };
    }

    if (!fs.existsSync(excelPath)) {
      return { success: false, error: "Excel file not found" };
    }

    // Read numbers from Excel
    const workbook = XLSX.readFile(excelPath);
    const sheetName = workbook.SheetNames[0];
    const worksheet = workbook.Sheets[sheetName];
    const rows = XLSX.utils.sheet_to_json(worksheet);

    // Extract phone numbers
    const phoneNumbers = rows.map(row => {
      const raw = row.Phone || row.phone || row.number;
      return raw ? formatNumber(raw) : null;
    }).filter(Boolean);

    if (phoneNumbers.length === 0) {
      return { success: false, error: "No valid phone numbers found in Excel" };
    }

    console.log(`🔍 Validating ${phoneNumbers.length} numbers from Excel using profile: ${profileName}`);
    
    const results = await validateWhatsAppNumbers(client, phoneNumbers, 3); // Slower for Excel validation
    
    // Create detailed report
    const report = {
      total: phoneNumbers.length,
      valid: results.valid,
      invalid: results.invalid.map(inv => ({
        phone: inv.phone,
        reason: inv.reason
      })),
      errors: results.errors.map(err => ({
        phone: err.phone,
        error: err.error
      })),
      summary: {
        validCount: results.valid.length,
        invalidCount: results.invalid.length,
        errorCount: results.errors.length,
        validPercentage: Math.round((results.valid.length / phoneNumbers.length) * 100)
      }
    };

    console.log(`✅ Excel validation complete: ${report.summary.validPercentage}% valid (${report.summary.validCount}/${report.total})`);
    
    return { success: true, report };
    
  } catch (error) {
    console.error('validate-excel-numbers error:', error.message);
    return { success: false, error: error.message };
  }
});

let usePopupWin = null;

ipcMain.on('open-Sendmsg', () => {
  usePopupWin = new BrowserWindow({
    width: 1000,
    height: 700,
    modal: true,
    parent: mainWin, // your main window variable
    webPreferences: {
      nodeIntegration: true,
      contextIsolation: false,
    },
  });

  usePopupWin.removeMenu();
  const popupPath = path.join(__dirname, 'scheduleMsg.html');
  usePopupWin.loadFile(popupPath);
});

// NOTE: removed stray 'save-excelll-file' handler. Use the generic 'save-excel-file' / 'save-image-file' handlers below.

// Helper to get timestamped filename
function getTimestampedFilename(originalName) {
  const ext = path.extname(originalName);
  const baseName = path.basename(originalName, ext);
  const now = new Date();
  const datetimeFormatted = now.toISOString()
    .replace('T', '_')
    .replace(/:/g, '')
    .replace('Z', '')
    .replace(/-/g, '')
    .replace(/\./g, '');
  return `${baseName}_${datetimeFormatted}${ext}`;
}

// Generic save handler
function saveFile(event, fileData, folderName, responseChannel) {
  const { name, buffer } = fileData;

  // Save inside the user's writable app data folder so packaged app can write files
  const folderPath = path.join(getUserDataDir(), folderName);

  if (!fs.existsSync(folderPath)) {
    fs.mkdirSync(folderPath, { recursive: true });
  }

  const newName = getTimestampedFilename(name);
  const filePath = path.join(folderPath, newName);

  fs.writeFile(filePath, buffer, (err) => {
    if (err) {
      console.error(`Failed to save file (${name}):`, err);
      event.reply(responseChannel, false);
    } else {
      console.log(`File saved at: ${filePath}`);
      event.reply(responseChannel, {
      success: true,
      filename: newName,
      path: filePath
    });
    }
  });
}

// Listen for Excel file save requests
ipcMain.on('save-excel-file', (event, fileData) => {
  saveFile(event, fileData, 'uploadedexcel', 'save-excel-file-response');
});

// Listen for Image file save requests
ipcMain.on('save-image-file', (event, fileData) => {
  saveFile(event, fileData, 'uploadedImages', 'save-image-file-response');
});

// Backwards-compatible handler: some renderers send 'save-media-file'
// so respond on 'save-media-file-response' and save into same uploadedImages folder
ipcMain.on('save-media-file', (event, fileData) => {
  saveFile(event, fileData, 'uploadedImages', 'save-media-file-response');
});

// Comprehensive Profile Rename Handler
ipcMain.handle('rename-profile-comprehensive', async (event, { oldName, newName }) => {
  try {
    console.log(`🔄 Starting comprehensive profile rename: "${oldName}" → "${newName}"`);
    
    // Step 1: Validate inputs
    event.sender.send('profile-rename-progress', { step: 1, total: 7, message: 'Validating profile names...', progress: 10 });
    
    if (!oldName || !newName) {
      throw new Error('Both old and new profile names are required');
    }
    
    if (oldName.trim() === newName.trim()) {
      throw new Error('New profile name must be different from current name');
    }

    // Validate new profile name using global validation function
    const validation = validateProfileName(newName);
    if (!validation.isValid) {
      throw new Error(`Invalid profile name: ${validation.errors.join(', ')}`);
    }
    
    // Check if old profile exists
    const existingProfile = db.prepare('SELECT * FROM profiles WHERE name = ? AND is_active = 1').get(oldName);
    if (!existingProfile) {
      throw new Error(`Profile "${oldName}" not found or inactive`);
    }
    
    // Check if new name already exists
    const nameExists = db.prepare('SELECT COUNT(*) as count FROM profiles WHERE name = ? AND is_active = 1').get(newName);
    if (nameExists.count > 0) {
      throw new Error(`Profile name "${newName}" already exists`);
    }

    // Step 2: Pause active operations for this profile
    event.sender.send('profile-rename-progress', { step: 2, total: 7, message: 'Pausing active operations...', progress: 20 });
    
    // Pause any active jobs using this profile
    const activeJobs = Object.keys(jobControllers || {});
    for (const jobId of activeJobs) {
      if (jobControllers[jobId] && profileJobs[oldName] && profileJobs[oldName].has(jobId)) {
        jobControllers[jobId].paused = true;
        console.log(`⏸️ Paused job ${jobId} for profile rename`);
      }
    }

    // Step 3: Update profiles table
    event.sender.send('profile-rename-progress', { step: 3, total: 7, message: 'Updating profile database...', progress: 35 });
    
    const updateProfileStmt = db.prepare('UPDATE profiles SET name = ?, modified = ? WHERE name = ?');
    const profileResult = updateProfileStmt.run(newName, getLocalISOString(), oldName);
    
    if (profileResult.changes === 0) {
      throw new Error('Failed to update profile in database');
    }
    
    console.log(`✅ Updated profile table: ${profileResult.changes} record(s)`);

    // Step 4: Update patients table
    event.sender.send('profile-rename-progress', { step: 4, total: 7, message: 'Updating customer assignments...', progress: 50 });
    
    const updatePatientsStmt = db.prepare('UPDATE patients SET profile = ?, mod_date = ? WHERE profile = ?');
    const patientsResult = updatePatientsStmt.run(newName, getLocalISOString(), oldName);
    
    console.log(`✅ Updated patients table: ${patientsResult.changes} record(s)`);

    // Step 5: Update schedules table
    event.sender.send('profile-rename-progress', { step: 5, total: 7, message: 'Updating scheduled campaigns...', progress: 65 });
    
    const schedulesStmt = db.prepare('SELECT id, name, profiles FROM schedules WHERE profiles LIKE ?');
    const schedules = schedulesStmt.all(`%"${oldName}"%`);
    
    let schedulesUpdated = 0;
    for (const schedule of schedules) {
      try {
        let profilesArray = JSON.parse(schedule.profiles || '[]');
        const index = profilesArray.indexOf(oldName);
        if (index !== -1) {
          profilesArray[index] = newName;
          const updateScheduleStmt = db.prepare('UPDATE schedules SET profiles = ?, modified = ? WHERE id = ?');
          updateScheduleStmt.run(JSON.stringify(profilesArray), getLocalISOString(), schedule.id);
          schedulesUpdated++;
        }
      } catch (e) {
        console.warn(`Failed to update schedule ${schedule.name}:`, e.message);
      }
    }
    
    console.log(`✅ Updated schedules: ${schedulesUpdated} record(s)`);

    // Step 6: Update WhatsApp session folder
    event.sender.send('profile-rename-progress', { step: 6, total: 7, message: 'Updating WhatsApp session...', progress: 80 });
    
    // First, stop the active client if it exists to release session folder lock
    let clientWasActive = false;
    if (activeClients[oldName]) {
      try {
        clientWasActive = true;
        console.log(`⏸️ Stopping client ${oldName} before renaming session...`);
        await activeClients[oldName].destroy();
        delete activeClients[oldName];
        console.log(`✅ Client ${oldName} stopped for session rename`);
        
        // Wait a moment for file handles to be released
        await new Promise(resolve => setTimeout(resolve, 1000));
      } catch (e) {
        console.warn(`⚠️ Error stopping client ${oldName}:`, e.message);
        delete activeClients[oldName]; // Still remove from activeClients
      }
    }
    
    const userDataPath = getUserDataDir();
    const authBasePath = path.join(userDataPath, '.wwebjs_auth');
    const oldSessionPath = path.join(authBasePath, `session-${oldName}`);
    const newSessionPath = path.join(authBasePath, `session-${newName}`);
    
    let sessionUpdated = false;
    if (fs.existsSync(oldSessionPath)) {
      try {
        // Ensure target directory doesn't exist
        if (fs.existsSync(newSessionPath)) {
          fs.rmSync(newSessionPath, { recursive: true, force: true });
        }
        
        fs.renameSync(oldSessionPath, newSessionPath);
        sessionUpdated = true;
        console.log(`✅ Renamed session folder: ${oldSessionPath} → ${newSessionPath}`);
      } catch (e) {
        console.warn('Failed to rename session folder:', e.message);
        // Non-fatal error - continue with rename
      }
    }

    // Step 7: Update active clients cache and restart if needed
    event.sender.send('profile-rename-progress', { step: 7, total: 7, message: 'Finalizing changes...', progress: 95 });
    
    // If client was active before, we need to restart it with the new name
    // Note: activeClients[oldName] was already deleted when we stopped the client
    if (clientWasActive) {
      try {
        console.log(`🔄 Restarting client with new name: ${newName}`);
        // We'll start the client but won't wait for full initialization to avoid blocking
        startWhatsAppClient(newName, null); // Pass null for event to avoid reply
        console.log(`✅ Client restart initiated for ${newName}`);
      } catch (e) {
        console.warn(`⚠️ Failed to restart client ${newName}:`, e.message);
      }
    }
    
    // Update profile jobs mapping
    if (profileJobs[oldName]) {
      profileJobs[newName] = profileJobs[oldName];
      delete profileJobs[oldName];
    }

    // Resume paused jobs with new profile name
    for (const jobId of activeJobs) {
      if (jobControllers[jobId] && profileJobs[newName] && profileJobs[newName].has(jobId)) {
        jobControllers[jobId].paused = false;
        console.log(`▶️ Resumed job ${jobId} with new profile name`);
      }
    }

    // Step 8: Complete
    event.sender.send('profile-rename-progress', { step: 7, total: 7, message: 'Profile rename completed!', progress: 100 });
    
    const summary = [
      `• Profile database: ✅ Updated`,
      `• Customer assignments: ✅ ${patientsResult.changes} records updated`,
      `• Scheduled campaigns: ✅ ${schedulesUpdated} schedules updated`,
      `• WhatsApp session: ${sessionUpdated ? '✅ Renamed' : '⚠️ Not found or failed'}`,
      `• Active connections: ✅ Updated`
    ].join('\n');

    console.log(`✅ Profile rename completed successfully: "${oldName}" → "${newName}"`);

    return {
      success: true,
      message: `Profile "${oldName}" successfully renamed to "${newName}"`,
      summary: summary,
      stats: {
        profilesUpdated: profileResult.changes,
        patientsUpdated: patientsResult.changes,
        schedulesUpdated: schedulesUpdated,
        sessionUpdated: sessionUpdated
      }
    };

  } catch (error) {
    console.error('❌ Profile rename failed:', error);
    
    // Send error progress update
    event.sender.send('profile-rename-progress', { 
      step: -1, 
      total: 7, 
      message: `Error: ${error.message}`, 
      progress: 0 
    });

    return {
      success: false,
      error: error.message
    };
  }
});

// Download sample Excel template from userData samples folder
ipcMain.handle('download-sample-template', async (event, templateName) => {
  try {
    const userDataPath = getUserDataDir();
    const samplesPath = path.join(userDataPath, 'samples');
    const templatePath = path.join(samplesPath, templateName);
    
    console.log('📥 Download sample template request:', templateName);
    console.log('   Looking in:', samplesPath);
    
    // Check if template file exists
    if (!fs.existsSync(templatePath)) {
      console.error('❌ Template file not found:', templatePath);
      return { 
        success: false, 
        error: `Template file "${templateName}" not found in samples folder.\nExpected location: ${templatePath}` 
      };
    }
    
    // Read the file
    const fileBuffer = fs.readFileSync(templatePath);
    const fileStats = fs.statSync(templatePath);
    
    // Show save dialog
    const { canceled, filePath } = await dialog.showSaveDialog(mainWin || null, {
      title: 'Download Sample Template',
      defaultPath: path.join(require('os').homedir(), 'Desktop', templateName),
      buttonLabel: 'Download',
      filters: [
        { name: 'Excel Files', extensions: ['xlsx', 'xls'] },
        { name: 'All Files', extensions: ['*'] }
      ]
    });

    if (canceled || !filePath) {
      return { success: false, canceled: true };
    }

    // Ensure parent directory exists
    try { 
      fs.mkdirSync(path.dirname(filePath), { recursive: true }); 
    } catch (e) {}

    // Copy template to selected location
    fs.writeFileSync(filePath, fileBuffer);
    
    console.log('✅ Sample template downloaded successfully to:', filePath);
    
    return { 
      success: true, 
      filePath: filePath,
      fileName: templateName,
      fileSize: fileStats.size,
      message: `Template "${templateName}" downloaded successfully!`
    };
    
  } catch (error) {
    console.error('❌ Error downloading sample template:', error);
    return { 
      success: false, 
      error: `Failed to download template: ${error.message}` 
    };
  }
});

ipcMain.on("send-message-proc", async (event, data) => {
  // First check internet connectivity before starting message sending process
  console.log('🔍 Checking internet connectivity before sending messages...');
  const connectivityResult = await checkInternetConnectivity();
  
  if (!connectivityResult.connected) {
    console.log('❌ No internet connection - cannot send messages');
    event.reply('send-progress', {
      sent: 0,
      failed: 0,
      total: 0,
      completed: true,
      error: connectivityResult.message,
      currentProfile: 'none',
      logs: []
    });
    return;
  }
  
  // console.log('✅ Internet connectivity confirmed, proceeding with message sending');

  const profiles = data.profiles;
  // Normalize profiles: accept either array of names or array of objects { name }
  let requestedProfiles = Array.isArray(profiles) ? profiles.map(p => {
    if (!p) return null;
    if (typeof p === 'string') return p;
    if (typeof p === 'object') return p.name || p.name?.toString() || null;
    return String(p);
  }).filter(Boolean) : [];
  
  // Handle "All" profiles selection - use all active profiles
  if (requestedProfiles.includes('All') || requestedProfiles.length === 0) {
    requestedProfiles = Object.keys(activeClients || {});
    console.log(`Using all active profiles: ${requestedProfiles.join(', ')}`);
  }
  
  const profileName = data.profiles[0];
  const excelPath = data.excelFile;
  // Accept multiple possible keys sent from various renderers for media path
  // older code/renderer used `mediaFile`, newer expects `imageFile` — support both
  let imagePath = data.imageFile || data.mediaFile || data.media_path || data.mediaFilePath || null;
  const messageType = data.messageType; // 'text', 'image', 'textWithImage'
  const template = data.template;
  const textMessage = data.textMessage;
  let limit = parseInt(data.limitValue) || null;
  const LimitOPtion = data.limitOption;
  // Anti-ban configuration (can be exposed to UI later)
  const dryRun = !!data.dryRun; // if true, simulate sends without calling WhatsApp
  const rateLimitPerMinute = Number(data.rateLimitPerMinute) || 30; // global msgs per minute
  const perProfileLimitPerMinute = Number(data.perProfileLimitPerMinute) || 20; // per profile per minute
  const maxConsecutiveFailuresBeforePause = Number(data.maxConsecutiveFailuresBeforePause) || 5;
  let sendtime = data.scheduletime|| "rightnow";
  let sendActtime = data.actualtime|| "";
  if(sendtime!="rightnow"){sendtime = sendActtime}
  console.log(sendtime);
  console.log(sendActtime);

  if (LimitOPtion === "noLimit"){limit = 100000}
  
  // Debug: Log all limit-related values
  console.log(`📊 LIMIT DEBUG - User limit: ${limit}, LimitOption: ${LimitOPtion}, limitValue: ${data.limitValue}`);
  
  console.log(`Found ${limit} numbers to message.`);
  try {
    // === STEP 1: Read Numbers from Excel or CSV ===
    if (!fs.existsSync(excelPath)) throw new Error("Excel file not found.");
    
    let rows = [];
    const fileExt = path.extname(excelPath).toLowerCase();
    
    if (fileExt === '.csv') {
      // Handle CSV files (from wizard mapping)
      console.log('📊 Reading CSV file:', excelPath);
      const csvContent = fs.readFileSync(excelPath, 'utf-8');
      const lines = csvContent.split('\n').filter(line => line.trim());
      
      if (lines.length === 0) throw new Error("CSV file is empty.");
      
      // Parse CSV headers
      const headers = lines[0].split(',').map(cell => cell.replace(/"/g, '').trim());
      console.log('📋 CSV Headers:', headers);
      
      // Parse CSV rows
      for (let i = 1; i < lines.length; i++) {
        const cells = lines[i].split(',').map(cell => cell.replace(/"/g, '').trim());
        const row = {};
        headers.forEach((header, index) => {
          row[header] = cells[index] || '';
        });
        if (Object.values(row).some(value => value.trim() !== '')) {
          rows.push(row);
        }
      }
    } else {
      // Handle Excel files (.xlsx, .xls)
      console.log('📊 Reading Excel file:', excelPath);
      const workbook = XLSX.readFile(excelPath);
      const sheetName = workbook.SheetNames[0];
      const worksheet = workbook.Sheets[sheetName];
      rows = XLSX.utils.sheet_to_json(worksheet);
    }

    // Build recipients preserving the original row so we can render templates per-row
    // Also collect rows that were skipped due to missing/invalid phone so we can log them
    const recipients = [];
    const skippedAtBuild = []; // { raw, row, reason }
    for (const row of rows) {
      const raw = row.Phone || row.phone || row.number || row.PhoneNumber || row.phoneNumber || row.Number;
      if (!raw || String(raw).trim() === '') {
        skippedAtBuild.push({ raw: raw || null, row, reason: 'missing' });
        continue;
      }
      const formatted = formatNumber(raw);
      if (!formatted) {
        skippedAtBuild.push({ raw, row, reason: 'invalid_format' });
        continue;
      }
      recipients.push({ number: formatted, row });
    }

    if (recipients.length === 0) {
      throw new Error("No valid phone numbers found in Excel.");
    }

    // Apply limit to recipients before processing
    if (limit && limit > 0 && recipients.length > limit) {
      recipients.splice(limit); // Keep only first 'limit' recipients
      console.log(`📊 Limited to ${limit} recipients out of ${recipients.length + (recipients.length - limit)} total`);
    }

    console.log(`Found ${recipients.length} numbers to message.`);
    const jsonLogPath  = getJsonLogsPath();
    const jobId = "job_" + getLocalISOString().replace(/[-:.TZ]/g, "");
    const timestampStr = getLocalISOString().replace(/[:.]/g, "-");
    const jobMeta = {
      id: jobId,
      timestamp: timestampStr,
      profiles: requestedProfiles,
      messageType : messageType,
      template_name: template || "(no template)",
      record_total: recipients.length,
      record_sent: 0,
      record_failed: 0,
      status: "scheduled",
      scheduled_time: sendtime,
      path: "" // to be updated later
    };
    // ✅ INSERT HERE
    jobControllers[jobId] = { paused: false, cancelled: false };

    const existingLogs = readJsonLog(jsonLogPath);
    existingLogs.push(jobMeta);
    writeJsonLog(jsonLogPath, existingLogs);
    // Immediately log any recipients that were dropped during initial formatting so they appear in reports
    if (skippedAtBuild.length > 0) {
      for (const s of skippedAtBuild) {
        try {
          const mid = 'ml_' + Date.now().toString() + '_' + Math.floor(Math.random() * 1000);
          insertMessageLog.run({
            id: mid,
            job_id: jobId,
            unique_id: (s.row && (s.row.unique_id || s.row.uniqueId)) || null,
            name: (s.row && (s.row.name || s.row.Name)) || null,
            phone: s.raw || null,
            profile: null,
            template: template || null,
            message: '',
            status: 'Failed',
            sent_at: getLocalISOString(),
            error: s.reason === 'missing' ? 'Missing phone in row' : 'Invalid phone format',
            media_path: null,
            media_filename: null
          });
        } catch (e) {
          console.warn('Failed to insert skipped-at-build message log:', e && e.message ? e.message : e);
        }
      }
      // Update JSON job meta counts to reflect these failures immediately
      try {
        const reports = readJsonLog(jsonLogPath);
        const idx = reports.findIndex(r => r.id === jobId);
        if (idx !== -1) {
          reports[idx].record_failed = (reports[idx].record_failed || 0) + skippedAtBuild.length;
          writeJsonLog(jsonLogPath, reports);
        }
      } catch (e) {
        console.warn('Failed to update job meta with skipped-at-build counts:', e && e.message ? e.message : e);
      }
    }
    // Inform renderer that the job was queued (we will send progress and finished events later)
    try { event.reply("send-message-response", { success: true, message: 'Job queued, Please Check Report', jobId }); } catch (e) {}
    // NOTE: sender popup close removed from main process to let renderer control closing.
    // The renderer will call window.close() when it receives the queued acknowledgement.

    // === STEP 2: Decide When to Start ===
    let delay = 0;
    if (sendtime && sendtime.toLowerCase() !== "rightnow") {

      const [sendHour, sendMinute] = sendtime.split(":").map(Number);

      const now = new Date();
      const currentHour = now.getHours();
      const currentMinute = now.getMinutes();

      // Convert both times to total minutes for easy comparison
      const sendTotalMinutes = sendHour * 60 + sendMinute;
      const currentTotalMinutes = currentHour * 60 + currentMinute;
      delay = sendTotalMinutes - currentTotalMinutes;
      if (delay < 0) delay = 0; // if past, send immediately
    }

    // shared counters/logs for the job (declared outside setTimeout so outer scope can reference)
    let allLogs = [];
    let failed = [];
    let sentCount = 0;
    // trackers for anti-ban
    const perProfileSentWindow = {}; // { profileName: [timestamps] }
    const perProfileFailures = {}; // { profileName: consecutiveFailures }
    let globalSentWindow = []; // timestamps of sends in the last minute

    function nowTs() { return Date.now(); }
    function pruneWindow(arr, windowMs) {
      const cutoff = Date.now() - windowMs;
      while (arr.length && arr[0] < cutoff) arr.shift();
    }

    async function rateLimitWait(profileName) {
      // prune old entries
      pruneWindow(globalSentWindow, 60000);
      if (!perProfileSentWindow[profileName]) perProfileSentWindow[profileName] = [];
      pruneWindow(perProfileSentWindow[profileName], 60000);

      // while limits exceeded, wait briefly
      while (globalSentWindow.length >= rateLimitPerMinute || perProfileSentWindow[profileName].length >= perProfileLimitPerMinute) {
        // wait 1s then recheck
        await new Promise(r => setTimeout(r, 1000));
        pruneWindow(globalSentWindow, 60000);
        pruneWindow(perProfileSentWindow[profileName], 60000);
      }
      // record tentative send time
      const ts = nowTs();
      globalSentWindow.push(ts);
      perProfileSentWindow[profileName].push(ts);
    }

    setTimeout(async () => {
      console.log(`Starting job ${jobId} at ${sendtime || "now"}`);
      updateJobInLog(jsonLogPath, jobId, { status: "in_progress" });
      // console.log(activeClients);
      try {
  // Filter to only profiles that have an active client
  const loggedProfiles = requestedProfiles.filter(p => !!activeClients[p]);
  const skippedProfiles = requestedProfiles.filter(p => !activeClients[p]);

      if (skippedProfiles.length > 0) {
        console.warn(`Skipped profiles not logged in: ${skippedProfiles.join(', ')}`);
      }

      if (loggedProfiles.length === 0) {
        // Nothing to do — update job log and notify renderer
        updateJobInLog(jsonLogPath, jobId, { status: 'failed', error: 'No profiles are logged in' });
        event.reply('send-message-response', { success: false, message: 'No profiles are logged in', skipped: skippedProfiles });
        delete jobControllers[jobId];
        return;
      }

      // Register this job against each profile so auth failures can pause it
      for (const p of loggedProfiles) {
        if (!profileJobs[p]) profileJobs[p] = new Set();
        profileJobs[p].add(jobId);
      }

      // Build a single sequential message queue (assign recipients round-robin to logged profiles)
      const messageQueue = [];
      for (let i = 0; i < recipients.length; i++) {
        const profileName = loggedProfiles[i % loggedProfiles.length];
        messageQueue.push({ profileName, number: recipients[i].number, row: recipients[i].row });
      }

      // Prepare media/template once for the run
      let media = null;
      // Track the actual media file path and filename used for this job so we can log/resend later
      let preparedMediaPath = null;
      let preparedMediaFilename = null;
      // Accept several synonyms from different UIs: 'media' (new), 'image' (legacy), and combined types
      if (messageType === "media" || messageType === "image" || messageType === "textWithImage" || messageType === "imageWithText") {
        if (!imagePath || !fs.existsSync(imagePath)) {
          throw new Error("Image file not found or not provided.");
        }
        media = MessageMedia.fromFilePath(imagePath);
        preparedMediaPath = imagePath;
        try { preparedMediaFilename = path.basename(preparedMediaPath); } catch (e) { preparedMediaFilename = null; }
        console.log(`Image loaded from: ${imagePath}`);
      }
      
      // Enhanced template support - handle both text and media templates
      let templateText = '';
      let templateData = null;
      
      if (template && template.trim() !== '') {
        try {
          // Get full template data to determine type and content
          templateData = getTemplateData(template);
          console.log(`📝 Template "${template}" loaded: Type=${templateData.type}, HasMedia=${!!templateData.media_path}`);
          
          // Set template text from template data
          templateText = templateData.message || '';
          
          // Don't auto-convert messageType anymore - let the user's choice be respected
          console.log(`� Using template "${template}" with messageType "${messageType}"`);
          
        } catch (e) {
          // Fallback to old method if template not found in new format
          console.warn(`Template "${template}" not found in database, trying legacy method:`, e.message);
          templateText = GettemplateText(template);
        }
      } else if (messageType === 'text') {
        // No template specified but messageType is text - use textMessage
        templateText = textMessage || '';
      }

      // helper: render template placeholders for a row (same logic as scheduled send)
      function renderTemplateForPatient(tmpl, patient) {
        if (!tmpl) return '';
        const p = patient || {};
        const normalized = Object.keys(p).reduce((acc, k) => {
          acc[k] = p[k];
          acc[k.toLowerCase()] = p[k];
          acc[k.replace(/[_\s]/g, '').toLowerCase()] = p[k];
          return acc;
        }, {});

  const nameVal = normalized['name'] || normalized['fullname'] || normalized['full name'] || p.name || p.Name || '';
  const firstName = (String(nameVal || '').trim().split(/\s+/)[0]) || '';

        // Support both {{key}} (exact match required) and {key} (fuzzy match allowed)
        return String(tmpl).replace(/(\{\{\s*([^}]+?)\s*\}\}|\{\s*([^}]+?)\s*\})/g, (m, _all, g2, g3) => {
          const isDouble = !!g2; // true for {{key}}
          const rawKey = String((g2 || g3) || '').trim();
          const k = rawKey;
          const kl = k.toLowerCase();

          // If double-braced, require exact column name (case-insensitive, but exact token)
          if (isDouble) {
            const keys = Object.keys(p || {});
            for (const candidate of keys) {
              if (String(candidate || '').trim().toLowerCase() === rawKey.toLowerCase()) {
                return p[candidate] == null ? '' : String(p[candidate]);
              }
            }
            // No exact column found — leave placeholder as-is so it is sent unchanged
            return m;
          }

          // Single-brace: restrict substitutions ONLY to the allowed keys list
          // Allowed (case-insensitive, underscores/spaces ignored): name, phone, var1..var6
          const allowed = new Set(['name','phone','var1','var2','var3','var4','var5','var6']);
          const normKey = k.replace(/[_\s]/g, '').toLowerCase();
          if (allowed.has(normKey)) {
            // Prefer normalized lookup (keys without spaces/underscores, lowercased)
            if (Object.prototype.hasOwnProperty.call(normalized, normKey)) return normalized[normKey] == null ? '' : String(normalized[normKey]);
            if (Object.prototype.hasOwnProperty.call(normalized, k)) return normalized[k] == null ? '' : String(normalized[k]);
            if (Object.prototype.hasOwnProperty.call(normalized, kl)) return normalized[kl] == null ? '' : String(normalized[kl]);
            // direct object fallbacks
            if (Object.prototype.hasOwnProperty.call(p, k)) return p[k] == null ? '' : String(p[k]);
            if (Object.prototype.hasOwnProperty.call(p, kl)) return p[kl] == null ? '' : String(p[kl]);
            return '';
          }
          // not an allowed single-brace key -> return empty (do not attempt other aliases)
          return '';
        });
      }

      // Validate that template placeholders are present in Excel columns (if any)
      if (messageType === 'text' && templateText) {
        const placeholderSet = new Set();
        let m;
        const re = /\{\s*([^}]+)\s*\}/g;
        while ((m = re.exec(templateText)) !== null) {
          const key = (m[1] || '').trim();
          if (key) placeholderSet.add(key);
        }

        if (placeholderSet.size > 0) {
          // collect normalized column names from the XLSX rows (union)
          const cols = new Set();
          for (const r of rows) {
            for (const k of Object.keys(r || {})) {
              const nk = String(k).toLowerCase().replace(/[_\s]/g, '');
              cols.add(nk);
            }
          }

          for (const rawKey of placeholderSet) {
            const base = String(rawKey).split('.')[0].toLowerCase().replace(/[_\s]/g, '');
            // allow 'firstname' to match 'name' too
            const ok = cols.has(base) || (base === 'firstname' && cols.has('name')) || (base === 'firstname' && cols.has('fullname'));
            if (!ok) {
              // abort early: missing placeholder column in Excel
              try { event.reply('send-message-response', { success: false, message: `Missing column for placeholder: {${rawKey}} in the Excel file.` }); } catch (e) {}
              return;
            }
          }
        }
      }

  // Send messages PARALLEL across profiles while maintaining all safety features
  // Group messages by profile for parallel processing
  const profileGroups = {};
  for (const item of messageQueue) {
    if (!profileGroups[item.profileName]) profileGroups[item.profileName] = [];
    profileGroups[item.profileName].push(item);
  }

  // Thread-safe shared counters for parallel processing
  const sharedCounters = { sentCount: 0, failedCount: 0 };
  const allLogsCollector = []; // Collect logs from all profiles
  const failedCollector = []; // Collect failed items from all profiles
  const counterLock = { inUse: false }; // Simple lock for atomic operations
  
  // Helper function for thread-safe counter updates
  async function safeUpdate(sentIncrement = 0, failedIncrement = 0, logEntry = null, failedEntry = null) {
    while (counterLock.inUse) {
      await new Promise(r => setTimeout(r, 10)); // Wait for lock
    }
    counterLock.inUse = true;
    try {
      sharedCounters.sentCount += sentIncrement;
      sharedCounters.failedCount += failedIncrement;
      if (logEntry) allLogsCollector.push(logEntry);
      if (failedEntry) failedCollector.push(failedEntry);
      // Update job log with current totals
      updateJobInLog(jsonLogPath, jobId, { 
        record_sent: sharedCounters.sentCount, 
        record_failed: sharedCounters.failedCount 
      });
    } finally {
      counterLock.inUse = false;
    }
  }

  // Process each profile in parallel
  const profileNames = Object.keys(profileGroups);
  const profilePromises = profileNames.map(async (profileName) => {
    const profileQueue = profileGroups[profileName] || [];
    const client = activeClients[profileName];
    
    // Per-profile rate limiting window
    const profileSentWindow = [];
    function pruneProfileWindow() {
      const cutoff = Date.now() - 60000;
      while (profileSentWindow.length && profileSentWindow[0] < cutoff) profileSentWindow.shift();
    }
    async function profileRateLimitWait() {
      pruneProfileWindow();
      while (profileSentWindow.length >= perProfileLimitPerMinute) {
        await new Promise(r => setTimeout(r, 1000));
        pruneProfileWindow();
      }
      profileSentWindow.push(Date.now());
    }

    // Track consecutive failures for this profile
    let profileFailures = 0;

    // Process messages for this profile sequentially (maintaining delays within profile)
    for (const item of profileQueue) {
      const controller = jobControllers[jobId];
      if (!controller) break; // job removed
      if (controller.cancelled) {
        console.warn(`❌ Job ${jobId} cancelled.`);
        break;
      }

      while (controller.paused) {
        await new Promise(res => setTimeout(res, 1000)); // check every 1s
      }

      if (!client) {
        const failedEntry = { number: item.number, error: `Profile ${item.profileName} not logged in` };
        await safeUpdate(0, 1, null, failedEntry);
        event.reply("message-progress", {
          id: jobId,
          profile: item.profileName,
          number: item.number,
          status: "failed",
          error: `Profile ${item.profileName} not logged in`,
          sent: sharedCounters.sentCount,
          failed: sharedCounters.failedCount,
          total: recipients.length
        });
        continue;
      }

      // Safety check: Skip if profile is in cooldown
      if (isProfileInCooldown(item.profileName)) {
        const failedEntry = { number: item.number, error: `Profile ${item.profileName} is in safety cooldown` };
        await safeUpdate(0, 1, null, failedEntry);
        event.reply("message-progress", {
          id: jobId,
          profile: item.profileName,
          number: item.number,
          status: "failed",
          error: `Profile ${item.profileName} is in safety cooldown`,
          sent: sharedCounters.sentCount,
          failed: sharedCounters.failedCount,
          total: recipients.length
        });
        continue;
      }

      // enforce rate limiting before attempting send
      await profileRateLimitWait();

      // Check per-profile daily limit (if configured) and skip/pause if reached
      try {
        const pLimit = getProfileDailyLimit(item.profileName);
        const pSent = getProfileDailySent(item.profileName);
        console.log(`📊 DAILY LIMIT DEBUG - Profile: ${item.profileName}, Daily limit: ${pLimit}, Already sent today: ${pSent}`);
        
        if (pLimit && Number(pLimit) > 0) {
          if (pSent >= Number(pLimit)) {
            console.log(`⚠️ DAILY LIMIT REACHED - Profile ${item.profileName} has reached daily limit of ${pLimit} (sent: ${pSent})`);
            
            // Send detailed feedback to user about daily limit reached
            try {
              event.reply('daily-limit-reached', {
                jobId: jobId,
                profileName: item.profileName,
                dailyLimit: pLimit,
                sentToday: pSent,
                message: `Daily limit reached for profile "${item.profileName}". Sent ${pSent}/${pLimit} messages today. Messages for this profile will be skipped.`,
                progress: {
                  sent: sharedCounters.sentCount,
                  failed: sharedCounters.failedCount,
                  total: recipients.length
                }
              });
            } catch (e) {
              console.warn('Failed to send daily-limit-reached event:', e && e.message ? e.message : e);
            }
            
            const logEntry = {
              Profile: item.profileName,
              Phone: item.number,
              Status: "Skipped",
              Timestamp: getLocalISOString(),
              MessageType: messageType,
              Message: "",
              Error: "Daily limit reached"
            };
            await safeUpdate(0, 1, logEntry);
            
            // Also log to message_logs table
            try {
              const mid = 'ml_' + Date.now().toString() + '_' + Math.floor(Math.random() * 1000);
              insertMessageLog.run({
                id: mid,
                job_id: jobId,
                unique_id: (item.row && (item.row.unique_id || item.row.uniqueId)) || null,
                name: (item.row && (item.row.name || item.row.Name)) || null,
                phone: item.number,
                profile: item.profileName,
                template: template || null,
                message: textMessage || null,
                status: 'Skipped',
                sent_at: getLocalISOString(),
                error: 'Daily limit reached',
                media_path: preparedMediaPath || null,
                media_filename: preparedMediaFilename || null
              });
            } catch (e) {
              console.warn('Failed to log daily limit skip to message_logs:', e && e.message ? e.message : e);
            }
            
            // Send progress update for the skipped message
            try {
              event.reply("message-progress", {
                id: jobId,
                profile: item.profileName,
                number: item.number,
                status: "skipped",
                error: "Daily limit reached",
                sent: sharedCounters.sentCount,
                failed: sharedCounters.failedCount,
                total: recipients.length
              });
            } catch (e) {
              console.warn('Failed to send progress update for daily limit skip:', e && e.message ? e.message : e);
            }
            
            // skip this recipient
            continue;
          }
        }
      } catch (e) { console.warn('Failed checking profile daily limit:', e && e.message ? e.message : e); }

      const timestamp = getLocalISOString();
      // VERIFY PHONE BEFORE ATTEMPTING SEND (no retries for invalid format)
      try {
        const phoneCheck = await verifyAndFormatPhone(client, item.number);
        if (!phoneCheck.valid) {
          // Log invalid phone and mark as failed for this recipient
          const reason = phoneCheck.reason || 'invalid';
          const messageId = 'msg_' + getLocalISOString().replace(/[-:.TZ]/g, '') + '_' + Math.random().toString(36).substr(2, 6);
          try {
            insertMessageLog.run({
              id: messageId,
              job_id: jobId,
              unique_id: (item.row && (item.row.unique_id || item.row.uniqueId)) || null,
              name: (item.row && (item.row.name || item.row.Name)) || null,
              phone: item.number || null,
              profile: item.profileName,
              template: template || null,
              message: '',
              status: 'Failed',
              sent_at: getLocalISOString(),
              error: `Invalid phone (${reason})`,
              media_path: preparedMediaPath || null,
              media_filename: preparedMediaFilename || null
            });
          } catch (e) { console.warn('insertMessageLog failed (invalid-phone-precheck):', e && e.message ? e.message : e); }

          allLogsCollector.push({ Profile: item.profileName, Phone: item.number, Status: 'Failed', Timestamp: getLocalISOString(), Message: '', Error: `Invalid phone (${reason})` });
          // update job counts
          await safeCounterUpdate(0, 1, { Profile: item.profileName, Phone: item.number, Status: 'Failed', Timestamp: getLocalISOString(), Message: '', Error: `Invalid phone (${reason})` });
          try { event.reply('message-progress', { id: jobId, profile: item.profileName, number: item.number, status: 'failed', error: `Invalid phone (${reason})`, sent: sharedCounters.sentCount, failed: sharedCounters.failedCount, total: recipients.length }); } catch (e) {}
          // skip this recipient
          continue;
        }
        // attach formatted jid for later use in retry loop
        item.__validated_jid = phoneCheck.jid;
      } catch (e) {
        console.warn('Phone precheck failed unexpectedly:', e && e.message ? e.message : e);
        // proceed to retry loop — validation will occur there
      }

      let attempt = 0;
      let success = false;
      let lastError = null;

      // retry loop with exponential backoff
      while (attempt <= 4 && !success) {
        attempt++;
        try {
          // Pre-send profile validation - check if client is ready
          const profileValidation = await validateProfileReadiness(item.profileName, client);
          if (!profileValidation.ready) {
            throw new Error(`Profile validation failed: ${profileValidation.error}`);
          }
          
          const jid = item.__validated_jid || formatNumber(item.number);
          let messageContent = "";

          if (dryRun) {
            // simulate send (rendered for text)
            if (messageType === 'text') {
              const renderedSim = renderTemplateForPatient(textMessage || templateText, item.row);
              messageContent = renderedSim;
            } else if (messageType === 'media') {
              messageContent = '(media file only)';
            } else if (messageType === 'template') {
              if (templateData && templateData.type === 'Media') {
                const renderedCaption = templateText ? renderTemplateForPatient(templateText, item.row) : '';
                messageContent = `[TEMPLATE MEDIA: ${templateData.media_filename || 'media'}]${renderedCaption ? ' - ' + renderedCaption : ''}`;
              } else {
                const renderedSim = renderTemplateForPatient(templateText, item.row);
                messageContent = renderedSim;
              }
            } else {
              messageContent = textMessage || templateText;
            }
            console.log(`[dryRun] Would send to ${item.profileName}:${item.number} -> ${messageContent}`);
          } else {
            // ACTUAL MESSAGE SENDING LOGIC
            if (messageType === "text") {
              // Pure text message (custom text, not template)
              const textToSend = textMessage || templateText;
              if (!textToSend || textToSend.trim() === '') throw new Error("No text message provided.");
              const rendered = renderTemplateForPatient(textToSend, item.row);
              if (!rendered || rendered.trim() === '') throw new Error("Rendered text message is empty.");
              await sendMessageSafely(client, jid, rendered);
              messageContent = rendered;
              console.log(`✅ Text message sent: ${rendered.substring(0, 50)}...`);
              
            } else if (messageType === "media") {
              // Pure media file - no text
              if (!media) throw new Error("No media file provided for media message.");
              await sendMessageSafely(client, jid, media);
              messageContent = "(media file only)";
              console.log(`✅ Media file sent without caption`);
              
            } else if (messageType === "template") {
              // Template-based message - check template type
              if (!template || !templateData) {
                throw new Error("No template selected for template message type.");
              }
              
              if (templateData.type === 'Media') {
                // Media template - send media with optional caption
                if (!templateData.media_path) {
                  throw new Error("Media template configured but no media file path found.");
                }
                
                const fs = require('fs');
                const path = require('path');
                let mediaPath = templateData.media_path;
                
                // If media_path is just a filename, construct full path
                if (!path.isAbsolute(mediaPath)) {
                  const userDataPath = getUserDataDir();
                  mediaPath = path.join(userDataPath, 'media', templateData.media_filename || templateData.media_path);
                }
                
                // Try to send media file
                if (fs.existsSync(mediaPath)) {
                  const templateMediaData = MessageMedia.fromFilePath(mediaPath);
                  
                  if (templateText && templateText.trim() !== '') {
                    // Send media with caption
                    const renderedCaption = renderTemplateForPatient(templateText, item.row);
                    await sendMessageSafely(client, jid, templateMediaData, { caption: renderedCaption });
                    messageContent = `[TEMPLATE MEDIA: ${templateData.media_filename || 'media'}] - ${renderedCaption}`;
                    console.log(`✅ Template media sent with caption: ${renderedCaption.substring(0, 50)}...`);
                  } else {
                    // Send media without caption
                    await sendMessageSafely(client, jid, templateMediaData);
                    messageContent = `[TEMPLATE MEDIA: ${templateData.media_filename || 'media'}]`;
                    console.log(`✅ Template media sent without caption`);
                  }
                } else {
                  throw new Error(`Template media file not found: ${mediaPath}`);
                }
              } else {
                // Text template - send text only
                if (!templateText || templateText.trim() === '') {
                  throw new Error("Text template has no message content.");
                }
                const rendered = renderTemplateForPatient(templateText, item.row);
                if (!rendered || rendered.trim() === '') throw new Error("Rendered template message is empty.");
                await sendMessageSafely(client, jid, rendered);
                messageContent = rendered;
                console.log(`✅ Template text sent: ${rendered.substring(0, 50)}...`);
              }
              
            } else {
              throw new Error(`Unsupported messageType: ${messageType}. Supported types: text, media, template`);
            }
          }

          // success path
          success = true;
          // increment daily stats
          try { incStat(item.profileName); } catch (e) {}
          // reset consecutive failure counter
          profileFailures = 0;

          const logEntry = {
            Profile: item.profileName,
            Phone: item.number,
            Status: "Sent",
            Timestamp: timestamp,
            MessageType: messageType,
            Message: messageContent,
            Error: ""
          };
          await safeUpdate(1, 0, logEntry);

          // persist a record into message_logs
          try {
            const mid = 'ml_' + Date.now().toString() + '_' + Math.floor(Math.random() * 1000);
            insertMessageLog.run({
              id: mid,
              job_id: jobId,
              unique_id: (item.row && (item.row.unique_id || item.row.uniqueId)) || null,
              name: (item.row && (item.row.name || item.row.Name)) || null,
              phone: item.number,
              profile: item.profileName,
              template: template || null,
              message: messageContent || null,
              media_path: preparedMediaPath || null,
              media_filename: preparedMediaFilename || null,
              status: 'Sent',
              sent_at: getLocalISOString(),
              error: ''
            });
          } catch (e) { console.warn('insertMessageLog failed (sent):', e && e.message ? e.message : e); }

          event.reply("message-progress", {
            id: jobId,
            profile: item.profileName,
            number: item.number,
            status: "sent",
            sent: sharedCounters.sentCount,
            failed: sharedCounters.failedCount,
            total: recipients.length
          });

          console.log(`[${item.profileName}] Sent to: ${item.number}`);
          
          // Update session health on success
          updateSessionHealth(item.profileName, 'messagesSent', (getSessionHealth(item.profileName)?.messagesSent || 0) + 1);
          updateSessionHealth(item.profileName, 'lastMessageTime', Date.now());
          updateSessionHealth(item.profileName, 'consecutiveFailures', 0); // Reset on success
          
        } catch (err) {
          lastError = err;
          console.error(`Attempt ${attempt} failed for ${item.number}:`, err && err.message ? err.message : err);

          // Special-case: WhatsApp Web 'findChat: new chat not found' transient error
          // Retry once immediately for this recipient to allow WA to recover/create chat.
          try {
            const _errMsg = String(err && (err.message || err) || '').toLowerCase();
            if ((_errMsg.includes('findchat') || _errMsg.includes('new chat not found') || _errMsg.includes('new chat not found')) && !item.__findChatRetried) {
              console.warn(`[${item.profileName}] Transient findChat error for ${item.number} - retrying once immediately`);
              item.__findChatRetried = true;
              // short pause to let WA internal state settle, then retry without increasing backoff
              await new Promise(r => setTimeout(r, 2000));
              continue; // go to next attempt immediately
            }
          } catch (e) {
            // ignore errors from retry logic
          }
          
          // Check if this is a WhatsApp validation error (number not on WhatsApp)
          if (err.message.includes('Number not registered on WhatsApp') || 
              err.message.includes('not registered on WhatsApp') || 
              err.message.includes('WhatsApp validation failed') ||
              err.message.includes('Invalid number')) {
            console.log(`❌ ${item.number} is not on WhatsApp, skipping retries`);
            // Don't retry for invalid WhatsApp numbers
            break;
          }
          
          // Update session health on failure
          const currentFailures = getSessionHealth(item.profileName)?.consecutiveFailures || 0;
          updateSessionHealth(item.profileName, 'consecutiveFailures', currentFailures + 1);
          
          // Check for rate limiting patterns
          if (err.message.includes('rate') || err.message.includes('limit') || err.message.includes('spam')) {
            const currentHits = getSessionHealth(item.profileName)?.rateLimitHits || 0;
            updateSessionHealth(item.profileName, 'rateLimitHits', currentHits + 1);
            console.warn(`[${item.profileName}] Possible rate limiting detected: ${err.message}`);
          }
          
          // record failure but don't add to final failed list until retries exhausted
          const backoffMs = Math.min(30000, Math.pow(2, attempt) * 1000); // 2s,4s,8s,16s,30s cap
          await new Promise(r => setTimeout(r, backoffMs));
        }
      }

      if (!success) {
        profileFailures++;
        
        // Check if we should put this profile into cooldown
        const health = getSessionHealth(item.profileName);
        if (health && health.consecutiveFailures >= 5) {
          setCooldown(item.profileName, 15); // 15 minute cooldown for high failure rate
        }
        if (health && health.rateLimitHits >= 3) {
          setCooldown(item.profileName, 30); // 30 minute cooldown for rate limiting
        }
        
        const failedEntry = { number: item.number, error: lastError ? lastError.message : 'Unknown' };
        
        // Create cleaner error message for display
        let displayError = lastError ? lastError.message : 'Unknown';
        if (displayError.includes('Number not registered on WhatsApp')) {
          displayError = 'Number not registered on WhatsApp';
        } else if (displayError.includes('WhatsApp validation failed')) {
          displayError = displayError.replace('WhatsApp validation failed: ', '');
        }
        
        const logEntry = {
          Profile: item.profileName,
          Phone: item.number,
          Status: "Failed",
          Timestamp: timestamp,
          MessageType: messageType,
          Message: textMessage || "(image only)",
          Error: displayError
        };
        await safeUpdate(0, 1, logEntry, failedEntry);

        // persist failure to message_logs with clean error message
        try {
          const mid = 'ml_' + Date.now().toString() + '_' + Math.floor(Math.random() * 1000);
          insertMessageLog.run({
            id: mid,
            job_id: jobId,
            unique_id: (item.row && (item.row.unique_id || item.row.uniqueId)) || null,
            name: (item.row && (item.row.name || item.row.Name)) || null,
            phone: item.number,
            profile: item.profileName,
            template: template || null,
            message: textMessage || null,
            media_path: preparedMediaPath || null,
            media_filename: preparedMediaFilename || null,
            status: 'Failed',
            sent_at: getLocalISOString(),
            error: displayError
          });
        } catch (e) { console.warn('insertMessageLog failed (failed):', e && e.message ? e.message : e); }

        event.reply("message-progress", {
          id: jobId,
          profile: item.profileName,
          number: item.number,
          status: "failed",
          error: displayError,
          sent: sharedCounters.sentCount,
          failed: sharedCounters.failedCount,
          total: recipients.length
        });

        // if profile has repeated failures, pause it and mark remaining messages as skipped
        if (profileFailures >= maxConsecutiveFailuresBeforePause) {
          console.warn(`Pausing profile ${item.profileName} due to repeated failures - marking remaining messages as skipped`);
          
          // Mark all remaining messages for this profile as skipped
          const remainingMessages = profileQueue.slice(profileQueue.indexOf(item) + 1);
          console.log(`Marking ${remainingMessages.length} remaining messages as skipped for profile ${item.profileName}`);
          
          for (const skippedItem of remainingMessages) {
            const skippedLogEntry = {
              Profile: skippedItem.profileName,
              Phone: skippedItem.number,
              Status: "Skipped",
              Timestamp: getLocalISOString(),
              MessageType: messageType,
              Message: textMessage || "(image only)",
              Error: `Profile paused due to ${profileFailures} consecutive failures - safety measure`
            };
            
            try {
              allLogsCollector.push(skippedLogEntry);
              sharedCounters.failedCount++; // Count skipped as failed for progress tracking
              
              // Also log to message_logs table
              const mid = 'ml_' + Date.now().toString() + '_' + Math.floor(Math.random() * 1000);
              insertMessageLog.run({
                id: mid,
                job_id: jobId,
                unique_id: (skippedItem.row && (skippedItem.row.unique_id || skippedItem.row.uniqueId)) || null,
                name: (skippedItem.row && (skippedItem.row.name || skippedItem.row.Name)) || null,
                phone: skippedItem.number,
                profile: skippedItem.profileName,
                template: template || null,
                message: textMessage || null,
                media_path: preparedMediaPath || null,
                media_filename: preparedMediaFilename || null,
                status: 'Skipped',
                sent_at: getLocalISOString(),
                error: `Profile paused due to ${profileFailures} consecutive failures - safety measure`
              });
              
              // Send progress update for each skipped message
              event.reply("message-progress", {
                id: jobId,
                profile: skippedItem.profileName,
                number: skippedItem.number,
                status: "skipped",
                error: `Profile paused due to consecutive failures - safety measure`,
                sent: sharedCounters.sentCount,
                failed: sharedCounters.failedCount,
                total: recipients.length
              });
            } catch (e) {
              console.warn('Failed to log skipped message:', e && e.message ? e.message : e);
            }
          }
          
          jobControllers[jobId].paused = true;
          updateJobInLog(jsonLogPath, jobId, { status: 'paused', paused: true });
          
          // Send detailed feedback to user about rate limiting/consecutive failures
          try {
            event.reply('job-paused', {
              jobId: jobId,
              reason: 'consecutive_failures',
              profileName: item.profileName,
              message: `Job paused due to ${profileFailures} consecutive failures on profile "${item.profileName}". ${remainingMessages.length} remaining messages marked as skipped. This is a safety measure to prevent account restrictions.`,
              progress: {
                sent: sharedCounters.sentCount,
                failed: sharedCounters.failedCount,
                total: recipients.length
              }
            });
          } catch (e) {
            console.warn('Failed to send job-paused event:', e && e.message ? e.message : e);
          }
          
          // Also send a progress update showing the current status
          try {
            event.reply('send-progress', {
              sent: sharedCounters.sentCount,
              failed: sharedCounters.failedCount,
              total: recipients.length,
              completed: false,
              paused: true,
              currentProfile: item.profileName,
              message: `Job paused due to safety limits on profile "${item.profileName}" - ${remainingMessages.length} messages skipped`,
              logs: []
            });
          } catch (e) {
            console.warn('Failed to send progress update:', e && e.message ? e.message : e);
          }
          
          break;
        }
      }

      // Enhanced adaptive delay with human-like patterns
      const messageCount = profileSentWindow.length;
      const delayBetween = getAdaptiveDelay(item.profileName, messageCount);
      await new Promise(res => setTimeout(res, delayBetween));
    }
  });

  // Wait for all profiles to complete processing in parallel
  await Promise.all(profilePromises);
  
  // Update final counts from shared counters
  sentCount = sharedCounters.sentCount;
  failed = failedCollector;
  allLogs.push(...allLogsCollector);

      // Calculate WhatsApp validation summary for reporting
      const whatsappValidationFailures = allLogs.filter(log => 
        log.Status === 'Failed' && 
        (log.Error.includes('Number not registered on WhatsApp') || 
         log.Error.includes('not registered on WhatsApp'))
      ).length;
      
      if (whatsappValidationFailures > 0) {
        console.log(`📊 WhatsApp Validation Summary: ${whatsappValidationFailures} numbers were not registered on WhatsApp out of ${recipients.length} total`);
      }

      // cleanup profileJobs entries for this job
      for (const p of loggedProfiles) {
        try { profileJobs[p].delete(jobId); } catch (e) {}
      }

      const logDir1 = path.join(getUserDataDir(), "logs");
      try { fs.mkdirSync(logDir1, { recursive: true }); } catch (e) {}
      const timestampStr1 = getLocalISOString().replace(/[:.]/g, "-");
      const logFileName1 = `log-multiclient-${timestampStr1}.xlsx`;
      const logFilePath1 = path.join(logDir1, logFileName1);
      const worksheet2 = XLSX.utils.json_to_sheet(allLogs);
      const logWorkbook = XLSX.utils.book_new();
      XLSX.utils.book_append_sheet(logWorkbook, worksheet2, "Message Log");
      XLSX.writeFile(logWorkbook, logFilePath1);

      // === STEP 5: Final JSON Update ===
      updateJobInLog(jsonLogPath, jobId, {
        record_sent: sentCount,
        record_failed: failed.length,
        status: "completed",
        path: logFilePath1
      });

      event.reply("message-finished", {
        id: jobId,
        total: recipients.length,
        sent: sentCount,
        failed: failed.length,
        path: logFilePath1
      });
      delete jobControllers[jobId];
      console.log("All messages sent.");
      } catch (err) {
        console.error("send-message-proc inner error:", err);
        updateJobInLog(jsonLogPath, jobId, { status: "failed" });
        event.reply("send-message-response", {
          success: false,
          message: "Error: " + err.message
        });
      }
    }, delay);
      
      // console.log("All messages processed");
      // // now safe to destroy
      // await client.destroy();

  } catch (err) {
    console.error("send-message-proc error:", err);
    event.reply("send-message-response", {
      success: false,
      message: "Error: " + err.message
    });
  }
  

});
// Send a schedule immediately from scheduledResults UI
ipcMain.on('send-schedule-now', (event, schedule) => {
  try {
    const patients = Array.isArray(schedule.patients) ? schedule.patients.slice() : [];
    const requestedProfiles = Array.isArray(schedule.profiles) ? schedule.profiles.slice() : [];
    const jsonLogPath = getJsonLogsPath();
    const jobId = 'schedjob_' + getLocalISOString().replace(/[-:.TZ]/g, '');
    const nowStr = getLocalISOString().replace(/[:.]/g, '-');
    const jobMeta = {
      id: jobId,
      timestamp: nowStr,
      schedule: schedule.schedule || '',
      template_name: schedule.templateName || '(no template)',
      record_total: patients.length,
      record_sent: 0,
      record_failed: 0,
      status: 'scheduled'
    };

    jobControllers[jobId] = { paused: false, cancelled: false };
    const existing = readJsonLog(jsonLogPath);
    existing.push(jobMeta);
    writeJsonLog(jsonLogPath, existing);

    // reply that job queued
    try { event.reply('schedule-send-response', { success: true, message: 'Job queued', jobId, schedule: schedule.schedule, total: patients.length }); } catch (e) {}

    // start async send (don't block)
    setTimeout(async () => {
      let sent = 0;
      let failed = 0;
      const allLogs = [];
      try {
        updateJobInLog(jsonLogPath, jobId, { status: 'in_progress' });

        const loggedProfiles = requestedProfiles.filter(p => !!activeClients[p]);
        if (loggedProfiles.length === 0) {
          updateJobInLog(jsonLogPath, jobId, { status: 'failed', error: 'No profiles logged in' });
          try { event.reply('schedule-message-finished', { id: jobId, total: patients.length, sent: 0, failed: patients.length, schedule: schedule.schedule }); } catch (e) {}
          delete jobControllers[jobId];
          return;
        }

        // register job to profiles
        for (const p of loggedProfiles) {
          if (!profileJobs[p]) profileJobs[p] = new Set();
          profileJobs[p].add(jobId);
        }

        // build queue round-robin
        const queue = [];
        for (let i = 0; i < patients.length; i++) {
          const profileName = loggedProfiles[i % loggedProfiles.length];
          queue.push({ profileName, patient: patients[i] });
        }

        // Initialize message counters for adaptive delay
        const sentCounters = new Map();

        // prepare template text
        let templateText = '';
        try { templateText = GettemplateText(schedule.templateName); } catch (e) { templateText = ''; }

        // Render template placeholders for a given patient record.
        // {{key}} => exact-match only (if not found, leave placeholder as-is)
        // {key}  => flexible/fuzzy matching (backwards compatible)
        function renderTemplateForPatient(tmpl, patient) {
          if (!tmpl) return '';
          const p = patient || {};
          const normalized = Object.keys(p).reduce((acc, k) => { acc[k] = p[k]; acc[k.toLowerCase()] = p[k]; acc[k.replace(/[_\s]/g,'').toLowerCase()] = p[k]; return acc; }, {});
          const firstName = (p.name && String(p.name).trim().split(/\s+/)[0]) || '';

          return String(tmpl).replace(/(\{\{\s*([^}]+?)\s*\}\}|\{\s*([^}]+?)\s*\})/g, (m, _all, g2, g3) => {
            const isDouble = !!g2;
            const rawKey = String((g2 || g3) || '').trim();
            const k = rawKey;
            const kl = k.toLowerCase();

            if (isDouble) {
              // exact-case-insensitive match only
              const keys = Object.keys(p || {});
              for (const candidate of keys) {
                if (String(candidate || '').trim().toLowerCase() === rawKey.toLowerCase()) {
                  return p[candidate] == null ? '' : String(p[candidate]);
                }
              }
              // not found -> return original placeholder unchanged
              return m;
            }

            // single-brace behaviour: restrict to allowed keys only
            const allowed = new Set(['name','phone','var1','var2','var3','var4','var5','var6']);
            const normKey = k.replace(/[_\s]/g,'').toLowerCase();
            if (allowed.has(normKey)) {
              // prefer normalized lookups
              if (Object.prototype.hasOwnProperty.call(normalized, normKey)) return normalized[normKey] == null ? '' : String(normalized[normKey]);
              if (Object.prototype.hasOwnProperty.call(normalized, k)) return normalized[k] == null ? '' : String(normalized[k]);
              if (Object.prototype.hasOwnProperty.call(normalized, kl)) return normalized[kl] == null ? '' : String(normalized[kl]);
              if (k.includes('.')) { const parts = k.split('.'); let cur = p; for (const part of parts) { if (cur && Object.prototype.hasOwnProperty.call(cur, part)) cur = cur[part]; else { cur = ''; break; } } return cur == null ? '' : String(cur); }
              if (Object.prototype.hasOwnProperty.call(p, k)) return p[k] == null ? '' : String(p[k]);
              if (Object.prototype.hasOwnProperty.call(p, kl)) return p[kl] == null ? '' : String(p[kl]);
              // allowed key but no value found -> empty
              return '';
            }

            // not an allowed single-brace key -> return empty (do not attempt fuzzy aliases)
            return '';
          });
        }

        // stmt to update patient last msg/template and schedule days
        const updateLastStmt = db.prepare('UPDATE patients SET Last_Msgsent_date = ?, last_template = ?, last_schedule_days = ?, mod_date = ? WHERE unique_id = ? OR phone = ?');

        for (const item of queue) {
          const controller = jobControllers[jobId];
          if (!controller) break;
          if (controller.cancelled) break;
          while (controller.paused) { await new Promise(r => setTimeout(r, 1000)); }

          const client = activeClients[item.profileName];
          if (!client) {
            failed++;
            allLogs.push({ Profile: item.profileName, Phone: item.patient.phone, Status: 'Failed', Timestamp: getLocalISOString(), Message: '', Error: 'Profile not logged in' });
            try {
              const mid = 'ml_' + Date.now().toString() + '_' + Math.floor(Math.random() * 1000);
              insertMessageLog.run({
                id: mid,
                job_id: jobId,
                unique_id: item.patient.unique_id || item.patient.uniqueId || null,
                name: item.patient.name || null,
                phone: item.patient.phone || null,
                profile: item.profileName,
                template: schedule.templateName || null,
                message: '',
                status: 'Failed',
                sent_at: getLocalISOString(),
                error: 'Profile not logged in'
              });
            } catch (e) { console.warn('insertMessageLog failed (sched failed-not-logged):', e && e.message ? e.message : e); }
            updateJobInLog(jsonLogPath, jobId, { record_sent: sent, record_failed: failed });
            try { event.reply('schedule-message-progress', { id: jobId, profile: item.profileName, number: item.patient.phone, status: 'failed', sent, failed, total: patients.length, schedule: schedule.schedule, template: schedule.templateName }); } catch (e) {}
            continue;
          }

          // simple send with retries
          let attempt = 0;
          let success = false;
          let lastError = null;
          while (attempt <= 3 && !success) {
            attempt++;
              try {
                // phone precheck before attempting send (no retries for invalid format)
                const phoneCheck = await verifyAndFormatPhone(client, item.patient.phone);
                if (!phoneCheck.valid) {
                  const reason = phoneCheck.reason || 'invalid';
                  // Log and mark failed
                  const messageId = 'msg_' + getLocalISOString().replace(/[-:.TZ]/g, '') + '_' + Math.random().toString(36).substr(2, 6);
                  try {
                    insertMessageLog.run({
                      id: messageId,
                      job_id: jobId,
                      unique_id: item.patient.unique_id || item.patient.uniqueId || null,
                      name: item.patient.name || null,
                      phone: item.patient.phone || null,
                      profile: item.profileName,
                      template: schedule.templateName || null,
                      message: '',
                      status: 'Failed',
                      sent_at: getLocalISOString(),
                      error: `Invalid phone (${reason})`
                    });
                  } catch (e) { console.warn('insertMessageLog failed (sched invalid-phone):', e && e.message ? e.message : e); }
                  allLogs.push({ Profile: item.profileName, Phone: item.patient.phone, Status: 'Failed', Timestamp: getLocalISOString(), Message: '', Error: `Invalid phone (${reason})` });
                  // don't retry
                  break;
                }
                const jid = phoneCheck.jid;
                // send text (render template per patient)
                if (!templateText) throw new Error('Template not found');
                const messageToSend = renderTemplateForPatient(templateText, item.patient);
                await sendMessageSafely(client, jid, messageToSend);
                success = true;
              } catch (err) {
              lastError = err;
              
              // Check if this is a WhatsApp validation error (number not on WhatsApp)
              if (err.message.includes('Number not registered on WhatsApp') || 
                  err.message.includes('not registered on WhatsApp') || 
                  err.message.includes('WhatsApp validation failed') ||
                  err.message.includes('Invalid number')) {
                console.log(`❌ ${item.patient.phone} is not on WhatsApp, skipping retries`);
                // Don't retry for invalid WhatsApp numbers
                break;
              }
              
              await new Promise(r => setTimeout(r, Math.min(30000, Math.pow(2, attempt) * 1000)));
            }
          }

          const timestamp = getLocalISOString();
          if (success) {
            sent++;
            try { incStat(item.profileName); } catch (e) {}
            const renderedSentMsg = (typeof renderTemplateForPatient === 'function') ? renderTemplateForPatient(templateText, item.patient) : templateText;
            allLogs.push({ Profile: item.profileName, Phone: item.patient.phone, Status: 'Sent', Timestamp: timestamp, Message: renderedSentMsg, Error: '' });
            try {
              const mid = 'ml_' + Date.now().toString() + '_' + Math.floor(Math.random() * 1000);
              insertMessageLog.run({
                id: mid,
                job_id: jobId,
                unique_id: item.patient.unique_id || item.patient.uniqueId || null,
                name: item.patient.name || null,
                phone: item.patient.phone || null,
                profile: item.profileName,
                template: schedule.templateName || null,
                message: renderedSentMsg || null,
                status: 'Sent',
                sent_at: timestamp,
                error: ''
              });
            } catch (e) { console.warn('insertMessageLog failed (sched sent):', e && e.message ? e.message : e); }
            // update DB record with schedule days
            try {
              updateLastStmt.run(timestamp, schedule.templateName || '', schedule.days || 0, timestamp, item.patient.unique_id || '', item.patient.phone || '');
            } catch (e) { console.error('Failed updating patient last msg:', e.message); }

            // Update sent counter for adaptive delay
            const currentCount = sentCounters.get(item.profileName) || 0;
            sentCounters.set(item.profileName, currentCount + 1);

            updateJobInLog(jsonLogPath, jobId, { record_sent: sent, record_failed: failed });
            try { event.reply('schedule-message-progress', { id: jobId, profile: item.profileName, number: item.patient.phone, status: 'sent', sent, failed, total: patients.length, schedule: schedule.schedule, template: schedule.templateName }); } catch (e) {}
          } else {
            failed++;
            // capture rendered message where possible
            const renderedFailedMsg = (typeof renderTemplateForPatient === 'function') ? renderTemplateForPatient(templateText, item.patient) : templateText;
            allLogs.push({ Profile: item.profileName, Phone: item.patient.phone, Status: 'Failed', Timestamp: timestamp, Message: renderedFailedMsg, Error: lastError ? String(lastError.message) : 'Unknown' });
            try {
              const mid = 'ml_' + Date.now().toString() + '_' + Math.floor(Math.random() * 1000);
              insertMessageLog.run({
                id: mid,
                job_id: jobId,
                unique_id: item.patient.unique_id || item.patient.uniqueId || null,
                name: item.patient.name || null,
                phone: item.patient.phone || null,
                profile: item.profileName,
                template: schedule.templateName || null,
                message: renderedFailedMsg || null,
                status: 'Failed',
                sent_at: timestamp,
                error: lastError ? (lastError.message || String(lastError)) : 'Unknown'
              });
            } catch (e) { console.warn('insertMessageLog failed (sched failed):', e && e.message ? e.message : e); }
            updateJobInLog(jsonLogPath, jobId, { record_sent: sent, record_failed: failed });
            try { event.reply('schedule-message-progress', { id: jobId, profile: item.profileName, number: item.patient.phone, status: 'failed', error: lastError ? String(lastError.message) : 'Unknown', sent, failed, total: patients.length, schedule: schedule.schedule, template: schedule.templateName }); } catch (e) {}
          }

          // Enhanced adaptive delay with human-like patterns
          const messageCount = sentCounters.get(item.profileName) || 0;
          const delayBetween = getAdaptiveDelay(item.profileName, messageCount);
          await new Promise(r => setTimeout(r, delayBetween));
        }

        // cleanup
        for (const p of loggedProfiles) { try { profileJobs[p].delete(jobId); } catch (e) {} }

        // write excel log
        try {
          const logDir1 = path.join(getUserDataDir(), 'logs');
          try { fs.mkdirSync(logDir1, { recursive: true }); } catch (e) {}
          const ts = getLocalISOString().replace(/[:.]/g, '-');
          const fname = `log-schedule-${ts}.xlsx`;
          const fpath = path.join(logDir1, fname);
          const worksheet2 = XLSX.utils.json_to_sheet(allLogs);
          const logWorkbook = XLSX.utils.book_new();
          XLSX.utils.book_append_sheet(logWorkbook, worksheet2, 'Message Log');
          XLSX.writeFile(logWorkbook, fpath);
          updateJobInLog(jsonLogPath, jobId, { record_sent: sent, record_failed: failed, status: 'completed', path: fpath });
          try { event.reply('schedule-message-finished', { id: jobId, total: patients.length, sent, failed, path: fpath, schedule: schedule.schedule }); } catch (e) {}
        } catch (e) {
          updateJobInLog(jsonLogPath, jobId, { record_sent: sent, record_failed: failed, status: 'completed' });
          try { event.reply('schedule-message-finished', { id: jobId, total: patients.length, sent, failed, schedule: schedule.schedule }); } catch (e) {}
        }

      } catch (err) {
        console.error('send-schedule-now failed inner:', err.message);
        updateJobInLog(jsonLogPath, jobId, { status: 'failed', error: String(err.message) });
        try { event.reply('schedule-message-finished', { id: jobId, total: patients.length, sent: 0, failed: patients.length, schedule: schedule.schedule, error: err.message }); } catch (e) {}
      }
      delete jobControllers[jobId];
    }, 0);

  } catch (e) {
    console.error('send-schedule-now handler error:', e.message);
    try { event.reply('schedule-send-response', { success: false, message: e.message }); } catch (err) {}
  }
});

// Send a schedule using patient-assigned profiles when available
// Falls back to the original round-robin behavior when no assigned profile or profile not logged in.
ipcMain.on('send-schedule-now-assigned', (event, schedule) => {
  try {
    const patients = Array.isArray(schedule.patients) ? schedule.patients.slice() : [];
    const requestedProfiles = Array.isArray(schedule.profiles) ? schedule.profiles.slice() : [];
    const jsonLogPath = getJsonLogsPath();
    const jobId = 'schedjob_' + getLocalISOString().replace(/[-:.TZ]/g, '');
    const nowStr = getLocalISOString().replace(/[:.]/g, '-');
    const jobMeta = {
      id: jobId,
      timestamp: nowStr,
      schedule: schedule.schedule || '',
      template_name: schedule.templateName || '(no template)',
      record_total: patients.length,
      record_sent: 0,
      record_failed: 0,
      status: 'scheduled'
    };

    jobControllers[jobId] = { paused: false, cancelled: false };
    const existing = readJsonLog(jsonLogPath);
    existing.push(jobMeta);
    writeJsonLog(jsonLogPath, existing);

    try { event.reply('schedule-send-response', { success: true, message: 'Job queued', jobId, schedule: schedule.schedule, total: patients.length }); } catch (e) {}

    // Start async processing
    setTimeout(async () => {
      let sent = 0;
      let failed = 0;
      const allLogs = [];
      try {
        updateJobInLog(jsonLogPath, jobId, { status: 'in_progress' });

        // get rate limit settings
        const settings = loadSettings();
        const rateLimitPerMinute = Number(settings.rateLimitPerMinute) || 30;
        const perProfileLimitPerMinute = Number(settings.perProfileLimitPerMinute) || 20;
        const maxConsecutiveFailuresBeforePause = Number(settings.maxConsecutiveFailuresBeforePause) || 5;

        // Build list of available (logged-in) profiles
        const availableProfiles = Object.keys(activeClients || {});
        
        // Handle "All" profiles selection for schedules
        let effectiveRequestedProfiles = requestedProfiles.slice();
        if (effectiveRequestedProfiles.includes('All') || effectiveRequestedProfiles.length === 0) {
          effectiveRequestedProfiles = availableProfiles;
          console.log(`Schedule using all active profiles: ${effectiveRequestedProfiles.join(', ')}`);
        }
        
        const loggedProfiles = effectiveRequestedProfiles.filter(p => !!activeClients[p]);

        if ((!loggedProfiles || loggedProfiles.length === 0) && availableProfiles.length === 0) {
          updateJobInLog(jsonLogPath, jobId, { status: 'failed', error: 'No profiles logged in' });
          try { event.reply('schedule-message-finished', { id: jobId, total: patients.length, sent: 0, failed: patients.length, schedule: schedule.schedule }); } catch (e) {}
          delete jobControllers[jobId];
          return;
        }

        // Build profile-wise groups. For patients with an assigned profile, require that profile to be active.
        const groups = {}; // profileName -> [patient]
        let unassignedCounter = 0;

        const poolForUnassigned = (loggedProfiles && loggedProfiles.length) ? loggedProfiles : availableProfiles;

        for (const p of patients) {
          const rawAssigned = p.profile || p.Profile || p.assigned_profile || p.assignedProfile || '';
          const assigned = rawAssigned ? String(rawAssigned) : '';

          if (assigned) {
            if (activeClients[assigned]) {
              groups[assigned] = groups[assigned] || [];
              groups[assigned].push(p);
              if (!profileJobs[assigned]) profileJobs[assigned] = new Set();
              profileJobs[assigned].add(jobId);
            } else {
              // Assigned profile not active -> skip sending to this patient as requested
              failed++;
              allLogs.push({ Profile: assigned, Phone: p.phone, Status: 'Skipped', Timestamp: getLocalISOString(), Message: '', Error: `Assigned profile not active: ${assigned}` });
              updateJobInLog(jsonLogPath, jobId, { record_sent: sent, record_failed: failed });
              try { event.reply('schedule-message-progress', { id: jobId, profile: assigned, number: p.phone, status: 'skipped', sent, failed, total: patients.length, schedule: schedule.schedule, template: schedule.templateName }); } catch (e) {}
              try {
                const mid = 'ml_' + Date.now().toString() + '_' + Math.floor(Math.random() * 1000);
                insertMessageLog.run({
                  id: mid,
                  job_id: jobId,
                  unique_id: p.unique_id || p.uniqueId || null,
                  name: p.name || null,
                  phone: p.phone || null,
                  profile: assigned,
                  template: schedule.templateName || null,
                  message: '',
                  status: 'Skipped',
                  sent_at: getLocalISOString(),
                  error: `Assigned profile not active: ${assigned}`
                });
              } catch (e) { console.warn('insertMessageLog failed (assigned-inactive):', e && e.message ? e.message : e); }
            }
          } else {
            // unassigned: distribute among pool for unassigned (only active profiles)
            if (!poolForUnassigned || poolForUnassigned.length === 0) {
              failed++;
              allLogs.push({ Profile: '(none)', Phone: p.phone, Status: 'Skipped', Timestamp: getLocalISOString(), Message: '', Error: 'No active profiles available for unassigned patient' });
              updateJobInLog(jsonLogPath, jobId, { record_sent: sent, record_failed: failed });
              try { event.reply('schedule-message-progress', { id: jobId, profile: null, number: p.phone, status: 'skipped', sent, failed, total: patients.length, schedule: schedule.schedule, template: schedule.templateName }); } catch (e) {}
              try {
                const mid = 'ml_' + Date.now().toString() + '_' + Math.floor(Math.random() * 1000);
                insertMessageLog.run({
                  id: mid,
                  job_id: jobId,
                  unique_id: p.unique_id || p.uniqueId || null,
                  name: p.name || null,
                  phone: p.phone || null,
                  profile: null,
                  template: schedule.templateName || null,
                  message: '',
                  status: 'Skipped',
                  sent_at: getLocalISOString(),
                  error: 'No active profiles available for unassigned patient'
                });
              } catch (e) { console.warn('insertMessageLog failed (unassigned-skip):', e && e.message ? e.message : e); }
              continue;
            }
            const profileName = poolForUnassigned[unassignedCounter % poolForUnassigned.length];
            unassignedCounter++;
            groups[profileName] = groups[profileName] || [];
            groups[profileName].push(p);
            if (!profileJobs[profileName]) profileJobs[profileName] = new Set();
            profileJobs[profileName].add(jobId);
          }
        }

        // prepare template text
        let templateText = '';
        try { templateText = GettemplateText(schedule.templateName); } catch (e) { templateText = ''; }

        const updateLastStmt = db.prepare('UPDATE patients SET Last_Msgsent_date = ?, last_template = ?, last_schedule_days = ?, mod_date = ? WHERE unique_id = ? OR phone = ?');

        // Rate limit trackers
        const perProfileSentWindow = {}; // profile -> [timestamps]
        const perProfileFailures = {}; // profile -> consecutiveFailures
        const globalSentWindow = [];
        function pruneWindow(arr, windowMs) {
          const cutoff = Date.now() - windowMs;
          while (arr.length && arr[0] < cutoff) arr.shift();
        }
        async function rateLimitWait(profileName) {
          pruneWindow(globalSentWindow, 60000);
          if (!perProfileSentWindow[profileName]) perProfileSentWindow[profileName] = [];
          pruneWindow(perProfileSentWindow[profileName], 60000);

          while (globalSentWindow.length >= rateLimitPerMinute || perProfileSentWindow[profileName].length >= perProfileLimitPerMinute) {
            await new Promise(r => setTimeout(r, 1000));
            pruneWindow(globalSentWindow, 60000);
            pruneWindow(perProfileSentWindow[profileName], 60000);
          }
          const ts = Date.now();
          globalSentWindow.push(ts);
          perProfileSentWindow[profileName].push(ts);
        }

        // Render function used for template placeholders
        // Rules:
        //  - {{key}} -> exact header match only (case-insensitive). If no exact column found, leave placeholder unchanged.
        //  - {key}  -> legacy single-brace behavior but restricted to whitelist: name, phone, var1..var6 (case-insensitive, underscores/spaces ignored).
        function renderTemplateForPatient(tmpl, patient) {
          if (!tmpl) return '';
          const p = patient || {};
          const normalized = Object.keys(p).reduce((acc, k) => {
            acc[k] = p[k];
            acc[k.toLowerCase()] = p[k];
            acc[k.replace(/[_\s]/g, '').toLowerCase()] = p[k];
            return acc;
          }, {});

          const nameVal = normalized['name'] || normalized['fullname'] || normalized['full name'] || p.name || p.Name || '';
          const firstName = (String(nameVal || '').trim().split(/\s+/)[0]) || '';

          return String(tmpl).replace(/(\{\{\s*([^}]+?)\s*\}\}|\{\s*([^}]+?)\s*\})/g, (m, _all, g2, g3) => {
            const isDouble = !!g2; // true for {{key}}
            const rawKey = String((g2 || g3) || '').trim();
            const k = rawKey;
            const kl = k.toLowerCase();

            if (isDouble) {
              // exact-match only (case-insensitive)
              const keys = Object.keys(p || {});
              for (const candidate of keys) {
                if (String(candidate || '').trim().toLowerCase() === rawKey.toLowerCase()) {
                  return p[candidate] == null ? '' : String(p[candidate]);
                }
              }
              // not found -> return original placeholder unchanged
              return m;
            }

            // Single-brace: restrict substitutions ONLY to the allowed keys list
            const allowed = new Set(['name','phone','var1','var2','var3','var4','var5','var6']);
            const normKey = k.replace(/[_\s]/g, '').toLowerCase();
            if (allowed.has(normKey)) {
              // Prefer normalized lookup
              if (Object.prototype.hasOwnProperty.call(normalized, normKey)) return normalized[normKey] == null ? '' : String(normalized[normKey]);
              if (Object.prototype.hasOwnProperty.call(normalized, k)) return normalized[k] == null ? '' : String(normalized[k]);
              if (Object.prototype.hasOwnProperty.call(normalized, kl)) return normalized[kl] == null ? '' : String(normalized[kl]);
              if (k.includes('.')) { const parts = k.split('.'); let cur = p; for (const part of parts) { if (cur && Object.prototype.hasOwnProperty.call(cur, part)) cur = cur[part]; else { cur = ''; break; } } return cur == null ? '' : String(cur); }
              if (Object.prototype.hasOwnProperty.call(p, k)) return p[k] == null ? '' : String(p[k]);
              if (Object.prototype.hasOwnProperty.call(p, kl)) return p[kl] == null ? '' : String(p[kl]);
              return '';
            }

            // not an allowed single-brace key -> return empty (do not attempt fuzzy aliases)
            return '';
          });
        }

        // Process groups PARALLEL across profiles while maintaining all safety features
        const profileNames = Object.keys(groups);
        
        // Thread-safe shared counters for parallel processing
        const sharedCounters = { sent: 0, failed: 0 };
        const allLogsCollector = []; // Collect logs from all profiles
        const counterLock = { inUse: false }; // Simple lock for atomic operations
        
        // Helper function for thread-safe counter updates
        async function safeCounterUpdate(sentIncrement = 0, failedIncrement = 0, logEntry = null) {
          while (counterLock.inUse) {
            await new Promise(r => setTimeout(r, 10)); // Wait for lock
          }
          counterLock.inUse = true;
          try {
            sharedCounters.sent += sentIncrement;
            sharedCounters.failed += failedIncrement;
            if (logEntry) allLogsCollector.push(logEntry);
            // Update job log with current totals
            updateJobInLog(jsonLogPath, jobId, { 
              record_sent: sharedCounters.sent, 
              record_failed: sharedCounters.failed 
            });
          } finally {
            counterLock.inUse = false;
          }
        }

        // Process each profile in parallel
        const profilePromises = profileNames.map(async (profileName) => {
          const client = activeClients[profileName];
          const patientsForProfile = groups[profileName] || [];

          // Per-profile rate limiting (each profile maintains its own window)
          const profileSentWindow = [];
          function pruneProfileWindow() {
            const cutoff = Date.now() - 60000;
            while (profileSentWindow.length && profileSentWindow[0] < cutoff) profileSentWindow.shift();
          }
          async function profileRateLimitWait() {
            pruneProfileWindow();
            while (profileSentWindow.length >= perProfileLimitPerMinute) {
              await new Promise(r => setTimeout(r, 1000));
              pruneProfileWindow();
            }
            profileSentWindow.push(Date.now());
          }

          // Track consecutive failures for this profile
          let profileFailures = 0;

          // If this profile has already reached its daily limit, skip entire group
          try {
            const pLimit = getProfileDailyLimit(profileName);
            if (pLimit && Number(pLimit) > 0) {
              const pSent = getProfileDailySent(profileName);
              if (pSent >= Number(pLimit)) {
                const skipMsg = `Daily limit reached for profile ${profileName}`;
                
                // Send detailed feedback to user about daily limit reached
                try {
                  event.reply('daily-limit-reached', {
                    jobId: jobId,
                    profileName: profileName,
                    dailyLimit: pLimit,
                    sentToday: pSent,
                    message: `Daily limit reached for profile "${profileName}". Sent ${pSent}/${pLimit} messages today. All remaining messages for this profile will be skipped.`,
                    progress: {
                      sent: sharedCounters.sent,
                      failed: sharedCounters.failed,
                      total: patients.length
                    },
                    skippedCount: patientsForProfile.length
                  });
                } catch (e) {
                  console.warn('Failed to send daily-limit-reached event:', e && e.message ? e.message : e);
                }
                
                for (const p of patientsForProfile) {
                  const logEntry = { Profile: profileName, Phone: p.phone, Status: 'Skipped', Timestamp: getLocalISOString(), Message: '', Error: skipMsg };
                  await safeCounterUpdate(0, 1, logEntry);
                  try {
                    const mid = 'ml_' + Date.now().toString() + '_' + Math.floor(Math.random() * 1000);
                    insertMessageLog.run({ id: mid, job_id: jobId, unique_id: p.unique_id || p.uniqueId || null, name: p.name || null, phone: p.phone || null, profile: profileName, template: schedule.templateName || null, message: '', status: 'Skipped', sent_at: getLocalISOString(), error: skipMsg });
                  } catch (e) { console.warn('insertMessageLog failed (profile-limit):', e && e.message ? e.message : e); }
                  
                  // Send progress update for each skipped message
                  try {
                    event.reply('schedule-message-progress', { 
                      id: jobId, 
                      profile: profileName, 
                      number: p.phone, 
                      status: 'skipped', 
                      error: skipMsg,
                      sent: sharedCounters.sent, 
                      failed: sharedCounters.failed, 
                      total: patients.length, 
                      schedule: schedule.schedule, 
                      template: schedule.templateName 
                    });
                  } catch (e) {
                    console.warn('Failed to send schedule progress update for daily limit skip:', e && e.message ? e.message : e);
                  }
                }
                return; // exit this profile's processing
              }
            }
          } catch (e) { console.warn('Failed checking profile daily limit:', e && e.message ? e.message : e); }

          if (!client) {
            // All patients in this group should be marked failed (profile went offline mid-job)
            for (const p of patientsForProfile) {
              const logEntry = { Profile: profileName || '(none)', Phone: p.phone, Status: 'Failed', Timestamp: getLocalISOString(), Message: '', Error: 'Profile not logged in' };
              await safeCounterUpdate(0, 1, logEntry);
              try { event.reply('schedule-message-progress', { id: jobId, profile: profileName, number: p.phone, status: 'failed', sent: sharedCounters.sent, failed: sharedCounters.failed, total: patients.length, schedule: schedule.schedule, template: schedule.templateName }); } catch (e) {}
            }
            return; // exit this profile's processing
          }

          // Process each patient for this profile sequentially (maintaining delays within profile)
          for (const p of patientsForProfile) {
            const controller = jobControllers[jobId];
            if (!controller) break;
            if (controller.cancelled) break;
            while (controller.paused) { await new Promise(r => setTimeout(r, 1000)); }

            // Enforce per-profile rate limits
            try { await profileRateLimitWait(); } catch (e) {}

            // Per-profile daily limit check (recheck as it might have changed during parallel processing)
            try {
              const pLimit = getProfileDailyLimit(profileName);
              if (pLimit && Number(pLimit) > 0) {
                const pSent = getProfileDailySent(profileName);
                if (pSent >= Number(pLimit)) {
                  // Mark this patient as skipped due to profile limit
                  const skipMsg = `Daily limit reached for profile ${profileName}`;
                  const logEntry = { Profile: profileName, Phone: p.phone, Status: 'Skipped', Timestamp: getLocalISOString(), Message: '', Error: skipMsg };
                  await safeCounterUpdate(0, 1, logEntry);
                  try {
                    const mid = 'ml_' + Date.now().toString() + '_' + Math.floor(Math.random() * 1000);
                    insertMessageLog.run({ id: mid, job_id: jobId, unique_id: p.unique_id || p.uniqueId || null, name: p.name || null, phone: p.phone || null, profile: profileName, template: schedule.templateName || null, message: '', status: 'Skipped', sent_at: getLocalISOString(), error: skipMsg });
                  } catch (e) { console.warn('insertMessageLog failed (profile-limit skip):', e && e.message ? e.message : e); }

                  try { event.reply('schedule-message-progress', { id: jobId, profile: profileName, number: p.phone, status: 'skipped', sent: sharedCounters.sent, failed: sharedCounters.failed, total: patients.length, schedule: schedule.schedule, template: schedule.templateName }); } catch (e) {}
                  continue;
                }
              }
            } catch (e) { console.warn('Failed checking profile daily limit for scheduled send:', e && e.message ? e.message : e); }

            let attempt = 0;
            let success = false;
            let lastError = null;
            while (attempt <= 3 && !success) {
              attempt++;
              try {
                // Enhanced profile validation before sending
                const profileValidation = await validateProfileReadiness(profileName, client);
                if (!profileValidation.ready) {
                  throw new Error(`Profile validation failed: ${profileValidation.error}`);
                }
                // phone precheck
                const phoneCheck = await verifyAndFormatPhone(client, p.phone);
                if (!phoneCheck.valid) {
                  const reason = phoneCheck.reason || 'invalid';
                  const messageId = 'msg_' + getLocalISOString().replace(/[-:.TZ]/g, '') + '_' + Math.random().toString(36).substr(2, 6);
                  try {
                    insertMessageLog.run({
                      id: messageId,
                      job_id: jobId,
                      unique_id: p.unique_id || p.uniqueId || null,
                      name: p.name || null,
                      phone: p.phone || null,
                      profile: profileName,
                      template: schedule.templateName || null,
                      message: '',
                      status: 'Failed',
                      sent_at: getLocalISOString(),
                      error: `Invalid phone (${reason})`
                    });
                  } catch (e) { console.warn('insertMessageLog failed (sched-assigned invalid-phone):', e && e.message ? e.message : e); }
                  await safeCounterUpdate(0, 1, { Profile: profileName, Phone: p.phone, Status: 'Failed', Timestamp: getLocalISOString(), Message: '', Error: `Invalid phone (${reason})` });
                  // don't retry
                  break;
                }
                const jid = phoneCheck.jid;
                if (!templateText) throw new Error('Template not found');
                const rendered = renderTemplateForPatient(templateText, p);
                await sendMessageSafely(client, jid, rendered);
                success = true;
              } catch (err) {
                lastError = err;
                
                // Check if this is a WhatsApp validation error (number not on WhatsApp)
                if (err.message.includes('Number not registered on WhatsApp') || 
                    err.message.includes('not regist ered on WhatsApp') || 
                    err.message.includes('WhatsApp validation failed') ||
                    err.message.includes('Invalid number')) {
                  console.log(`❌ ${p.phone} is not on WhatsApp, skipping retries`);
                  // Don't retry for invalid WhatsApp numbers
                  break;
                }
                
                await new Promise(r => setTimeout(r, Math.min(30000, Math.pow(2, attempt) * 1000)));
              }
            }

            const timestamp = getLocalISOString();
            if (success) {
              try { incStat(profileName); } catch (e) {}
              // reset consecutive failure counter on success
              profileFailures = 0;
              const renderedMessage = renderTemplateForPatient(templateText, p);
              const logEntry = { Profile: profileName, Phone: p.phone, Status: 'Sent', Timestamp: timestamp, Message: renderedMessage, Error: '' };
              await safeCounterUpdate(1, 0, logEntry);
              
              try {
                const mid = 'ml_' + Date.now().toString() + '_' + Math.floor(Math.random() * 1000);
                insertMessageLog.run({
                  id: mid,
                  job_id: jobId,
                  unique_id: p.unique_id || p.uniqueId || null,
                  name: p.name || null,
                  phone: p.phone || null,
                  profile: profileName,
                  template: schedule.templateName || null,
                  message: renderedMessage || null,
                  status: 'Sent',
                  sent_at: timestamp,
                  error: ''
                });
              } catch (e) { console.warn('insertMessageLog failed (sent):', e && e.message ? e.message : e); }
              
              // Also log to JSON file for reports
              try {
                const existingLogs = readJsonLog();
                existingLogs.push({
                  Profile: profileName,
                  Phone: p.phone,
                  Status: 'Sent',
                  Timestamp: timestamp,
                  Message: renderedMessage,
                  Template: schedule.templateName || '',
                  CustomerName: p.name || ''
                });
                writeJsonLog(null, existingLogs);
              } catch (e) { console.warn('JSON log failed:', e && e.message ? e.message : e); }
              
              try { updateLastStmt.run(timestamp, schedule.templateName || '', schedule.days || 0, timestamp, p.unique_id || '', p.phone || ''); } catch (e) {}

              try { event.reply('schedule-message-progress', { id: jobId, profile: profileName, number: p.phone, status: 'sent', sent: sharedCounters.sent, failed: sharedCounters.failed, total: patients.length, schedule: schedule.schedule, template: schedule.templateName }); } catch (e) {}
            } else {
              // increment consecutive failure counter
              profileFailures++;
              const renderedMessage = renderTemplateForPatient(templateText, p);
              const logEntry = { Profile: profileName, Phone: p.phone, Status: 'Failed', Timestamp: timestamp, Message: renderedMessage, Error: lastError ? String(lastError.message) : 'Unknown' };
              await safeCounterUpdate(0, 1, logEntry);
              
              try {
                const mid = 'ml_' + Date.now().toString() + '_' + Math.floor(Math.random() * 1000);
                insertMessageLog.run({
                  id: mid,
                  job_id: jobId,
                  unique_id: p.unique_id || p.uniqueId || null,
                  name: p.name || null,
                  phone: p.phone || null,
                  profile: profileName,
                  template: schedule.templateName || null,
                  message: renderedMessage || null,
                  status: 'Failed',
                  sent_at: timestamp,
                  error: lastError ? (lastError.message || String(lastError)) : 'Unknown'
                });
              } catch (e) { console.warn('insertMessageLog failed (failed):', e && e.message ? e.message : e); }

              // Also log failed message to JSON file for reports
              try {
                const existingLogs = readJsonLog();
                existingLogs.push({
                  Profile: profileName,
                  Phone: p.phone,
                  Status: 'Failed',
                  Timestamp: timestamp,
                  Message: renderedMessage,
                  Template: schedule.templateName || '',
                  CustomerName: p.name || ''
                });
                writeJsonLog(null, existingLogs);
              } catch (e) { console.warn('JSON log failed (failed):', e && e.message ? e.message : e); }

              try { event.reply('schedule-message-progress', { id: jobId, profile: profileName, number: p.phone, status: 'failed', error: lastError ? String(lastError.message) : 'Unknown', sent: sharedCounters.sent, failed: sharedCounters.failed, total: patients.length, schedule: schedule.schedule, template: schedule.templateName }); } catch (e) {}

              // Check if profile has hit consecutive failure limit - pause job if so
              if (profileFailures >= maxConsecutiveFailuresBeforePause) {
                console.warn(`Pausing job ${jobId} due to ${profileFailures} consecutive failures for profile ${profileName}`);
                jobControllers[jobId].paused = true;
                updateJobInLog(jsonLogPath, jobId, { status: 'paused', paused: true });
                
                // Send detailed feedback to user about consecutive failures
                try { 
                  event.reply('job-paused', {
                    jobId: jobId,
                    reason: 'consecutive_failures',
                    profile: profileName,
                    message: `Schedule job paused due to ${profileFailures} consecutive failures on profile "${profileName}". This is a safety measure to prevent account restrictions.`,
                    sent: sharedCounters.sent,
                    failed: sharedCounters.failed,
                    total: patients.length,
                    schedule: schedule.schedule
                  });
                  
                  // Also send schedule progress update
                  event.reply('schedule-message-progress', {
                    id: jobId,
                    profile: profileName,
                    status: 'paused',
                    message: `Job paused due to safety limits on profile "${profileName}"`,
                    sent: sharedCounters.sent,
                    failed: sharedCounters.failed,
                    total: patients.length,
                    schedule: schedule.schedule,
                    template: schedule.templateName,
                    paused: true
                  });
                } catch (e) {}
                
                // Exit the patient loop for this profile to avoid further failures
                break;
              }
            }

            // Enhanced adaptive delay with human-like patterns  
            const messageCount = profileSentWindow.length || 0;
            const delayBetween = getAdaptiveDelay(profileName, messageCount);
            await new Promise(r => setTimeout(r, delayBetween));
          }
        });

        // Wait for all profiles to complete processing in parallel
        await Promise.all(profilePromises);
        
        // Update final counts from shared counters
        sent = sharedCounters.sent;
        failed = sharedCounters.failed;
        allLogs.push(...allLogsCollector);

        // cleanup profileJobs entries for this job
        for (const pName of Object.keys(profileJobs || {})) { try { profileJobs[pName].delete(jobId); } catch (e) {} }

        // write excel log
        try {
          const logDir1 = path.join(getUserDataDir(), 'logs');
          try { fs.mkdirSync(logDir1, { recursive: true }); } catch (e) {}
          const ts = getLocalISOString().replace(/[:.]/g, '-');
          const fname = `log-schedule-${ts}.xlsx`;
          const fpath = path.join(logDir1, fname);
          const worksheet2 = XLSX.utils.json_to_sheet(allLogs);
          const logWorkbook = XLSX.utils.book_new();
          XLSX.utils.book_append_sheet(logWorkbook, worksheet2, 'Message Log');
          XLSX.writeFile(logWorkbook, fpath);
          updateJobInLog(jsonLogPath, jobId, { record_sent: sent, record_failed: failed, status: 'completed', path: fpath });
          try { event.reply('schedule-message-finished', { id: jobId, total: patients.length, sent, failed, path: fpath, schedule: schedule.schedule }); } catch (e) {}
        } catch (e) {
          updateJobInLog(jsonLogPath, jobId, { record_sent: sent, record_failed: failed, status: 'completed' });
          try { event.reply('schedule-message-finished', { id: jobId, total: patients.length, sent, failed, schedule: schedule.schedule }); } catch (e) {}
        }

      } catch (err) {
        console.error('send-schedule-now-assigned failed inner:', err && err.message ? err.message : err);
        updateJobInLog(jsonLogPath, jobId, { status: 'failed', error: String(err && err.message ? err.message : err) });
        try { event.reply('schedule-message-finished', { id: jobId, total: patients.length, sent: 0, failed: patients.length, schedule: schedule.schedule, error: err && err.message ? err.message : String(err) }); } catch (e) {}
      }
      delete jobControllers[jobId];
    }, 0);

  } catch (e) {
    console.error('send-schedule-now-assigned handler error:', e && e.message ? e.message : e);
    try { event.reply('schedule-send-response', { success: false, message: e && e.message ? e.message : String(e) }); } catch (err) {}
  }
});
const reportFile = getJsonLogsPath();

// IPC to return report logs to renderer
ipcMain.handle('get-reports', () => {
  try {
    const reports = readJsonLog() || [];
    // Enrich each report with count of 'Not WhatsApp' records from message_logs table
    return (reports || []).map(r => {
      try {
        if (!r || !r.id) return r;
        // Count rows where job_id matches and error mentions Not WhatsApp (case-insensitive)
        const row = db.prepare("SELECT COUNT(*) as cnt FROM message_logs WHERE job_id = ? AND lower(error) LIKE ?").get(r.id, '%not whatsapp%');
        r.record_not_whatsapp = row ? (row.cnt || 0) : 0;
      } catch (e) {
        // On any DB error, set to 0 and continue
        r.record_not_whatsapp = 0;
      }
      return r;
    });
  } catch (e) {
    console.error('get-reports failed to read/enrich reports:', e && e.message ? e.message : e);
    return readJsonLog();
  }
});

// Return failed message logs for a given job id
ipcMain.handle('get-failed-message-logs', (event, jobId) => {
  try {
    if (!jobId) return [];
    const rows = db.prepare("SELECT * FROM message_logs WHERE job_id = ? AND status = 'Failed' ORDER BY sent_at ASC").all(jobId);
    return rows || [];
  } catch (e) {
    console.error('get-failed-message-logs failed:', e && e.message ? e.message : e);
    return [];
  }
});

// Return all message logs for a given job id (all statuses)
ipcMain.handle('get-report-records', (event, jobId) => {
  try {
    if (!jobId) return [];
    const rows = db.prepare("SELECT * FROM message_logs WHERE job_id = ? ORDER BY sent_at ASC").all(jobId);
    return rows || [];
  } catch (e) {
    console.error('get-report-records failed:', e && e.message ? e.message : e);
    return [];
  }
});

// Export report records (for a job) to an Excel file chosen by the user
ipcMain.handle('export-report-records', async (event, { jobId } = {}) => {
  try {
    if (!jobId) return { success: false, error: 'No jobId provided' };
    const rows = db.prepare("SELECT * FROM message_logs WHERE job_id = ? ORDER BY sent_at ASC").all(jobId) || [];

    // Convert to worksheet
    const ws = XLSX.utils.json_to_sheet(rows);
    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, 'Report');

    const now = new Date();
    const timestamp = now.toISOString().replace(/[:.]/g, '-');
    const defaultName = `report_${jobId}_${timestamp}.xlsx`;

    const { canceled, filePath: destPath } = await dialog.showSaveDialog({
      title: 'Export report to Excel',
      defaultPath: defaultName,
      buttonLabel: 'Export'
    });

    if (canceled || !destPath) return { success: false, error: 'Save canceled' };

    XLSX.writeFile(wb, destPath);
    return { success: true, path: destPath };
  } catch (e) {
    console.error('export-report-records error:', e && e.message ? e.message : e);
    return { success: false, error: e && e.message ? e.message : String(e) };
  }
});

// Resend selected failed message logs. Expects an array of message_log rows or ids.
ipcMain.on('resend-failed-message-logs', async (event, payload) => {
  try {
    // payload can be { rows: [...], ids: [...] }
    const rows = Array.isArray(payload && payload.rows) ? payload.rows : null;
    const ids = Array.isArray(payload && payload.ids) ? payload.ids : null;
    let toProcess = [];

    if (rows && rows.length > 0) toProcess = rows.slice();
    else if (ids && ids.length > 0) {
      // fetch rows by ids
      const placeholders = ids.map(() => '?').join(',');
      const stmt = db.prepare(`SELECT * FROM message_logs WHERE id IN (${placeholders})`);
      toProcess = stmt.all(...ids);
    } else {
      event.reply('resend-failed-response', { success: false, message: 'No rows or ids provided' });
      return;
    }

    if (toProcess.length === 0) {
      event.reply('resend-failed-response', { success: true, processed: 0, message: 'Nothing to resend' });
      return;
    }

    let processed = 0;
    let sent = 0;
    let failed = 0;

    // Prepared statement to update existing message_log rows when resending
    const updateMessageLogStmt = db.prepare(`
      UPDATE message_logs SET
        job_id = @job_id,
        unique_id = @unique_id,
        name = @name,
        phone = @phone,
        profile = @profile,
        template = @template,
        message = @message,
        status = @status,
        sent_at = @sent_at,
        error = @error,
        media_path = @media_path,
        media_filename = @media_filename
      WHERE id = @id
    `);

    for (const row of toProcess) {
      try {
        // If the original job has a controller, respect its paused/cancelled flags
        try {
          if (row && row.job_id && jobControllers && jobControllers[row.job_id]) {
            const controller = jobControllers[row.job_id];
            // while paused, wait (but keep sending small progress updates so UI stays responsive)
            while (controller.paused) {
              try { event.reply('resend-failed-progress', { total: toProcess.length, processed, sent, failed, row, paused: true }); } catch (e) {}
              await new Promise(r => setTimeout(r, 500));
            }
            // if cancelled while waiting or at any point, abort the resend operation
            if (controller.cancelled) {
              try { event.reply('resend-failed-response', { success: false, processed, sent, failed, cancelled: true, message: 'Resend cancelled by job controller' }); } catch (e) {}
              return;
            }
          }
        } catch (e) {
          // non-fatal - continue processing this row
          console.warn('resend loop controller check failed:', e && e.message ? e.message : e);
        }

        const profile = row.profile || null;
        const client = profile ? activeClients[profile] : null;

        if (!client) {
          failed++;
          processed++;
          // Update existing failed row with latest attempt info (fallback to insert if no id)
          try {
            if (row && row.id) {
              updateMessageLogStmt.run({
                id: row.id,
                job_id: row.job_id || null,
                unique_id: row.unique_id || null,
                name: row.name || null,
                phone: row.phone || null,
                profile: profile,
                template: row.template || null,
                message: row.message || null,
                status: 'Failed',
                sent_at: getLocalISOString(),
                error: 'Resend failed: profile not logged in'
              });
            } else {
              const mid = 'resend_' + Date.now().toString() + '_' + Math.floor(Math.random() * 1000);
              insertMessageLog.run({ id: mid, job_id: row.job_id || null, unique_id: row.unique_id || null, name: row.name || null, phone: row.phone || null, profile: profile, template: row.template || null, message: row.message || null, status: 'Failed', sent_at: getLocalISOString(), error: 'Resend failed: profile not logged in' });
            }
          } catch (e) {
            console.warn('Failed to update message_log for profile-not-logged-in:', e && e.message ? e.message : e);
          }
          event.reply('resend-failed-progress', { total: toProcess.length, processed, sent, failed, row });
          continue;
        }

        // verify phone
        const phoneCheck = await verifyAndFormatPhone(client, row.phone);
        if (!phoneCheck.valid) {
          failed++;
          processed++;
          const reason = phoneCheck.reason || 'invalid';
          try {
            if (row && row.id) {
              updateMessageLogStmt.run({
                id: row.id,
                job_id: row.job_id || null,
                unique_id: row.unique_id || null,
                name: row.name || null,
                phone: row.phone || null,
                profile: profile,
                template: row.template || null,
                message: row.message || null,
                status: 'Failed',
                sent_at: getLocalISOString(),
                error: `Resend invalid phone (${reason})`
              });
            } else {
              const mid = 'resend_' + Date.now().toString() + '_' + Math.floor(Math.random() * 1000);
              insertMessageLog.run({ id: mid, job_id: row.job_id || null, unique_id: row.unique_id || null, name: row.name || null, phone: row.phone || null, profile: profile, template: row.template || null, message: row.message || null, status: 'Failed', sent_at: getLocalISOString(), error: `Resend invalid phone (${reason})` });
            }
          } catch (e) {
            console.warn('Failed to update message_log for invalid-phone:', e && e.message ? e.message : e);
          }
          event.reply('resend-failed-progress', { total: toProcess.length, processed, sent, failed, row });
          continue;
        }

        const jid = phoneCheck.jid;
        // message to send: prefer stored rendered message, fallback to template text
        let messageToSend = row.message || '';
        if ((!messageToSend || messageToSend.trim() === '') && row.template) {
          try {
            const t = getTemplateData(row.template);
            messageToSend = t && t.message ? t.message : '';
          } catch (e) {
            messageToSend = '';
          }
        }

        // allow media-only resends: check stored media path
        const rowMediaPath = row.media_path || null;
        const rowMediaFilename = row.media_filename || (row.media_path ? path.basename(row.media_path) : null);

        if ((!messageToSend || messageToSend.trim() === '') && !rowMediaPath) {
          // Nothing to send - mark as failed
          failed++;
          processed++;
          try {
            if (row && row.id) {
              updateMessageLogStmt.run({ id: row.id, job_id: row.job_id || null, unique_id: row.unique_id || null, name: row.name || null, phone: row.phone || null, profile: profile, template: row.template || null, message: null, status: 'Failed', sent_at: getLocalISOString(), error: 'Resend failed: no message/template/media available', media_path: rowMediaPath, media_filename: rowMediaFilename });
            } else {
              const mid = 'resend_' + Date.now().toString() + '_' + Math.floor(Math.random() * 1000);
              insertMessageLog.run({ id: mid, job_id: row.job_id || null, unique_id: row.unique_id || null, name: row.name || null, phone: row.phone || null, profile: profile, template: row.template || null, message: null, status: 'Failed', sent_at: getLocalISOString(), error: 'Resend failed: no message/template/media available', media_path: rowMediaPath, media_filename: rowMediaFilename });
            }
          } catch (e) {
            console.warn('Failed to update message_log for no-message:', e && e.message ? e.message : e);
          }
          event.reply('resend-failed-progress', { total: toProcess.length, processed, sent, failed, row });
          continue;
        }

        // attempt send (prefer media if available)
        try {
          if (rowMediaPath && fs.existsSync(rowMediaPath)) {
            // send media using stored file
            const { MessageMedia } = require('whatsapp-web.js');
            let mediaData;
            try {
              mediaData = MessageMedia.fromFilePath(rowMediaPath);
            } catch (e) {
              // fallback: try reading buffer and creating MessageMedia from base64
              try {
                const buf = fs.readFileSync(rowMediaPath);
                const b64 = buf.toString('base64');
                const ext = (path.extname(rowMediaPath) || '').toLowerCase().replace('.', '');
                let mime = 'application/octet-stream';
                if (ext === 'jpg' || ext === 'jpeg') mime = 'image/jpeg';
                else if (ext === 'png') mime = 'image/png';
                else if (ext === 'gif') mime = 'image/gif';
                else if (ext === 'webp') mime = 'image/webp';
                else if (ext === 'pdf') mime = 'application/pdf';
                mediaData = new MessageMedia(mime, b64, rowMediaFilename || null);
              } catch (ee) { mediaData = null; }
            }

            if (mediaData) {
              if (messageToSend && messageToSend.trim() !== '') {
                await sendMessageSafely(client, jid, mediaData, { caption: messageToSend });
              } else {
                await sendMessageSafely(client, jid, mediaData);
              }

              // update existing log to Sent (or insert fallback) with media info
              try {
                if (row && row.id) {
                  updateMessageLogStmt.run({ id: row.id, job_id: row.job_id || null, unique_id: row.unique_id || null, name: row.name || null, phone: row.phone || null, profile: profile, template: row.template || null, message: messageToSend ? String(messageToSend).substring(0, 500) : null, status: 'Sent', sent_at: getLocalISOString(), error: null, media_path: rowMediaPath, media_filename: rowMediaFilename });
                } else {
                  const mid = 'resend_' + Date.now().toString() + '_' + Math.floor(Math.random() * 1000);
                  insertMessageLog.run({ id: mid, job_id: row.job_id || null, unique_id: row.unique_id || null, name: row.name || null, phone: row.phone || null, profile: profile, template: row.template || null, message: messageToSend ? String(messageToSend).substring(0, 500) : null, status: 'Sent', sent_at: getLocalISOString(), error: null, media_path: rowMediaPath, media_filename: rowMediaFilename });
                }
              } catch (e) { console.warn('Failed to update message_log as Sent (media):', e && e.message ? e.message : e); }
            } else {
              // media couldn't be read - fallback to text send if available
              if (messageToSend && messageToSend.trim() !== '') {
                await sendMessageSafely(client, jid, messageToSend);
                if (row && row.id) {
                  updateMessageLogStmt.run({ id: row.id, job_id: row.job_id || null, unique_id: row.unique_id || null, name: row.name || null, phone: row.phone || null, profile: profile, template: row.template || null, message: messageToSend ? String(messageToSend).substring(0, 500) : null, status: 'Sent', sent_at: getLocalISOString(), error: null, media_path: rowMediaPath, media_filename: rowMediaFilename });
                }
              } else {
                throw new Error('Resend media unavailable and no text fallback');
              }
            }
          } else {
            // No media file available; send as text
            await sendMessageSafely(client, jid, messageToSend);

            // update existing log to Sent (or insert fallback)
            try {
              if (row && row.id) {
                updateMessageLogStmt.run({ id: row.id, job_id: row.job_id || null, unique_id: row.unique_id || null, name: row.name || null, phone: row.phone || null, profile: profile, template: row.template || null, message: messageToSend ? String(messageToSend).substring(0, 500) : null, status: 'Sent', sent_at: getLocalISOString(), error: null, media_path: rowMediaPath, media_filename: rowMediaFilename });
              } else {
                const mid = 'resend_' + Date.now().toString() + '_' + Math.floor(Math.random() * 1000);
                insertMessageLog.run({ id: mid, job_id: row.job_id || null, unique_id: row.unique_id || null, name: row.name || null, phone: row.phone || null, profile: profile, template: row.template || null, message: messageToSend ? String(messageToSend).substring(0, 500) : null, status: 'Sent', sent_at: getLocalISOString(), error: null, media_path: rowMediaPath, media_filename: rowMediaFilename });
              }
            } catch (e) { console.warn('Failed to update message_log as Sent:', e && e.message ? e.message : e); }
          }

          // update patient last msg date
          try {
            const ts = getLocalISOString();
            const updateLastStmt = db.prepare('UPDATE patients SET Last_Msgsent_date = ?, mod_date = ? WHERE unique_id = ? OR phone = ?');
            updateLastStmt.run(ts, ts, row.unique_id || '', row.phone || '');
          } catch (e) {}

          sent++;
          processed++;
          event.reply('resend-failed-progress', { total: toProcess.length, processed, sent, failed, row });
        } catch (sendErr) {
          failed++;
          processed++;
          try {
            if (row && row.id) {
              updateMessageLogStmt.run({ id: row.id, job_id: row.job_id || null, unique_id: row.unique_id || null, name: row.name || null, phone: row.phone || null, profile: profile, template: row.template || null, message: row.message || null, status: 'Failed', sent_at: getLocalISOString(), error: `Resend send error: ${sendErr && sendErr.message ? sendErr.message : String(sendErr)}` });
            } else {
              const mid = 'resend_' + Date.now().toString() + '_' + Math.floor(Math.random() * 1000);
              insertMessageLog.run({ id: mid, job_id: row.job_id || null, unique_id: row.unique_id || null, name: row.name || null, phone: row.phone || null, profile: profile, template: row.template || null, message: row.message || null, status: 'Failed', sent_at: getLocalISOString(), error: `Resend send error: ${sendErr && sendErr.message ? sendErr.message : String(sendErr)}` });
            }
          } catch (e) {
            console.warn('Failed to update message_log for send error:', e && e.message ? e.message : e);
          }
          event.reply('resend-failed-progress', { total: toProcess.length, processed, sent, failed, row, error: sendErr && sendErr.message ? sendErr.message : String(sendErr) });
        }

        // small pause between resends to reduce rate-limit risk
        await new Promise(r => setTimeout(r, 1200));

      } catch (e) {
        // catch per-row unexpected errors
        failed++;
        processed++;
        try {
          if (row && row.id) {
            updateMessageLogStmt.run({ id: row.id, job_id: row.job_id || null, unique_id: row.unique_id || null, name: row.name || null, phone: row.phone || null, profile: row.profile || null, template: row.template || null, message: row.message || null, status: 'Failed', sent_at: getLocalISOString(), error: `Resend unexpected error: ${e && e.message ? e.message : e}` });
          } else {
            const mid = 'resend_' + Date.now().toString() + '_' + Math.floor(Math.random() * 1000);
            insertMessageLog.run({ id: mid, job_id: row.job_id || null, unique_id: row.unique_id || null, name: row.name || null, phone: row.phone || null, profile: row.profile || null, template: row.template || null, message: row.message || null, status: 'Failed', sent_at: getLocalISOString(), error: `Resend unexpected error: ${e && e.message ? e.message : e}` });
          }
        } catch (xx) {}
        event.reply('resend-failed-progress', { total: toProcess.length, processed, sent, failed, row, error: e && e.message ? e.message : String(e) });
      }
    }

    // after processing all, update JSON job summaries for affected job_ids
    try {
      const jsonPath = getJsonLogsPath();
      const logs = readJsonLog(jsonPath);
      const jobMap = {}; // job_id -> {sentInc, failedInc}
      for (const r of toProcess) {
        const jid = r.job_id || null;
        if (!jid) continue;
        jobMap[jid] = jobMap[jid] || { sentInc: 0, failedInc: 0 };
      }
      // Recompute counts from DB for each job and write back
      for (const jid of Object.keys(jobMap)) {
        try {
          const sentRow = db.prepare("SELECT COUNT(*) as cnt FROM message_logs WHERE job_id = ? AND status = 'Sent'").get(jid);
          const failedRow = db.prepare("SELECT COUNT(*) as cnt FROM message_logs WHERE job_id = ? AND status = 'Failed'").get(jid);
          const recSent = sentRow ? (sentRow.cnt || 0) : 0;
          const recFailed = failedRow ? (failedRow.cnt || 0) : 0;
          const idx = logs.findIndex(x => x.id === jid);
          if (idx !== -1) {
            logs[idx].record_sent = recSent;
            logs[idx].record_failed = recFailed;
          }
        } catch (e) { console.warn('Failed to recompute job counts after resend:', e && e.message ? e.message : e); }
      }
      try { writeJsonLog(jsonPath, logs); event.reply('reports-updated', logs); } catch (e) {}
    } catch (e) {}

    event.reply('resend-failed-response', { success: true, processed, sent, failed });
  } catch (err) {
    console.error('resend-failed-message-logs error:', err && err.message ? err.message : err);
    try { event.reply('resend-failed-response', { success: false, message: err && err.message ? err.message : String(err) }); } catch (e) {}
  }
});

// return daily stats for last N days
ipcMain.handle('get-daily-stats', (event, days = 7) => {
  try {
    return getDailyStats(days);
  } catch (e) { console.error('get-daily-stats failed', e.message); return []; }
});

// Get today's message counts per profile
ipcMain.handle('get-today-profile-counts', () => {
  try {
    const today = new Date().toISOString().slice(0,10);
    const row = db.prepare('SELECT per_profile FROM daily_stats WHERE day = ?').get(today);
    if (row && row.per_profile) {
      return JSON.parse(row.per_profile);
    }
    return {}; // No data for today
  } catch (e) {
    console.error('get-today-profile-counts failed', e.message);
    return {};
  }
});

// Get in-progress message counts per profile
ipcMain.handle('get-inprogress-profile-counts', () => {
  try {
    const reports = readJsonLog();
    const today = new Date().toISOString().slice(0, 10); // Get today's date (YYYY-MM-DD)
    console.log('📊 Total jobs in log:', reports.length, '| Checking for today:', today);
    
    const inProgressCounts = {};
    
    // Find all in-progress jobs FROM TODAY ONLY
    const activeJobs = reports.filter(job => {
      const status = (job.status || '').toLowerCase();
      const isActive = status === 'in_progress' || status === 'scheduled';
      
      if (!isActive) return false;
      
      // Check if job is from today
      const jobDate = job.timestamp ? job.timestamp.slice(0, 10) : '';
      const isFromToday = jobDate === today;
      
      if (isActive && isFromToday) {
        console.log(`📈 Today's active job found: ${job.id} - Status: ${status} - Date: ${jobDate} - Total: ${job.record_total} - Sent: ${job.record_sent}`);
      } else if (isActive && !isFromToday) {
        console.log(`📅 Old active job ignored: ${job.id} - Status: ${status} - Date: ${jobDate} (not today)`);
      }
      
      return isActive && isFromToday;
    });
    
    console.log('📊 Active jobs found:', activeJobs.length);
    
    if (activeJobs.length === 0) {
      console.log('✅ No active jobs - returning empty counts');
      return {};
    }
    
    // Calculate remaining messages per profile for active jobs
    for (const job of activeJobs) {
      const profiles = Array.isArray(job.profiles) ? job.profiles : [job.profiles].filter(Boolean);
      const totalRecords = job.record_total || 0;
      const sentRecords = job.record_sent || 0;
      const remainingInJob = Math.max(0, totalRecords - sentRecords);
      
      console.log(`📈 Job ${job.id}: ${remainingInJob} remaining (${totalRecords} total - ${sentRecords} sent) across ${profiles.length} profiles`);
      
      // Distribute remaining messages across profiles (simple equal distribution)
      const messagesPerProfile = profiles.length > 0 ? Math.ceil(remainingInJob / profiles.length) : 0;
      
      for (const profile of profiles) {
        if (profile && profile.trim()) {
          inProgressCounts[profile] = (inProgressCounts[profile] || 0) + messagesPerProfile;
          console.log(`📈 Profile ${profile}: +${messagesPerProfile} messages (total now: ${inProgressCounts[profile]})`);
        }
      }
    }
    
    console.log('📊 Final in-progress profile counts:', inProgressCounts);
    return inProgressCounts;
  } catch (e) {
    console.error('❌ get-inprogress-profile-counts failed:', e.message);
    return {};
  }
});

// Settings and alerts IPC
ipcMain.handle('get-settings', () => {
  return loadSettings();
});

ipcMain.handle('save-settings', (event, settings) => {
  const ok = saveSettings(settings);
  return { success: ok };
});

ipcMain.handle('get-alerts', () => {
  return getAlerts(500);
});

// Provide package.json information (version/release date) to renderer
ipcMain.handle('get-app-package', () => {
  try {
    const pkg = require(path.join(__dirname, 'package.json'));
    pkg.releaseDate = pkg.releaseDate || pkg.buildDate || '';
    return pkg;
  } catch (e) {
    console.error('Error fetching package.json:', e.message);
    return { version: 'N/A', releaseDate: 'N/A' };
  }
});
// Clear all alerts from database
ipcMain.handle('clear-alerts', () => {
  try {
    if (typeof db !== 'undefined' && db) {
      const stmt = db.prepare('DELETE FROM alerts');
      const result = stmt.run();
      console.log(`✅ Cleared ${result.changes} alerts from database`);
      return { success: true, cleared: result.changes };
    } else {
      console.error('❌ Database not available for clearing alerts');
      return { success: false, error: 'Database not available' };
    }
  } catch (e) {
    console.error('❌ Failed to clear alerts from database:', e.message);
    return { success: false, error: e.message };
  }
});

// Clear all logs and files from specified folders
ipcMain.handle('clear-all-logs', () => {
  try {
    console.log('🗑️ Starting clear all logs operation...');
    
    const userDataPath = getUserDataDir();
    console.log('🗂️ User data path:', userDataPath);
    
    // Define folders to clean (keep folders, delete contents)
    // All folders are inside userDataPath, not project root
    const foldersToClean = [
      'exports',
      'Jsonlogs', 
      'logs',
      'patientexcel',      // Changed from 'patientExcel' to match actual folder
      'uploadedexcel',
      'uploadedImages',
      'dndExcel'
    ];
    
    let totalFilesDeleted = 0;
    let totalDatabaseRecords = 0;
    const results = {
      success: true,
      files: 0,
      reports: 0,
      logs: 0,
      folders: []
    };
    
    // Clean each folder - all folders are inside userDataPath
    for (const folderName of foldersToClean) {
      try {
        const folderPath = path.join(userDataPath, folderName);
        
        if (fs.existsSync(folderPath)) {
          console.log(`🔄 Cleaning folder: ${folderPath}`);
          
          // Get all files in the folder
          const files = fs.readdirSync(folderPath);
          let folderFilesDeleted = 0;
          
          for (const file of files) {
            const filePath = path.join(folderPath, file);
            const stat = fs.statSync(filePath);
            
            if (stat.isFile()) {
              try {
                fs.unlinkSync(filePath);
                folderFilesDeleted++;
                console.log(`✅ Deleted: ${filePath}`);
              } catch (fileErr) {
                console.warn(`⚠️ Failed to delete file ${filePath}:`, fileErr.message);
              }
            } else if (stat.isDirectory()) {
              // Recursively delete subdirectory contents but keep the subdirectory
              try {
                const subFiles = fs.readdirSync(filePath);
                for (const subFile of subFiles) {
                  const subFilePath = path.join(filePath, subFile);
                  if (fs.statSync(subFilePath).isFile()) {
                    fs.unlinkSync(subFilePath);
                    folderFilesDeleted++;
                  }
                }
                console.log(`✅ Cleaned subdirectory: ${filePath}`);
              } catch (subErr) {
                console.warn(`⚠️ Failed to clean subdirectory ${filePath}:`, subErr.message);
              }
            }
          }
          
          totalFilesDeleted += folderFilesDeleted;
          results.folders.push({
            name: folderName,
            path: folderPath,
            filesDeleted: folderFilesDeleted
          });
          
          console.log(`✅ Cleaned ${folderName}: ${folderFilesDeleted} files deleted`);
        } else {
          console.log(`⚠️ Folder not found: ${folderPath}`);
          results.folders.push({
            name: folderName,
            path: folderPath,
            filesDeleted: 0,
            note: 'Folder not found'
          });
        }
      } catch (folderErr) {
        console.error(`❌ Error cleaning folder ${folderName}:`, folderErr.message);
        results.folders.push({
          name: folderName,
          error: folderErr.message
        });
      }
    }
    
    // Clear only message_logs table from database (other tables preserved)
    if (typeof db !== 'undefined' && db) {
      try {
        // Clear message logs from database
        const logsStmt = db.prepare('DELETE FROM message_logs');
        const logsResult = logsStmt.run();
        results.logs = logsResult.changes || 0;
        totalDatabaseRecords += results.logs;
        console.log(`✅ Cleared ${results.logs} message logs from database`);
        console.log('ℹ️ Other database tables preserved (patients, profiles, schedules, etc.)');
      } catch (logTableErr) {
        console.log('ℹ️ Message logs table not found or empty');
        results.logs = 0;
      }
    }
    
    // Clear JSON log files that contain reports data
    try {
      const jsonLogsPath = getJsonLogsPath();
      if (fs.existsSync(jsonLogsPath)) {
        // Clear the reports JSON file by writing empty array
        writeJsonLog(jsonLogsPath, []);
        console.log(`✅ Cleared JSON reports log: ${jsonLogsPath}`);
        results.reports = 1; // Count only the JSON file clearing
      }
    } catch (jsonErr) {
      console.warn('⚠️ Failed to clear JSON reports log:', jsonErr.message);
    }
    
    results.files = totalFilesDeleted;
    
    console.log(`🎉 Clear all logs completed!`);
    console.log(`📊 Summary: ${totalFilesDeleted} files deleted, ${totalDatabaseRecords} message logs cleared from database`);
    
    return results;
    
  } catch (e) {
    console.error('❌ Failed to clear all logs:', e.message);
    return { 
      success: false, 
      error: e.message,
      files: 0,
      reports: 0,
      logs: 0
    };
  }
});

ipcMain.handle('update-report', (event, jobId, updates) => {
  try {
    updateJobInLog(null, jobId, updates);
    return { success: true };
  } catch (e) {
    console.error('Failed to update report via IPC:', e.message);
    return { success: false, error: e.message };
  }
});

function updateReportFile(jobId, updates) {
  if (!fs.existsSync(reportFile)) return;

  const content = fs.readFileSync(reportFile, "utf-8");
  let reports = JSON.parse(content);

  reports = reports.map(r => {
    if (r.id === jobId) {
      return { ...r, ...updates };
    }
    return r;
  });

  fs.writeFileSync(reportFile, JSON.stringify(reports, null, 2));
}

// Pause
ipcMain.on("pause-job", (event, jobId) => {
  console.log("📌 Pause request received for job:", jobId);
  if (jobControllers[jobId]) {
    jobControllers[jobId].paused = true;
    updateReportFile(jobId, { paused: true, status: "paused" });
    console.log("✅ Job paused and JSON updated:", jobId);
    event.reply("job-paused", jobId);
    
    // Send updated reports to refresh UI
    try {
      event.reply("reports-updated", readJsonLog());
    } catch (e) {
      console.warn("Failed to send reports update after pause:", e.message);
    }
  } else {
    console.log("❌ Job not found in controllers:", jobId);
    // Job might be completed, just update the status in reports for UI consistency
    updateReportFile(jobId, { paused: true, status: "paused" });
    event.reply("job-paused", jobId);
    try {
      event.reply("reports-updated", readJsonLog());
    } catch (e) {
      console.warn("Failed to send reports update after pause (no controller):", e.message);
    }
  }
});

// Resume
ipcMain.on("resume-job", (event, jobId) => {
  console.log("📌 Resume request received for job:", jobId);
  if (jobControllers[jobId]) {
    jobControllers[jobId].paused = false;
    updateReportFile(jobId, { paused: false, status: "in_progress" });
    console.log("✅ Job resumed and JSON updated:", jobId);
    event.reply("job-resumed", jobId);
    
    // Send updated reports to refresh UI
    try {
      event.reply("reports-updated", readJsonLog());
    } catch (e) {
      console.warn("Failed to send reports update after resume:", e.message);
    }
  } else {
    console.log("❌ Job not found in controllers:", jobId);
    // Job might be completed or not running, inform UI
    event.reply("job-resume-failed", { jobId, reason: "Job not active or completed" });
    try {
      event.reply("reports-updated", readJsonLog());
    } catch (e) {
      console.warn("Failed to send reports update after resume fail:", e.message);
    }
  }
});

// Cancel
ipcMain.on("cancel-job", (event, jobId) => {
  console.log("📌 Cancel request received for job:", jobId);
  if (jobControllers[jobId]) {
    jobControllers[jobId].cancelled = true;
    updateReportFile(jobId, { cancelled: true, status: "cancelled" });
    console.log("✅ Job cancelled and JSON updated:", jobId);
    event.reply("job-cancelled", jobId);
    
    // Send updated reports to refresh UI
    try {
      event.reply("reports-updated", readJsonLog());
    } catch (e) {
      console.warn("Failed to send reports update after cancel:", e.message);
    }
  } else {
    console.log("❌ Job not found in controllers:", jobId);
    // Job might be completed, just update the status in reports for UI consistency
    updateReportFile(jobId, { cancelled: true, status: "cancelled" });
    event.reply("job-cancelled", jobId);
    try {
      event.reply("reports-updated", readJsonLog());
    } catch (e) {
      console.warn("Failed to send reports update after cancel (no controller):", e.message);
    }
  }
});

// Debug: Get job controller status
ipcMain.handle('get-job-status', (event, jobId) => {
  try {
    const controller = jobControllers[jobId];
    if (controller) {
      return {
        exists: true,
        paused: controller.paused,
        cancelled: controller.cancelled,
        status: controller.paused ? 'Paused' : controller.cancelled ? 'Cancelled' : 'Running'
      };
    } else {
      return {
        exists: false,
        status: 'Not Found or Completed'
      };
    }
  } catch (e) {
    console.error('get-job-status error:', e.message);
    return { exists: false, error: e.message };
  }
});

// Debug: Get all active job controllers
ipcMain.handle('get-active-jobs', () => {
  try {
    const jobs = {};
    console.log('🔍 Current jobControllers keys:', Object.keys(jobControllers || {}));
    for (const [jobId, controller] of Object.entries(jobControllers || {})) {
      jobs[jobId] = {
        paused: controller.paused,
        cancelled: controller.cancelled,
        status: controller.paused ? 'Paused' : controller.cancelled ? 'Cancelled' : 'Running'
      };
    }
    console.log('🔍 Active jobs result:', jobs);
    return jobs;
  } catch (e) {
    console.error('get-active-jobs error:', e.message);
    return {};
  }
});

// Enhanced profile validation IPC handler
ipcMain.handle('validate-profile-readiness', async (event, profileName) => {
  try {
    const validation = await validateProfileReadiness(profileName);
    return validation;
  } catch (e) {
    console.error('validate-profile-readiness error:', e.message);
    return {
      ready: false,
      error: `Validation exception: ${e.message}`,
      code: 'VALIDATION_EXCEPTION'
    };
  }
});

// Validate multiple profiles at once
ipcMain.handle('validate-multiple-profiles', async (event, profileNames) => {
  try {
    const results = {};
    
    for (const profileName of profileNames) {
      try {
        const validation = await validateProfileReadiness(profileName);
        results[profileName] = validation;
      } catch (e) {
        results[profileName] = {
          ready: false,
          error: `Validation exception: ${e.message}`,
          code: 'VALIDATION_EXCEPTION'
        };
      }
    }
    
    return results;
  } catch (e) {
    console.error('validate-multiple-profiles error:', e.message);
    return {};
  }
});


///////////////////// Work on Patient page //////////////////////////////////////////////////
// ✅ Database file
// const dbPath = path.join(__dirname, "patients.db");
// const db = new Database(dbPath);

// === SETUP DB ===
let db;
  const userDataPath = app.getPath('userData');
  const dbPath = path.join(userDataPath, 'patients.db');

  // Determine where the packaged DB lives. When packaged, resources live in process.resourcesPath
  let packagedDbPath;
  try {
    if (app.isPackaged) {
      packagedDbPath = path.join(process.resourcesPath, 'assets', 'db', 'patients.db');
    } else {
      packagedDbPath = path.join(__dirname, 'assets', 'db', 'patients.db');
    }

    // Fallback: if not found, try one level up (possible during some packaging setups)
    if (!fs.existsSync(packagedDbPath)) {
      const alt = path.join(__dirname, '..', 'assets', 'db', 'patients.db');
      if (fs.existsSync(alt)) packagedDbPath = alt;
    }
  } catch (e) {
    console.error('Error resolving packagedDbPath:', e.message);
    packagedDbPath = path.join(__dirname, 'assets', 'db', 'patients.db');
  }

  // Ensure userData folder exists
  try { fs.mkdirSync(path.dirname(dbPath), { recursive: true }); } catch (e) {}

  // Copy DB to userData path on first run if a packaged DB exists
  try {
    if (!fs.existsSync(dbPath)) {
      if (fs.existsSync(packagedDbPath)) {
        fs.copyFileSync(packagedDbPath, dbPath);
        console.log('Copied packaged DB to writable location:', dbPath);
      } else {
        // Packaged DB not present — proceed and let better-sqlite3 create an empty DB file
        console.warn('packaged patients.db not found at', packagedDbPath, '; an empty DB will be created at', dbPath);
      }
    }
  } catch (e) {
    console.error('Failed to copy packaged DB:', e.message);
  }

  // Open with better-sqlite3 (this will create the DB file if it doesn't exist)
  // By default don't verbose-log every SQL statement (it floods the console).
  // Set environment variable DEBUG_SQL=1 to enable SQL logging for debugging.
  const verboseLog = process.env.DEBUG_SQL ? console.log : () => {};
  db = new Database(dbPath, { verbose: verboseLog });
  // console.log('Connected to patients.db',dbPath);


db.prepare(`
  CREATE TABLE IF NOT EXISTS patients (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    unique_id TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    phone TEXT NOT NULL,
    profile TEXT,
    last_visited DATE,
    Last_Msgsent_date DATE,
    last_template TEXT,
    Add_Date DATE,
    mod_date DATE,
    Is_DND INTEGER DEFAULT 0,
    is_active INTEGER DEFAULT 1,
    is_Deleted INTEGER DEFAULT 0,
    UNIQUE(name, phone) -- ensures the combination is unique
  )
`).run();

// Add new columns if they don't exist
try {
  db.prepare(`ALTER TABLE patients ADD COLUMN IsNew INTEGER DEFAULT 1`).run();
} catch (e) {
  // Column already exists, ignore error
  if (!e.message.includes('duplicate column name')) {
    console.warn('Error adding IsNew column:', e.message);
  }
}

try {
  db.prepare(`ALTER TABLE patients ADD COLUMN Filedate DATETIME`).run();
} catch (e) {
  // Column already exists, ignore error
  if (!e.message.includes('duplicate column name')) {
    console.warn('Error adding Filedate column:', e.message);
  }
}

// Ensure legacy databases get the new 'profile' column
try {
  const cols = db.prepare("PRAGMA table_info(patients)").all();
  const hasProfile = cols.some(c => c.name && c.name.toLowerCase() === 'profile');
  if (!hasProfile) {
    try {
      db.prepare('ALTER TABLE patients ADD COLUMN profile TEXT').run();
      console.log('Added profile column to patients table');
    } catch (e) {
      // ALTER TABLE may fail on some sqlite builds or readonly DBs; log and continue
      console.warn('Could not add profile column to patients table:', e.message);
    }
  }
} catch (e) {
  console.warn('Failed to verify/add profile column:', e.message);
}

// Add schedule days column if it doesn't exist
try {
  db.prepare(`ALTER TABLE patients ADD COLUMN last_schedule_days INTEGER DEFAULT 0`).run();
} catch (e) {
  // Column already exists, ignore error
  if (!e.message.includes('duplicate column name')) {
    console.warn('Error adding last_schedule_days column:', e.message);
  }
}


// Helper: get configured daily limit for a profile. Gets limit from app_settings.defaultMessageLimitPerProfile only
function getProfileDailyLimit(profileName) {
  try {
    const s = loadSettings();
    const v = s && (s.defaultMessageLimitPerProfile || s.messageLimit) ? Number(s.defaultMessageLimitPerProfile || s.messageLimit) : 0;
    return v || 0;
  } catch (e) { return 0; }
}

// Helper: count messages sent by profile today (local date) using message_logs when available
function getProfileDailySent(profileName) {
  try {
    // Prefer the consolidated daily_stats table which stores per-profile counts in JSON.
    // daily_stats row structure: { day: 'YYYY-MM-DD', total: INT, per_profile: JSON }
    const today = getLocalISOString().slice(0, 10);
    const row = db.prepare('SELECT per_profile FROM daily_stats WHERE day = ?').get(today);
    if (row && row.per_profile) {
      try {
        const per = JSON.parse(row.per_profile || '{}');
        return Number(per[profileName] || 0);
      } catch (e) {
        // malformed JSON -> fall through to message_logs fallback
      }
    }
  } catch (e) {
    // ignore and fall back to message_logs
  }

  try {
    // Fallback: count successful rows in message_logs for today's local date
    const row = db.prepare("SELECT COUNT(*) as cnt FROM message_logs WHERE profile = ? AND status = 'Sent' AND date(sent_at) = date('now','localtime')").get(profileName);
    return row ? (row.cnt || 0) : 0;
  } catch (e2) {
    return 0;
  }
}

// When a profile reaches its daily limit, mark this send as skipped and optionally pause the job
function handleProfileLimitReached(profileName, jobId, item, allLogs, jsonLogPath) {
  try {
    const msg = `Daily limit reached for profile ${profileName}`;
    console.warn(msg);
    // Record skip in memory log and DB
    allLogs.push({ Profile: profileName, Phone: item && (item.number || item.phone) || '', Status: 'Skipped', Timestamp: getLocalISOString(), Message: '', Error: msg });
    try {
      const mid = 'ml_' + Date.now().toString() + '_' + Math.floor(Math.random() * 1000);
      insertMessageLog.run({
        id: mid,
        job_id: jobId || null,
        unique_id: item && (item.row && (item.row.unique_id || item.row.uniqueId) || item.unique_id) || null,
        name: item && (item.row && (item.row.name || item.row.Name) || item.name) || null,
        phone: item && (item.number || item.phone) || null,
        profile: profileName,
        template: (item && item.template) || null,
        message: '',
        status: 'Skipped',
        sent_at: getLocalISOString(),
        error: msg
      });
    } catch (e) { console.warn('insertMessageLog failed when recording profile-limit skip:', e && e.message ? e.message : e); }

    // Update JSON job log if present
    try { if (jsonLogPath && jobId) updateJobInLog(jsonLogPath, jobId, { record_failed: null }); } catch (e) {}

    // Default behavior: skip this profile's sends only. If you want to stop the entire job, set this flag in settings.
    try {
      const s = loadSettings();
      if (s && s.stopJobWhenProfileLimitReached) {
        if (jobId && jobControllers[jobId]) {
          jobControllers[jobId].paused = true;
          console.log(`Job ${jobId} paused because profile ${profileName} hit its daily limit`);
          try { if (jsonLogPath) updateJobInLog(jsonLogPath, jobId, { status: 'paused', paused: true }); } catch (e) {}
        }
      }
    } catch (e) {}

  } catch (e) { console.error('handleProfileLimitReached error:', e && e.message ? e.message : e); }
}

// Create templates table to persist templates.json data
db.prepare(`
  CREATE TABLE IF NOT EXISTS templates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    type TEXT DEFAULT 'Text',
    message TEXT,
    media_path TEXT,
    media_filename TEXT,
    sendOption TEXT DEFAULT 'instant',
    afterDays INTEGER DEFAULT 0,
    is_active INTEGER DEFAULT 1,
    is_delete INTEGER DEFAULT 0,
    created TEXT,
    modified TEXT
  )
`).run();

// Add new columns to existing templates table if they don't exist
try {
  db.prepare("ALTER TABLE templates ADD COLUMN type TEXT DEFAULT 'Text'").run();
} catch (e) {
  // Column already exists, ignore
}
try {
  db.prepare("ALTER TABLE templates ADD COLUMN media_path TEXT").run();
} catch (e) {
  // Column already exists, ignore
}
try {
  db.prepare("ALTER TABLE templates ADD COLUMN media_filename TEXT").run();
} catch (e) {
  // Column already exists, ignore
}

// Prepare an upsert for single template operations
const upsertTemplate = db.prepare(`
  INSERT INTO templates (name, type, message, media_path, media_filename, sendOption, afterDays, is_active, is_delete, created, modified)
  VALUES (@name,@type,@message,@media_path,@media_filename,@sendOption,@afterDays,@is_active,@is_delete,@created,@modified)
  ON CONFLICT(name) DO UPDATE SET
    type=excluded.type,
    message=excluded.message,
    media_path=excluded.media_path,
    media_filename=excluded.media_filename,
    sendOption=excluded.sendOption,
    afterDays=excluded.afterDays,
    is_active=excluded.is_active,
    is_delete=excluded.is_delete,
    modified=excluded.modified
`);

// Create schedules table to store scheduled campaigns
db.prepare(`
  CREATE TABLE IF NOT EXISTS schedules (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    templateName TEXT,
    days INTEGER DEFAULT 0,
    profiles TEXT, -- JSON array of profile names
    is_active INTEGER DEFAULT 1,
    created TEXT,
    modified TEXT
  )
`).run();

const upsertSchedule = db.prepare(`
  INSERT INTO schedules (name, templateName, days, profiles, is_active, created, modified)
  VALUES (@name,@templateName,@days,@profiles,@is_active,@created,@modified)
  ON CONFLICT(name) DO UPDATE SET
    templateName=excluded.templateName,
    days=excluded.days,
    profiles=excluded.profiles,
    is_active=excluded.is_active,
    modified=excluded.modified
`);

// Create profiles table to persist profiles.json data
db.prepare(`
  CREATE TABLE IF NOT EXISTS profiles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    number TEXT,
    pushname TEXT,
    session TEXT,
    is_active INTEGER DEFAULT 1,
    is_delete INTEGER DEFAULT 0,
    created TEXT,
    modified TEXT,
    last_connected TEXT
  )
`).run();

// Migrate legacy profiles.json to DB on first run if DB table empty
try {
  const countRow = db.prepare('SELECT COUNT(*) as cnt FROM profiles').get();
  const cnt = countRow ? countRow.cnt : 0;
  if (cnt === 0) {
    const legacy = getProfilesFile();
    if (fs.existsSync(legacy)) {
      try {
        const raw = JSON.parse(fs.readFileSync(legacy, 'utf-8'));
        if (Array.isArray(raw) && raw.length > 0) {
          const upsert = db.prepare(`
            INSERT INTO profiles (name, number, pushname, session, is_active, is_delete, created, modified, last_connected)
            VALUES (@name,@number,@pushname,@session,@is_active,@is_delete,@created,@modified,@last_connected)
            ON CONFLICT(name) DO UPDATE SET
              number=excluded.number,
              pushname=excluded.pushname,
              session=excluded.session,
              is_active=excluded.is_active,
              is_delete=excluded.is_delete,
              modified=excluded.modified,
              last_connected=excluded.last_connected
          `);
          const insertMany = db.transaction((rows) => {
            for (const p of rows) {
              upsert.run({
                name: p.name,
                number: p.number || "",
                pushname: p.pushname || "",
                session: p.session || "",
                is_active: p.is_active === undefined ? 1 : p.is_active,
                is_delete: p.is_delete === undefined ? 0 : p.is_delete,
                created: p.created ? String(p.created) : getLocalISOString(),
                modified: p.modified ? String(p.modified) : getLocalISOString(),
                last_connected: p.last_connected ? String(p.last_connected) : getLocalISOString()
              });
            }
          });
          insertMany(raw);
          console.log('Migrated legacy profiles.json into profiles table');
        }
      } catch (e) {
        console.error('Failed to migrate legacy profiles.json:', e.message);
      }
    }
  }
} catch (e) {
  console.error('Profiles migration check failed:', e.message);
}

// Ensure auxiliary tables (alerts, daily_stats, app_settings) exist AFTER DB is opened
try {
  if (typeof ensureAuxTables === 'function') ensureAuxTables();
} catch (e) {
  console.error('ensureAuxTables post-init failed:', e.message);
}

// Create a persistent message log table to track every send attempt (success/failed/skipped)
try {
  db.prepare(`
    CREATE TABLE IF NOT EXISTS message_logs (
      id TEXT PRIMARY KEY,
      job_id TEXT,
      unique_id TEXT,
      name TEXT,
      phone TEXT,
      profile TEXT,
      template TEXT,
        message TEXT,
        status TEXT,
        sent_at TEXT,
        error TEXT,
        media_path TEXT,
        media_filename TEXT
    )
  `).run();
} catch (e) {
  console.error('Failed to create message_logs table:', e && e.message ? e.message : e);
}

  // Migration: ensure media_path and media_filename columns exist for older DBs
  try {
    const cols = db.prepare("PRAGMA table_info('message_logs')").all();
    const names = cols.map(c => c.name);
    if (!names.includes('media_path')) {
      try { db.prepare("ALTER TABLE message_logs ADD COLUMN media_path TEXT").run(); console.log('DB migrated: added media_path to message_logs'); } catch (e) { console.warn('Failed to add media_path column:', e && e.message ? e.message : e); }
    }
    if (!names.includes('media_filename')) {
      try { db.prepare("ALTER TABLE message_logs ADD COLUMN media_filename TEXT").run(); console.log('DB migrated: added media_filename to message_logs'); } catch (e) { console.warn('Failed to add media_filename column:', e && e.message ? e.message : e); }
    }
  } catch (e) { console.warn('message_logs migration check failed:', e && e.message ? e.message : e); }

// Prepared statement to insert message log rows
const insertMessageLog = db.prepare(`
  INSERT INTO message_logs (id, job_id, unique_id, name, phone, profile, template, message, status, sent_at, error, media_path, media_filename)
  VALUES (@id,@job_id,@unique_id,@name,@phone,@profile,@template,@message,@status,@sent_at,@error,@media_path,@media_filename)
`);

// Wrap the prepared statement's run() to log payloads and failures for diagnostics
try {
  const _origInsertRun = insertMessageLog.run.bind(insertMessageLog);
  insertMessageLog.run = function (payload) {
    try {
      // shallow log of key fields to avoid huge console output
      console.log('insertMessageLog.run payload:', {
        id: payload && payload.id,
        job_id: payload && payload.job_id,
        phone: payload && payload.phone,
        profile: payload && payload.profile,
        status: payload && payload.status,
        sent_at: payload && payload.sent_at
      });
      const res = _origInsertRun(payload);
      // optionally log success
      // console.log('insertMessageLog.run success', res);
      return res;
    } catch (err) {
      console.error('insertMessageLog.run ERROR:', err && err.message ? err.message : err, 'payload:', payload);
      throw err;
    }
  };
} catch (e) {
  console.warn('Failed to wrap insertMessageLog.run for diagnostics:', e && e.message ? e.message : e);
}

// Diagnostic IPC: return runtime DB path, table list, message_logs count and recent rows
try {
  ipcMain.handle('diag-message-logs', async () => {
    try {
      const dbPathLocal = typeof dbPath !== 'undefined' ? dbPath : (app && app.getPath ? path.join(app.getPath('userData'), 'patients.db') : 'unknown');
      const tables = db.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().map(r => r.name);
      let count = 0;
      try { const row = db.prepare('SELECT COUNT(*) as cnt FROM message_logs').get(); count = row ? row.cnt : 0; } catch (e) { /* ignore */ }
      let sample = [];
      try { sample = db.prepare('SELECT * FROM message_logs ORDER BY sent_at DESC LIMIT 20').all(); } catch (e) { sample = []; }
      return { dbPath: dbPathLocal, tables, count, sample };
    } catch (err) {
      return { error: err && err.message ? err.message : String(err) };
    }
  });
} catch (e) {
  console.warn('Failed to register diag-message-logs IPC:', e && e.message ? e.message : e);
}


// ✅ UPSERT (Insert or Update if conflict on name+phone)
const upsertPatient = db.prepare(`
  INSERT INTO patients (unique_id, name, phone, profile, last_visited, Last_Msgsent_date, last_template, last_schedule_days, Add_Date, mod_date, IsNew, Filedate, Is_DND) VALUES (
    @unique_id, @name, @phone, @profile, @last_visited, @Last_Msgsent_date, @last_template, @last_schedule_days, @Add_Date, @mod_date, @IsNew, @Filedate, @Is_DND )
  ON CONFLICT(name, phone) DO UPDATE SET
    unique_id = CASE WHEN excluded.unique_id IS NOT NULL AND excluded.unique_id != '' THEN excluded.unique_id ELSE patients.unique_id END,
    last_visited = CASE WHEN excluded.last_visited IS NOT NULL THEN excluded.last_visited ELSE patients.last_visited END,
    Last_Msgsent_date = CASE WHEN excluded.Last_Msgsent_date IS NOT NULL THEN excluded.Last_Msgsent_date ELSE patients.Last_Msgsent_date END,
    last_template = CASE WHEN excluded.last_template IS NOT NULL THEN excluded.last_template ELSE patients.last_template END,
    last_schedule_days = CASE WHEN excluded.last_schedule_days IS NOT NULL THEN excluded.last_schedule_days ELSE patients.last_schedule_days END,
    profile = CASE WHEN excluded.profile IS NOT NULL THEN excluded.profile ELSE patients.profile END,
    Is_DND = CASE WHEN excluded.Is_DND IS NOT NULL THEN excluded.Is_DND ELSE patients.Is_DND END,
    mod_date = excluded.mod_date,
    is_active = 1,
    is_Deleted = 0,
    IsNew = 0,
    Filedate = excluded.Filedate
    -- Now unique_id WILL be updated if provided from Excel
`);

// ✅ Insert or update single patient
function savePatient(patient) {
  if (!patient.unique_id) {
    // Generate unique_id if not provided
    patient.unique_id = `PAT-${Date.now()}-${Math.floor(Math.random() * 1000)}`;
  }
  if (!patient.add_date) patient.add_date = getLocalISOString();
  patient.mod_date = getLocalISOString();
  
  // Set default values for new columns
  if (patient.IsNew === undefined) patient.IsNew = 1; // New patient by default
  patient.Filedate = getLocalISOString(); // Always update Filedate to current timestamp

  return upsertPatient.run(patient);
}

// Fetch patients
ipcMain.handle("get-patients", () => {
  return db.prepare("SELECT * FROM patients where is_active=1 and Is_DND=0 and is_Deleted = 0 ORDER BY Add_Date DESC").all();
});

// Return patients by DND flag (Is_DND: 0 or 1)
ipcMain.handle('get-patients-by-dnd', (event, isDnd) => {
  try {
  const flag = Number(isDnd) ? 1 : 0;
  return db.prepare('SELECT * FROM patients WHERE is_active=1 AND is_Deleted=0 AND Is_DND = ? ORDER BY Add_Date DESC').all(flag);
  } catch (e) {
    console.error('get-patients-by-dnd error:', e.message);
    return [];
  }
});

// Get ALL patients for export (both Active and DND) - only specific columns
ipcMain.handle('get-all-patients-for-export', () => {
  try {
    return db.prepare('SELECT unique_id, name, phone, last_visited, Last_Msgsent_date, last_template, Is_DND, profile FROM patients WHERE is_active=1 AND is_Deleted=0 ORDER BY Is_DND ASC, Add_Date DESC').all();
  } catch (e) {
    console.error('get-all-patients-for-export error:', e.message);
    return [];
  }
});

// Get sync report data - patients ordered by Filedate DESC with new/old indicators
ipcMain.handle('get-sync-report', () => {
  try {
    // Get the latest filedate to filter only the most recent import
    const latestFiledateRow = db.prepare('SELECT MAX(Filedate) as latest FROM patients WHERE is_active=1 AND is_Deleted=0 AND Filedate IS NOT NULL').get();
    const latestFiledate = latestFiledateRow ? latestFiledateRow.latest : null;
    
    if (!latestFiledate) {
      return { success: false, message: 'No patients with filedate found', data: [] };
    }
    
    // Get all patients from the latest filedate, ordered by Filedate DESC
    const patients = db.prepare(`
      SELECT *, 
        CASE 
          WHEN IsNew = 1 THEN 'New Patient'
          WHEN IsNew = 0 THEN 'Existing Patient'
          ELSE 'Unknown'
        END as PatientStatus,
        datetime(Filedate) as FormattedFiledate
      FROM patients 
      WHERE is_active=1 AND is_Deleted=0 AND Filedate = ?
      ORDER BY Filedate DESC, Add_Date DESC
    `).all(latestFiledate);
    
    const newCount = patients.filter(p => p.IsNew === 1).length;
    const existingCount = patients.filter(p => p.IsNew === 0).length;
    
    return { 
      success: true, 
      data: patients,
      summary: {
        latestFiledate: latestFiledate,
        totalPatients: patients.length,
        newPatients: newCount,
        existingPatients: existingCount
      }
    };
  } catch (e) {
    console.error('get-sync-report error:', e.message);
    return { success: false, error: e.message, data: [] };
  }
});

// Get all unique filedates for sync report filtering
ipcMain.handle('get-sync-filedates', () => {
  try {
    const filedates = db.prepare(`
      SELECT DISTINCT Filedate, 
        COUNT(*) as patient_count,
        SUM(CASE WHEN IsNew = 1 THEN 1 ELSE 0 END) as new_count,
        SUM(CASE WHEN IsNew = 0 THEN 1 ELSE 0 END) as existing_count,
        datetime(Filedate) as FormattedFiledate
      FROM patients 
      WHERE is_active=1 AND is_Deleted=0 AND Filedate IS NOT NULL
      GROUP BY Filedate
      ORDER BY Filedate DESC
    `).all();
    
    return { success: true, data: filedates };
  } catch (e) {
    console.error('get-sync-filedates error:', e.message);
    return { success: false, error: e.message, data: [] };
  }
});

// Set DND flag for a patient by phone
ipcMain.handle('set-dnd', (event, { phone, unique_id, isDnd }) => {
  try {
    const now = getLocalISOString();
    const flag = Number(isDnd) ? 1 : 0;
    let res;
    // Helper to generate phone variants to match DB (digits only, with/without country code)
    function phoneVariants(raw) {
      if (!raw) return [];
      const s = String(raw).trim();
      const digits = s.replace(/\D/g, '');
      const out = new Set([s, digits]);
      if (digits) {
        // without leading zeros
        const noLeadingZeros = digits.replace(/^0+/, '');
        out.add(noLeadingZeros);
        // with common country code 91
        if (!noLeadingZeros.startsWith('91')) out.add('91' + noLeadingZeros);
        else out.add(noLeadingZeros.slice(2));
        // plus prefix
        out.add('+' + noLeadingZeros);
      }
      return Array.from(out);
    }

    if (unique_id) {
      res = db.prepare('UPDATE patients SET Is_DND = ?, mod_date = ? WHERE unique_id = ?').run(flag, now, unique_id);
      console.log(`set-dnd by unique_id=${unique_id} -> changes=${res.changes}`);
    } else {
      // try several phone variants to increase match chance
      const variants = phoneVariants(phone);
      let totalChanges = 0;
      for (const v of variants) {
        const r = db.prepare('UPDATE patients SET Is_DND = ?, mod_date = ? WHERE phone = ?').run(flag, now, v);
        totalChanges += (r && r.changes) ? r.changes : 0;
      }
      // also attempt a fallback fuzzy update using LIKE (match numbers that contain digits)
      if (totalChanges === 0 && phone) {
        const digits = String(phone).replace(/\D/g, '');
        if (digits) {
          const r = db.prepare("UPDATE patients SET Is_DND = ?, mod_date = ? WHERE REPLACE(REPLACE(REPLACE(phone,' ',''),'+',''),'-','') LIKE '%' || ? || '%' ").run(flag, now, digits);
          totalChanges += (r && r.changes) ? r.changes : 0;
        }
      }
      res = { changes: totalChanges };
      console.log(`set-dnd by phone variants for=${phone} -> totalChanges=${res.changes}`);
    }
    return { success: res.changes > 0, changes: res.changes };
  } catch (e) {
    console.error('set-dnd error:', e.message);
    return { success: false, error: e.message };
  }
});

// Update a patient's profile field
ipcMain.handle('update-patient-profile', (event, { unique_id, phone, profile }) => {
  try {
    const now = getLocalISOString();
    const stmt = db.prepare('UPDATE patients SET profile = ?, mod_date = ? WHERE unique_id = ? OR phone = ?');
    const res = stmt.run(profile || null, now, unique_id || '', phone || '');
    return { success: true, changes: res.changes };
  } catch (e) {
    console.error('update-patient-profile failed:', e.message);
    return { success: false, error: e.message };
  }
});

// Bulk set default profile for patients by unique_id or phone
ipcMain.handle('bulk-set-default-profile', (event, { profile, phones }) => {
  try {
    if (!profile) return { success: false, error: 'No profile provided' };
    if (!Array.isArray(phones) || phones.length === 0) return { success: false, error: 'No identifiers provided' };
    const now = getLocalISOString();
    const stmt = db.prepare('UPDATE patients SET profile = ?, mod_date = ? WHERE unique_id = ? OR phone = ?');
    let total = 0;
    const tx = db.transaction((items) => {
      for (const id of items) {
        try {
          const r = stmt.run(profile, now, id || '', id || '');
          total += (r && r.changes) ? r.changes : 0;
        } catch (e) {
          console.warn('bulk-set-default-profile row failed for', id, e && e.message ? e.message : e);
        }
      }
    });
    tx(phones);
    return { success: true, changes: total };
  } catch (e) {
    console.error('bulk-set-default-profile failed:', e && e.message ? e.message : e);
    return { success: false, error: e && e.message ? e.message : String(e) };
  }
});

// Bulk rename profile: replace patients with profile = fromProfile to toProfile
ipcMain.handle('bulk-rename-profile', (event, { fromProfile, toProfile }) => {
  try {
    if (!fromProfile || !toProfile) return { success: false, error: 'Missing fromProfile or toProfile' };
    const now = getLocalISOString();
    const stmt = db.prepare('UPDATE patients SET profile = ?, mod_date = ? WHERE profile = ?');
    const res = stmt.run(toProfile, now, fromProfile);
    return { success: true, changes: res.changes };
  } catch (e) {
    console.error('bulk-rename-profile failed:', e && e.message ? e.message : e);
    return { success: false, error: e && e.message ? e.message : String(e) };
  }
});

// Add patient
// ipcMain.handle("add-patient", (event, patient) => {
//   const stmt = db.prepare(
//     "INSERT INTO patients (unique_id, name, phone, last_visited) VALUES (?, ?, ?, ?)"
//   );
//   stmt.run(patient.uniqueId, patient.name, patient.phone, patient.lastVisited);
// });

// ✅ Add or update a single patient
ipcMain.handle("add-patient", (event, patient) => {
 // console.log(patient)
  patient.unique_id = patient.uniqueId;
  patient.name = patient.name;
  
  // Clean phone number - handle float values
  let phoneNumber = patient.phone || "";
  if (typeof phoneNumber === 'number') {
    // Convert float to string and remove decimal point if it's .0
    phoneNumber = phoneNumber.toString();
    if (phoneNumber.endsWith('.0')) {
      phoneNumber = phoneNumber.slice(0, -2);
    }
  } else {
    phoneNumber = String(phoneNumber).trim();
  }
  patient.phone = phoneNumber;
  
  patient.last_visited = patient.lastVisited;
  const now = getLocalISOString();
  patient.Add_Date = now;
  patient.mod_date = now;
  
  // Set default values for new columns
  if (patient.IsNew === undefined) patient.IsNew = 1; // New patient by default
  patient.Filedate = now; // Always update Filedate to current timestamp
  
  // Set default values for required fields that might be missing
  if (patient.Last_Msgsent_date === undefined) patient.Last_Msgsent_date = now;
  if (patient.last_template === undefined) patient.last_template = 'welcome';
  if (patient.profile === undefined) patient.profile = null;
  if (patient.Is_DND === undefined) patient.Is_DND = 0;
  if (patient.last_schedule_days === undefined) patient.last_schedule_days = 0;
  
  return upsertPatient.run(patient);
});

// ✅ Delete multiple customers by unique_ids
ipcMain.handle("delete-customers-bulk", (event, uniqueIds) => {
  try {
    if (!Array.isArray(uniqueIds) || uniqueIds.length === 0) {
      return { success: false, error: 'No customer IDs provided' };
    }
    
    // Create placeholders for IN clause
    const placeholders = uniqueIds.map(() => '?').join(',');
    const deleteStmt = db.prepare(`DELETE FROM patients WHERE unique_id IN (${placeholders})`);
    
    const result = deleteStmt.run(...uniqueIds);
    
    return {
      success: true,
      deletedCount: result.changes || 0,
      message: `Deleted ${result.changes} customers`
    };
  } catch (error) {
    console.error('Error deleting customers in bulk:', error);
    return {
      success: false,
      error: error.message || 'Failed to delete customers'
    };
  }
});

// ✅ Delete ALL customers
ipcMain.handle("delete-all-customers", (event) => {
  try {
    // First count total customers
    const countStmt = db.prepare('SELECT COUNT(*) as total FROM patients');
    const countResult = countStmt.get();
    const totalCount = countResult.total || 0;
    
    if (totalCount === 0) {
      return { success: true, deletedCount: 0, message: 'No customers to delete' };
    }
    
    // Delete all customers
    const deleteStmt = db.prepare('DELETE FROM patients');
    const result = deleteStmt.run();
    
    return {
      success: true,
      deletedCount: result.changes || totalCount,
      message: `Deleted all ${result.changes} customers`
    };
  } catch (error) {
    console.error('Error deleting all customers:', error);
    return {
      success: false,
      error: error.message || 'Failed to delete all customers'
    };
  }
});

// ✅ Direct DELETE FROM patients (no conditions) - for Delete All Records button
ipcMain.handle("delete-all-records-direct", (event) => {
  try {
    console.log('🗑️ Executing direct DELETE FROM patients...');
    
    // Get count before deletion for logging
    const countStmt = db.prepare('SELECT COUNT(*) as total FROM patients');
    const beforeCount = countStmt.get().total || 0;
    
    console.log(`📊 Records before deletion: ${beforeCount}`);
    
    // Execute direct DELETE FROM patients with no WHERE clause
    const deleteStmt = db.prepare('DELETE FROM patients');
    const result = deleteStmt.run();
    
    console.log(`✅ Delete operation completed. Changes: ${result.changes}`);
    
    // Verify deletion
    const afterCount = countStmt.get().total || 0;
    console.log(`📊 Records after deletion: ${afterCount}`);
    
    return {
      success: true,
      deletedCount: result.changes || beforeCount,
      beforeCount: beforeCount,
      afterCount: afterCount,
      message: `Direct DELETE FROM patients executed. Removed ${result.changes} records.`
    };
  } catch (error) {
    console.error('❌ Error in direct delete operation:', error);
    return {
      success: false,
      error: error.message || 'Failed to execute DELETE FROM patients'
    };
  }
});

// Listen for Excel file save requests
ipcMain.on('save-importexcel-file', (event, fileData) => {
  saveFile(event, fileData, 'patientexcel', 'save-importexcel-file-response');
});

// Receive file from renderer and save it
ipcMain.handle("save-patientExcel-file", async (event, file) => {
  try {
    const folderPath = path.join(getUserDataDir(), "patientExcel");

    if (!fs.existsSync(folderPath)) {
      fs.mkdirSync(folderPath, { recursive: true });
    }
    const ext = path.extname(file.name);                // e.g. '.xlsx'
    const baseName = path.basename(file.name, ext);     // e.g. 'myfile'
    const now = new Date();
    const datetimeFormatted = now.toISOString()
    .replace('T', '_')         // '2025-09-20T12:34:56.789Z' -> '2025-09-20_12:34:56.789Z'
    .replace(/:/g, '')          // remove colons: '2025-09-20_123456.789Z'
    .replace('Z', '')           // remove trailing Z: '2025-09-20_123456.789'
    .replace(/-/g, '')          // remove dashes in date: '20250920_123456.789'
    .replace(/\./g, ''); 

    const newName = `${baseName}_${datetimeFormatted}${ext}`;
    console.log(newName);
    const savePath = path.join(folderPath, newName);

    await fs.promises.writeFile(savePath, file.data);

    //console.log("File saved to:", savePath);
    return savePath;
  } catch (err) {
    console.error("Failed to save Excel file:", err);
    return null;
  }
});


ipcMain.handle("import-patients", (event, filePath) => {
  console.log("Import file path:", filePath);
  try {
    if (!fs.existsSync(filePath)) {
      return { success: false, error: "File does not exist" };
    }

    const workbook = XLSX.readFile(filePath); // 🔥 Read Excel from file path
    const sheet = workbook.Sheets[workbook.SheetNames[0]];
    const patients = XLSX.utils.sheet_to_json(sheet);

    console.log("Parsed patients from Excel:", patients.length, "rows");
    if (patients.length > 0) {
      console.log("First row sample:", patients[0]);
      console.log("Excel column names:", Object.keys(patients[0]));
    }

    const now = getLocalISOString();

    const insertMany = db.transaction((rows) => {
      let processedCount = 0;
      
      for (const p of rows) {
        console.log("Processing patient:", p.name || 'No Name', p.phone || 'No Phone');
        console.log("Raw Excel row data:", p);
        
        // Handle last_visited - check multiple possible column names
        const lastVisitedStr = p.lastVisited || p.last_visited || p['last_visited'] || null;
        const lastVisitedDate = parseDate(lastVisitedStr);
        
        // Set default values for imported patients - override any existing data
        const lastMsgSentDate = getLocalISOString();
        const lastTemplate = 'welcome';
        const lastScheduleDays = 0;
        // console.log("lastTemplate from Excel:", {
        //   raw: p.lastTemplate,
        //   type: typeof p.lastTemplate,
        //   final: lastTemplate
        // });
        
        // Clean phone number - handle float values from Excel
        let phoneNumber = p.phone || "";
        if (typeof phoneNumber === 'number') {
          // Convert float to string and remove decimal point if it's .0
          phoneNumber = phoneNumber.toString();
          if (phoneNumber.endsWith('.0')) {
            phoneNumber = phoneNumber.slice(0, -2);
          }
        } else {
          phoneNumber = String(phoneNumber).trim();
        }
        
        // Handle unique_id: support multiple column names, or generate based on name+phone logic
        let uniqueId = p.uniqueId || p.unique_id || p.UniqueId || p['Unique ID'] || p['unique_id'] || "";
        if (typeof uniqueId === 'number') {
          uniqueId = uniqueId.toString();
          if (uniqueId.endsWith('.0')) {
            uniqueId = uniqueId.slice(0, -2);
          }
        } else {
          uniqueId = String(uniqueId || "").trim();
        }
        
        // Generate unique_id based on your requirements if missing
        if (!uniqueId) {
          const CustomerName = p.name ? String(p.name).trim() : "";
          const cleanPhone = phoneNumber ? String(phoneNumber).trim() : "";
          
          if (CustomerName && cleanPhone) {
            // If both name and phone exist: unique_id = name + phone
            uniqueId = `${CustomerName}_${cleanPhone}`;
          } else if (cleanPhone) {
            // If only phone exists: unique_id = phone
            uniqueId = cleanPhone;
          } else if (CustomerName) {
            // If only name exists: use name with timestamp for uniqueness
            uniqueId = `${CustomerName}_${Date.now()}`;
          } else {
            // Fallback: generate unique ID
            uniqueId = `PAT-${Date.now()}-${Math.floor(Math.random() * 10000)}`;
          }
        }
        
        const patient = {
          unique_id: uniqueId,
          name: p.name ? String(p.name).trim() : "", // Save empty string if no name (NOT NULL constraint)
          phone: phoneNumber || "", // Save empty string if no phone (NOT NULL constraint)
          profile: (p.profile || p.Profile) ? String(p.profile || p.Profile).trim() : null, // Save null if no profile
          last_visited: lastVisitedDate,
          Last_Msgsent_date: now,
          last_template: 'welcome',
          last_schedule_days: 0, // Always set to 0 for all imports
          Add_Date: now,
          mod_date: now,
          IsNew: p.IsNew !== undefined ? p.IsNew : 1, // Default to 1 (true) if not specified
          Filedate: now, // Always set to current timestamp during import
          Is_DND: 0 // Default to 0 if not available
        };

        // Handle IsDND if present in Excel
        if (p.hasOwnProperty('IsDND') || p.hasOwnProperty('Is_DND') || p.hasOwnProperty('isDND') || p.hasOwnProperty('is_dnd')) {
          const isDndValue = p.IsDND || p.Is_DND || p.isDND || p.is_dnd;
          if (isDndValue === 0 || isDndValue === 1 || isDndValue === '0' || isDndValue === '1') {
            patient.Is_DND = Number(isDndValue);
          }
        }

        // Validate required fields (phone is mandatory due to NOT NULL constraint)
        if (!patient.phone || patient.phone.trim() === "") {
          console.warn("Skipping invalid patient - missing phone:", patient);
          continue;
        }

        //console.log("Patient object to save:", patient);

        try {
          const result = upsertPatient.run(patient);
          console.log("Database result:", result);
          processedCount++;
        } catch (err) {
          console.error(`Error importing patient ${patient.name || 'No Name'} (${patient.phone || 'No Phone'}):`, err.message);
          console.error("Full error:", err);
          continue;
        }
      }
      
      return processedCount;
    });

    const processedCount = insertMany(patients);
    console.log("Import completed. Processed count:", processedCount);

    return { success: true, count: processedCount };
  } catch (err) {
    console.error("Failed to import Excel:", err);
    return { success: false, error: err.message };
  }

});

// Bulk import patients from wizard (same as original Excel import - FAST!)
ipcMain.handle("bulk-import-patients", (event, customers) => {
  try {
    console.log("🚀 Bulk import starting for", customers.length, "customers");
    
    const now = getLocalISOString();
    
    const insertMany = db.transaction((rows) => {
      let processedCount = 0;
      
      for (const customer of rows) {
        // Skip if no phone number (same validation as original)
        if (!customer.phone || String(customer.phone).trim() === "") {
          console.warn("Skipping invalid customer - missing phone:", customer);
          continue;
        }
        
        // Clean phone number (same logic as original)
        let phoneNumber = customer.phone || "";
        if (typeof phoneNumber === 'number') {
          phoneNumber = phoneNumber.toString();
          if (phoneNumber.endsWith('.0')) {
            phoneNumber = phoneNumber.slice(0, -2);
          }
        } else {
          phoneNumber = String(phoneNumber).trim();
        }
        
        // Handle unique_id (same logic as original)
        let uniqueId = customer.unique_id || customer.uniqueId || "";
        if (typeof uniqueId === 'number') {
          uniqueId = uniqueId.toString();
          if (uniqueId.endsWith('.0')) {
            uniqueId = uniqueId.slice(0, -2);
          }
        } else {
          uniqueId = String(uniqueId || "").trim();
        }
        
        // Generate unique_id if missing (same logic as original)
        if (!uniqueId) {
          const CustomerName = customer.name ? String(customer.name).trim() : "";
          const cleanPhone = phoneNumber ? String(phoneNumber).trim() : "";
          
          if (CustomerName && cleanPhone) {
            uniqueId = `${CustomerName}_${cleanPhone}`;
          } else if (cleanPhone) {
            uniqueId = cleanPhone;
          } else if (CustomerName) {
            uniqueId = `${CustomerName}_${Date.now()}`;
          } else {
            uniqueId = `PAT-${Date.now()}-${Math.floor(Math.random() * 10000)}`;
          }
        }
        
        const patient = {
          unique_id: uniqueId,
          name: customer.name ? String(customer.name).trim() : "",
          phone: phoneNumber || "",
          profile: customer.profile ? String(customer.profile).trim() : null,
          last_visited: customer.lastVisited || customer.last_visited || null,
          Last_Msgsent_date: now,
          last_template: 'welcome',
          last_schedule_days: 0,
          Add_Date: now,
          mod_date: now,
          IsNew: 1, // New patient by default
          Filedate: now,
          Is_DND: 0 // Default to 0
        };

        try {
          // Use the SAME upsertPatient function as original Excel import
          const result = upsertPatient.run(patient);
          processedCount++;
        } catch (err) {
          console.error(`Error importing customer ${patient.name || 'No Name'} (${patient.phone || 'No Phone'}):`, err.message);
          continue;
        }
      }
      
      return processedCount;
    });

    const processedCount = insertMany(customers);
    console.log("Bulk import completed. Processed count:", processedCount);

    return { success: true, count: processedCount };
  } catch (err) {
    console.error("Failed to bulk import customers:", err);
    return { success: false, error: err.message };
  }
});

// Receive file from renderer and save it
ipcMain.handle("save-dndExcel-file", async (event, file) => {
  try {
    const folderPath = path.join(getUserDataDir(), "dndExcel");

    if (!fs.existsSync(folderPath)) {
      fs.mkdirSync(folderPath, { recursive: true });
    }
    const ext = path.extname(file.name);                // e.g. '.xlsx'
    const baseName = path.basename(file.name, ext);     // e.g. 'myfile'
    const now = new Date();
    const datetimeFormatted = now.toISOString()
    .replace('T', '_')         // '2025-09-20T12:34:56.789Z' -> '2025-09-20_12:34:56.789Z'
    .replace(/:/g, '')          // remove colons: '2025-09-20_123456.789Z'
    .replace('Z', '')           // remove trailing Z: '2025-09-20_123456.789'
    .replace(/-/g, '')          // remove dashes in date: '20250920_123456.789'
    .replace(/\./g, ''); 

    const newName = `${baseName}_${datetimeFormatted}${ext}`;
    console.log(newName);
    const savePath = path.join(folderPath, newName);

    await fs.promises.writeFile(savePath, file.data);

    //console.log("File saved to:", savePath);
    return savePath;
  } catch (err) {
    console.error("Failed to save Excel file:", err);
    return null;
  }
});

ipcMain.handle("import-dnd", (event, filePath) => {
  console.log(filePath);
  try {
    if (!fs.existsSync(filePath)) {
      return { success: false, error: "File does not exist" };
    }
    const workbook = XLSX.readFile(filePath); // 🔥 Read Excel from file path
    const sheet = workbook.Sheets[workbook.SheetNames[0]];
    const patients = XLSX.utils.sheet_to_json(sheet);
    const now = getLocalISOString();
    const updateMany = db.transaction((rows) => {
      for (const p of rows) {
        const patient = {  
          phone: p.phone,
          mod_date: now,
          Is_DND: 1,
        };
        updateDndByPhone.run(patient); // Assumes upsertStmt is prepared
      }
    });

    updateMany(patients);

    return { success: true, count: patients.length };
  } catch (err) {
    console.error("Failed to import Excel:", err);
    return { success: false, error: err.message };
  }

});

// ✅ Export patients to Excel
ipcMain.handle("export-patients", async (event, patientsDataArg) => {
  try {
    // If renderer provided a dataset (visible rows), use it; otherwise fallback to DB
    let patientsData = Array.isArray(patientsDataArg) && patientsDataArg.length > 0 ? patientsDataArg : db.prepare(`
      SELECT * FROM patients WHERE is_active=1 and is_deleted = 0
    `).all();

    const ws = XLSX.utils.json_to_sheet(patientsData);
    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, "Patients");

    // Generate unique filename with timestamp
    const now = new Date();
    const timestamp = now.toISOString()
      .replace('T', '_')
      .replace(/:/g, '')
      .replace(/\./g, '')
      .replace('Z', '');
    
    const defaultName = `patients_export_${timestamp}.xlsx`;

    // Ask user where to save the file
    const { canceled, filePath } = await dialog.showSaveDialog(mainWin || null, {
      title: 'Save Patients Export',
      defaultPath: path.join(require('os').homedir(), 'Desktop', defaultName),
      buttonLabel: 'Save',
      filters: [
        { name: 'Excel', extensions: ['xlsx'] },
        { name: 'All Files', extensions: ['*'] }
      ]
    });

    if (canceled || !filePath) return { success: false, canceled: true };

    // Ensure parent dir exists
    try { fs.mkdirSync(path.dirname(filePath), { recursive: true }); } catch (e) {}

    // Try to write file with retry mechanism for locked files
    let attempts = 0;
    const maxAttempts = 3;
    
    while (attempts < maxAttempts) {
      try {
        // Check if file exists and is locked
        if (fs.existsSync(filePath)) {
          try {
            // Try to open the file to check if it's locked
            const fd = fs.openSync(filePath, 'r+');
            fs.closeSync(fd);
          } catch (lockError) {
            if (lockError.code === 'EBUSY' || lockError.code === 'EACCES') {
              // File is locked, suggest alternative filename
              const ext = path.extname(filePath);
              const base = path.basename(filePath, ext);
              const dir = path.dirname(filePath);
              const newFilePath = path.join(dir, `${base}_copy_${Date.now()}${ext}`);
              
              XLSX.writeFile(wb, newFilePath);
              return { 
                success: true, 
                file: newFilePath,
                message: `Original file was locked. Saved as: ${path.basename(newFilePath)}`
              };
            }
            throw lockError;
          }
        }
        
        XLSX.writeFile(wb, filePath);
        return { success: true, file: filePath };
        
      } catch (writeError) {
        attempts++;
        
        if (writeError.code === 'EBUSY' || writeError.code === 'EACCES') {
          if (attempts >= maxAttempts) {
            // Final attempt: create a new file with timestamp
            const ext = path.extname(filePath);
            const base = path.basename(filePath, ext);
            const dir = path.dirname(filePath);
            const finalFilePath = path.join(dir, `${base}_${Date.now()}${ext}`);
            
            try {
              XLSX.writeFile(wb, finalFilePath);
              return { 
                success: true, 
                file: finalFilePath,
                message: `Original location was busy. Saved as: ${path.basename(finalFilePath)}`
              };
            } catch (finalError) {
              return { 
                success: false, 
                error: `Failed to save export file. Please close any open Excel files and try again. Error: ${finalError.message}`
              };
            }
          }
          
          // Wait a bit before retry
          await new Promise(resolve => setTimeout(resolve, 1000));
        } else {
          throw writeError;
        }
      }
    }
  } catch (err) {
    console.error('export-patients failed:', err);
    return { success: false, error: err && err.message ? err.message : String(err) };
  }
});

// ✅ Soft delete patient
ipcMain.handle("delete-patient", (event, pid) => {
  const now = getLocalISOString();
  return db.prepare(`
    UPDATE patients
    SET is_deleted = 1, is_active = 0, mod_date = ?
    WHERE id = ?
  `).run(now, pid);
});

function startWhatsAppClient(profileName, event = null) {
  if (activeClients[profileName]) {
    console.log(`♻️ Reusing existing client for ${profileName}`);
    return activeClients[profileName];
  }

  // Ensure LocalAuth uses a writable path (userData) when the app is packaged.
  const authBasePath = path.join(app.getPath('userData'), '.wwebjs_auth');
  try { fs.mkdirSync(authBasePath, { recursive: true }); } catch (e) {}

  // Try to detect an installed Chrome/Chromium on Windows so puppeteer can use it
  function findChromeOnWindows() {
    const candidates = [
      process.env['PROGRAMFILES'] && path.join(process.env['PROGRAMFILES'], 'Google', 'Chrome', 'Application', 'chrome.exe'),
      process.env['PROGRAMFILES(X86)'] && path.join(process.env['PROGRAMFILES(X86)'], 'Google', 'Chrome', 'Application', 'chrome.exe'),
      process.env['LOCALAPPDATA'] && path.join(process.env['LOCALAPPDATA'], 'Google', 'Chrome', 'Application', 'chrome.exe')
    ].filter(Boolean);

    for (const p of candidates) {
      try { if (fs.existsSync(p)) return p; } catch (e) {}
    }
    return null;
  }

  const chromePath = process.platform === 'win32' ? findChromeOnWindows() : null;
  //console.log(`Using LocalAuth base: ${authBasePath}`);
  //console.log(`Detected chrome executable: ${chromePath || 'none'}`);

  // ADVANCED anti-detection browser configuration
  const antiDetectionArgs = [
    "--no-sandbox",
    "--disable-setuid-sandbox",
    
    // CRITICAL: Remove automation detection banners and indicators
    "--disable-blink-features=AutomationControlled",
    "--exclude-switches=enable-automation",
    "--disable-extensions-except",
    "--disable-plugins-discovery",
    "--disable-plugins",
    
    // Hide automation APIs and interfaces  
    "--disable-features=VizDisplayCompositor,TranslateUI,BlinkGenPropertyTrees",
    "--disable-dev-shm-usage",
    "--disable-web-security",
    "--disable-features=VizDisplayCompositor",
    "--disable-ipc-flooding-protection",
    
    // Remove automation signatures
    "--no-first-run",
    "--no-default-browser-check", 
    "--disable-default-apps",
    "--disable-popup-blocking",
    "--disable-prompt-on-repost",
    "--disable-hang-monitor",
    "--disable-sync",
    
    // Performance & memory masking
    "--disable-background-timer-throttling",
    "--disable-backgrounding-occluded-windows", 
    "--disable-renderer-backgrounding",
    "--disable-background-networking",
    "--memory-pressure-off",
    "--max_old_space_size=4096",
    
    // Network & fingerprint masking
    "--disable-logging",
    "--disable-gpu-logging",
    "--silent-debugger-extension-api",
    "--disable-extensions-file-access-check",
    "--disable-extensions-http-throttling",
    
    // Randomized window size to avoid fingerprinting
    `--window-size=${1200 + Math.floor(Math.random() * 400)},${800 + Math.floor(Math.random() * 300)}`,
    
    // User data simulation
    "--enable-features=NetworkService,NetworkServiceLogging",
    "--use-fake-ui-for-media-stream",
    "--use-fake-device-for-media-stream"
  ];

  // Randomized user agents for different profiles
  const userAgents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
  ];
  
  // Select consistent user agent for this profile (based on profile name hash)
  const profileHash = profileName.split('').reduce((a, b) => a + b.charCodeAt(0), 0);
  const selectedUserAgent = userAgents[profileHash % userAgents.length];

  // Create puppeteer config without userDataDir when using LocalAuth
  // LocalAuth manages its own user data directory internally
  const puppeteerConfig = {
    headless: true, 
    executablePath: chromePath || undefined, 
    args: antiDetectionArgs,
    
    // ADVANCED stealth configuration
    defaultViewport: null,
    // NOTE: userDataDir removed - LocalAuth manages this internally
    // This fixes: "LocalAuth is not compatible with a user-supplied userDataDir"
    ignoreDefaultArgs: [
      '--enable-automation',
      '--enable-blink-features=AutomationControlled',
      '--disable-background-timer-throttling',
      '--disable-backgrounding-occluded-windows',
      '--disable-renderer-backgrounding'
    ],
    
    // Additional stealth options
    ignoreHTTPSErrors: true,
    devtools: false,
    
    // Handle permissions to avoid prompts
    args: antiDetectionArgs.concat([
      '--disable-notifications',
      '--disable-geolocation', 
      '--disable-microphone',
      '--disable-camera'
    ])
  };

  // For EXE compatibility, ensure we have proper temp directory fallback
  if (process.pkg && !chromePath) {
    // When packaged as EXE and no Chrome found, add additional args for stability
    puppeteerConfig.args = puppeteerConfig.args.concat([
      '--disable-dev-shm-usage',
      '--disable-gpu',
      '--single-process'
    ]);
  }

  const client = new Client({
    authStrategy: new LocalAuth({ 
      clientId: profileName, 
      dataPath: authBasePath 
    }),
    puppeteer: puppeteerConfig,
    
    // Session configuration
    takeoverOnConflict: false,
    takeoverTimeoutMs: 0,
  });

  // Register client early so debug endpoints can see it while it initializes
  try {
    activeClients[profileName] = client;
  } catch (e) {
    console.error('Failed to register client in activeClients early:', e.message);
  }

  // Extra logging to surface errors when running as EXE and alert jobs
  client.on('auth_failure', (msg) => {
    console.error(`Auth failure for ${profileName}:`, msg);
    logAlert({ profile: profileName, level: 'error', type: 'auth_failure', message: String(msg) });
    // pause any jobs using this profile
    const jobs = profileJobs[profileName] ? Array.from(profileJobs[profileName]) : [];
    for (const jid of jobs) {
      if (jobControllers[jid]) {
        jobControllers[jid].paused = true;
        updateJobInLog(getJsonLogsPath(), jid, { status: 'paused', paused: true, error: `auth_failure: ${msg}` });
        try { if (mainWin) mainWin.webContents.send('job-paused', jid); } catch (e) {}
      }
    }
  });
  client.on('change_state', state => {
    console.log(`State changed for ${profileName}:`, state);
  });

  client.on("qr", async qr => {
    //console.log(`⚠️ QR requested for ${profileName}`);
    if (event) {
      const qrImage = await QRCode.toDataURL(qr);
      event.sender.send("show-qr", { profileName, qrImage });
    }
  });

  client.on("ready", async () => {
    //console.log(`✅ ${profileName} logged in`);
    
    // COMPREHENSIVE anti-detection: Setup browser automation concealment
    try {
      // CRITICAL: Inject advanced stealth scripts to completely hide automation
      await client.pupPage.evaluateOnNewDocument(() => {
        // 1. REMOVE ALL AUTOMATION SIGNATURES
        delete navigator.__proto__.webdriver;
        delete window.chrome.runtime.onConnect;
        delete window.chrome.runtime.onMessage;
        
        // 2. OVERRIDE AUTOMATION DETECTION PROPERTIES
        Object.defineProperty(navigator, 'webdriver', {
          get: () => false,
          configurable: true
        });
        
        // 3. MASK AUTOMATION FLAGS
        Object.defineProperty(window, 'chrome', {
          get: () => ({
            runtime: {},
            loadTimes: function() {},
            csi: function() {},
            app: {}
          }),
          configurable: true
        });
        
        // 4. SIMULATE REAL BROWSER PLUGINS
        Object.defineProperty(navigator, 'plugins', {
          get: () => [
            {
              0: {type: "application/x-google-chrome-pdf", suffixes: "pdf", description: "Portable Document Format"},
              description: "Portable Document Format",
              filename: "internal-pdf-viewer",
              length: 1,
              name: "Chrome PDF Plugin"
            },
            {
              0: {type: "application/pdf", suffixes: "pdf", description: ""},
              description: "",
              filename: "mhjfbmdgcfjbbpaeojofohoefgiehjai",
              length: 1,
              name: "Chrome PDF Viewer"
            }
          ],
          configurable: true
        });
        
        // 5. REALISTIC LANGUAGE PREFERENCES
        Object.defineProperty(navigator, 'languages', {
          get: () => ['en-US', 'en', 'es'],
          configurable: true
        });
        
        // 6. OVERRIDE PERMISSIONS API
        const originalQuery = window.navigator.permissions.query;
        window.navigator.permissions.query = (parameters) => (
          parameters.name === 'notifications' ?
            Promise.resolve({ state: 'granted' }) :
            originalQuery(parameters)
        );
        
        // 7. ADVANCED CANVAS FINGERPRINTING PROTECTION
        const getContext = HTMLCanvasElement.prototype.getContext;
        HTMLCanvasElement.prototype.getContext = function(type) {
          if (type === '2d') {
            const context = getContext.call(this, type);
            const originalFillText = context.fillText;
            const originalStrokeText = context.strokeText;
            
            // Add slight noise to text rendering
            context.fillText = function() {
              const noise = (Math.random() - 0.5) * 0.0001;
              arguments[1] += noise;
              arguments[2] += noise;
              return originalFillText.apply(this, arguments);
            };
            
            context.strokeText = function() {
              const noise = (Math.random() - 0.5) * 0.0001;  
              arguments[1] += noise;
              arguments[2] += noise;
              return originalStrokeText.apply(this, arguments);
            };
            
            return context;
          }
          return getContext.call(this, type);
        };
        
        // 8. WEBGL FINGERPRINTING PROTECTION  
        const getParameter = WebGLRenderingContext.prototype.getParameter;
        WebGLRenderingContext.prototype.getParameter = function(parameter) {
          // Randomize GPU renderer strings
          if (parameter === 37445) {
            return 'Intel Inc.';
          }
          if (parameter === 37446) {
            return 'Intel(R) HD Graphics 620';  
          }
          return getParameter.call(this, parameter);
        };
        
        // 9. AUDIO CONTEXT FINGERPRINTING PROTECTION
        const originalGetChannelData = AudioBuffer.prototype.getChannelData;
        AudioBuffer.prototype.getChannelData = function() {
          const originalData = originalGetChannelData.apply(this, arguments);
          // Add minimal noise to audio fingerprinting
          for (let i = 0; i < originalData.length; i += 100) {
            originalData[i] = originalData[i] + (Math.random() - 0.5) * 0.00001;
          }
          return originalData;
        };
        
        // 10. SCREEN RESOLUTION RANDOMIZATION
        Object.defineProperty(screen, 'width', {
          get: () => 1920 + Math.floor(Math.random() * 100) - 50
        });
        Object.defineProperty(screen, 'height', {
          get: () => 1080 + Math.floor(Math.random() * 100) - 50
        });
        
        // 11. TIMEZONE NORMALIZATION
        Date.prototype.getTimezoneOffset = function() {
          return -330; // IST timezone offset
        };
        
        // 12. MEMORY INFO SPOOFING
        Object.defineProperty(navigator, 'deviceMemory', {
          get: () => 8,
          configurable: true
        });
        
        // 13. CONNECTION INFO SPOOFING
        Object.defineProperty(navigator, 'connection', {
          get: () => ({
            effectiveType: '4g',
            downlink: 10,
            rtt: 50
          }),
          configurable: true
        });
        
        console.log('[STEALTH] Advanced anti-detection measures activated');
      });
      
      // Set randomized user agent
      await client.pupPage.setUserAgent(selectedUserAgent);
      
      // Set realistic viewport with slight randomization
      await client.pupPage.setViewport({
        width: 1366 + Math.floor(Math.random() * 200) - 100,
        height: 768 + Math.floor(Math.random() * 200) - 100,
        deviceScaleFactor: 1,
        isMobile: false,
        hasTouch: false
      });
      
      // Set realistic geolocation (optional)
      await client.pupPage.setGeolocation({
        latitude: 28.6139 + (Math.random() - 0.5) * 0.1, // Delhi area with variance
        longitude: 77.2090 + (Math.random() - 0.5) * 0.1,
        accuracy: 100
      });
      
      // Add realistic browser history and session storage
      await client.pupPage.evaluateOnNewDocument(() => {
        // Simulate browser history
        localStorage.setItem('lastVisit', Date.now() - Math.floor(Math.random() * 86400000));
        
        // Add some realistic session storage
        sessionStorage.setItem('sessionId', Math.random().toString(36).substr(2, 9));
        sessionStorage.setItem('startTime', Date.now());
      });
      
      // Apply additional stealth countermeasures
      await addStealthCountermeasures(client.pupPage);
      
      // Setup network traffic masking
      await setupNetworkMasking(client.pupPage);
      
      // Simulate browser extensions for realism
      await simulateBrowserExtensions(client.pupPage);
      
      //console.log(`[${profileName}] ✅ MAXIMUM STEALTH MODE ACTIVATED`);
      
    } catch (error) {
      console.log(`[${profileName}] Stealth setup warning:`, error.message);
    }
    
    activeClients[profileName] = client; // cache client

  // Store the actual session directory under userData
  const authBasePath = path.join(getUserDataDir(), '.wwebjs_auth');
  const sessionFolder = path.join(authBasePath, `session-${profileName}`);
  addOrUpdateProfile(profileName, sessionFolder, client.info);

    if (event) event.sender.send("profile-added", profileName);
    if (mainWin) mainWin.webContents.send("refresh-profiles");
  });

  client.on("disconnected", reason => {
    console.log(`⚠️ ${profileName} disconnected: ${reason}`);
    delete activeClients[profileName]; // remove from cache
    logAlert({ profile: profileName, level: 'warning', type: 'disconnected', message: String(reason) });
    let profiles = loadProfiles();
    let profile = profiles.find(p => p.name === profileName);
    if (profile) {
      profile.is_active = 0;
      profile.modified = getLocalISOString();
      saveProfiles(profiles);
    }
    // pause jobs associated with this profile
    const jobs = profileJobs[profileName] ? Array.from(profileJobs[profileName]) : [];
    for (const jid of jobs) {
      if (jobControllers[jid]) {
        jobControllers[jid].paused = true;
        updateJobInLog(getJsonLogsPath(), jid, { status: 'paused', paused: true, error: `disconnected: ${reason}` });
        try { if (mainWin) mainWin.webContents.send('job-paused', jid); } catch (e) {}
      }
    }
    if (mainWin) mainWin.webContents.send("refresh-profiles");
  });

  client.initialize();
  return client;
}

function formatNumber(num) {
  if (!num) return null;
  num = num.toString().replace(/\D/g, ""); // remove non-digit chars

  // WhatsApp IDs shouldn't have leading 0
  if (num.startsWith("0")) {
    num = num.slice(1);
  }

  // Enforce reasonable length: minimum 10 (local), maximum 12 (country+local)
  if (num.length < 10) return null;
  if (num.length > 12) return null;

  // If number is exactly 10 digits, assume local and prepend country code 91
  if (num.length === 10) {
    num = '91' + num;
  }

  // Otherwise, if number already includes country code (e.g., 11-12 digits), leave as-is
  return num + "@c.us";
}
  
// Verify and format a phone number for sending. Returns an object:
// { valid: boolean, jid: string|null, reason: string|null, isWhatsApp: boolean|null }
// - reason: 'missing' | 'too_short' | 'invalid_eleven_91' | 'not_whatsapp' | 'format_error' | 'check_failed' | null
async function verifyAndFormatPhone(client, rawPhone) {
  const out = { valid: false, jid: null, reason: null, isWhatsApp: null };
  try {
    if (!rawPhone && rawPhone !== 0) {
      out.reason = 'missing';
      return out;
    }
    const s = String(rawPhone).trim();
    const digits = s.replace(/\D/g, '');
    if (!digits || digits.length < 10) {
      out.reason = 'too_short';
      return out;
    }

    // Reject 11-digit numbers that start with '91' as invalid per user rule
    // (explicit policy: if number is 11 digits and starts with 91, mark as not valid)
    if (digits.length === 11 && digits.startsWith('91')) {
      out.reason = 'invalid_eleven_91';
      return out;
    }

    // formatNumber will add country code assumptions and return JID
    let jid;
    try {
      jid = formatNumber(digits);
    } catch (e) {
      out.reason = 'format_error';
      return out;
    }
    out.jid = jid;

    // If client provided and supports isRegisteredUser, check WhatsApp registration
    if (client && typeof client.isRegisteredUser === 'function') {
      try {
        const registered = await client.isRegisteredUser(jid);
        out.isWhatsApp = !!registered;
        if (!registered) {
          out.reason = 'not_whatsapp';
          return out;
        }
      } catch (e) {
        // If the check fails for some reason, return check_failed but still provide jid
        out.isWhatsApp = null;
        out.reason = 'check_failed';
        out.valid = true; // allow sending attempt when check can't be made
        return out;
      }
    }

    out.valid = true;
    return out;
  } catch (e) {
    out.reason = 'format_error';
    return out;
  }
}
function GettemplateText(templateName) {
  // Use DB-backed loader (loadTemplates prefers DB and falls back to JSON)
  const templates = loadTemplates();
  const template = templates.find(t => t.name === templateName && t.is_delete === 0);

  if (!template) {
    throw new Error(`Template "${templateName}" not found or is marked deleted.`);
  }

  return template.message;
}

// Get complete template data including media info
function getTemplateData(templateName) {
  const templates = loadTemplates();
  const template = templates.find(t => t.name === templateName && t.is_delete === 0);

  if (!template) {
    throw new Error(`Template "${templateName}" not found or is marked deleted.`);
  }

  return {
    name: template.name,
    type: template.type || 'Text',
    message: template.message || '',
    media_path: template.media_path,
    media_filename: template.media_filename
  };
}

function splitIntoChunks(array, numChunks) {
  const chunkSize = Math.ceil(array.length / numChunks);
  const chunks = [];
  for (let i = 0; i < numChunks; i++) {
    chunks.push(array.slice(i * chunkSize, (i + 1) * chunkSize));
  }
  return chunks;
}

function readJsonLog(filePath) {
  const fp = filePath || getJsonLogsPath();
  if (!fs.existsSync(fp)) return [];
  try {
    const raw = fs.readFileSync(fp, "utf-8");
    return JSON.parse(raw);
  } catch (e) {
    console.error("Failed to read JSON log:", e.message);
    return [];
  }
}

function writeJsonLog(filePath, data) {
  const fp = filePath || getJsonLogsPath();
  try {
    fs.writeFileSync(fp, JSON.stringify(data, null, 2), "utf-8");
  } catch (e) {
    console.error("Failed to write JSON log:", e.message);
  }
}

function updateJobInLog(filePath, jobId, updates) {
  const fp = filePath || getJsonLogsPath();
  const logs = readJsonLog(fp);
  const index = logs.findIndex(job => job.id === jobId);
  if (index === -1) return;
  logs[index] = { ...logs[index], ...updates };
  writeJsonLog(fp, logs);
}

function setImmediatePromise() {
  return new Promise(resolve => setImmediate(resolve));
}

const supportedFormats = [
  "YYYY-MM-DD",
  "DD-MM-YYYY",
  "DD/MM/YYYY",
  "D MMM YYYY",
  "D-MMM-YYYY",
  "MMMM D, YYYY",
  "MMM D, YYYY"
];

function parseDate(input) {
  // Handle null, undefined, empty string, or whitespace-only strings
  if (!input || String(input).trim() === '') return null;

  // ✅ Excel serial number (e.g. 45920)
  if (typeof input === "number") {
    const excelEpoch = new Date(Date.UTC(1899, 11, 30));
    const date = new Date(excelEpoch.getTime() + input * 86400000);
    return dayjs(date).format("YYYY-MM-DD");
  }

  // ✅ Native JS Date
  if (input instanceof Date && !isNaN(input)) {
    return dayjs(input).format("YYYY-MM-DD");
  }

  // ✅ String with custom formats
  if (typeof input === "string") {
    const trimmed = input.trim();
    if (trimmed === '') return null; // Handle empty trimmed strings
    const parsed = dayjs(trimmed, supportedFormats, true);
    return parsed.isValid() ? parsed.format("YYYY-MM-DD") : null;
  }

  // ❌ Fallback
  return null;
}

// New function specifically for parsing datetime fields (like last message sent date)
function parseDateTime(input) {
  // Handle null, undefined, empty string, or whitespace-only strings
  if (!input || String(input).trim() === '') return null;

  // ✅ Excel serial number (e.g. 45920) - convert to datetime with default time
  if (typeof input === "number") {
    const excelEpoch = new Date(Date.UTC(1899, 11, 30));
    const date = new Date(excelEpoch.getTime() + input * 86400000);
    // If it's a date-only value, add default time (12:00:00)
    return dayjs(date).toISOString();
  }

  // ✅ Native JS Date
  if (input instanceof Date && !isNaN(input)) {
    const parsed = dayjs(input);
    // Check if time is exactly midnight (00:00:00) - likely date-only input
    if (parsed.hour() === 0 && parsed.minute() === 0 && parsed.second() === 0) {
      // Add default time (12:00:00) for date-only inputs
      return parsed.hour(12).toISOString();
    }
    return parsed.toISOString();
  }

  // ✅ String with custom formats
  if (typeof input === "string") {
    const trimmed = input.trim();
    if (trimmed === '') return null; // Handle empty trimmed strings
    
    const parsed = dayjs(trimmed, supportedFormats, true);
    if (parsed.isValid()) {
      // Check if the input string contains time information
      const hasTime = /\d{1,2}:\d{2}/.test(trimmed) || trimmed.includes('AM') || trimmed.includes('PM');
      
      if (!hasTime) {
        // Date-only string - add default time (12:00:00)
        return parsed.hour(12).minute(0).second(0).toISOString();
      } else {
        // Already has time - preserve it
        return parsed.toISOString();
      }
    }
    return null;
  }

  // ❌ Fallback
  return null;
}

ipcMain.on("force-exit", (event, jobIds) => {
  jobIds.forEach((jobId) => {
    if (jobControllers[jobId]) {
      jobControllers[jobId].cancelled = true;
      // update JSON log also
      updateJobInLog(jsonLogPath, jobId, {
        status: "cancelled"
      });
    }
  });
  // Now really close the window
  app.exit(0);
});

ipcMain.on("normal-exit", () => {
  app.exit(0);
});


const updateDndByPhone = db.prepare(`
  UPDATE patients
  SET
    mod_date = @mod_date,
    Is_DND = @Is_DND
  WHERE phone = @phone
`);

ipcMain.handle("update-dnd", (event, patient) => {
  try {
    const now = getLocalISOString();
    // Accept either { phone, isDnd } or an object with only phone (legacy)
    const flag = patient && typeof patient.isDnd !== 'undefined' ? (Number(patient.isDnd) ? 1 : 0) : 1;

    // Reuse the same normalization logic as set-dnd
    function phoneVariants(raw) {
      if (!raw) return [];
      const s = String(raw).trim();
      const digits = s.replace(/\D/g, '');
      const out = new Set([s, digits]);
      if (digits) {
        const noLeadingZeros = digits.replace(/^0+/, '');
        out.add(noLeadingZeros);
        if (!noLeadingZeros.startsWith('91')) out.add('91' + noLeadingZeros);
        else out.add(noLeadingZeros.slice(2));
        out.add('+' + noLeadingZeros);
      }
      return Array.from(out);
    }

    const phone = patient && (patient.phone || patient.phonenumber || patient.number) ? (patient.phone || patient.phonenumber || patient.number) : null;
    if (!phone) {
      return { success: false, error: 'No phone provided' };
    }

    // Try exact variants first using prepared statement
    const variants = phoneVariants(phone);
    let totalChanges = 0;
    for (const v of variants) {
      const r = updateDndByPhone.run({ phone: v, mod_date: now, Is_DND: flag });
      totalChanges += (r && r.changes) ? r.changes : 0;
    }

    // Fallback: fuzzy match on digits inside stored phone
    if (totalChanges === 0) {
      const digits = String(phone).replace(/\D/g, '');
      if (digits) {
        const r = db.prepare("UPDATE patients SET mod_date = ?, Is_DND = ? WHERE REPLACE(REPLACE(REPLACE(phone,' ',''),'+',''),'-','') LIKE '%' || ? || '%' ").run(now, flag, digits);
        totalChanges += (r && r.changes) ? r.changes : 0;
      }
    }

    console.log(`update-dnd for=${phone} isDnd=${flag} -> totalChanges=${totalChanges}`);
    return { success: totalChanges > 0, changes: totalChanges };
  } catch (e) {
    console.error('update-dnd error:', e.message);
    return { success: false, error: e.message };
  }
});

async function getDiskSerial() {
  try {
    const disks = await si.diskLayout();
    return disks[0]?.serialNum?.trim() || "UNKNOWN";
  } catch {
    return "UNKNOWN";
  }
}

ipcMain.on("login-request", async (event, credentials) => {
  try {
    const serial = await getDiskSerial();
    // console.log("Disk Serial:", serial);
    const response = await axios.post("https://ticket.digitalsolutions.co.in/WinApplogin", {
      Email: credentials.username,
      Password: credentials.password,
      HwrDtls: serial
    });
    //console.log(response);
    const result = response.data;
    // console.log(result);
    //return
   // const status = response.data?.status || "failed";

    // If API returned user details, persist them for later use
    try {
      const ud = result && (result.UserDetails || result.userDetails || null);
      if (ud) {
        const licenseInfo = {
          mudId: ud.ID || ud.id || null,
          email: ud.Email || ud.email || credentials.username,
          name: ud.Name || ud.name || null,
          profileCount: (ud.ProfileCount !== undefined && ud.ProfileCount !== null) ? Number(ud.ProfileCount) : (ud.Profiles || null),
          expiry: ud.ExpiryDate || ud.expiryDate || ud.Expiry || null,
          raw: ud
        };

        // Persist licenseInfo but merge into existing settings so we don't overwrite other keys
        try {
          const settings = loadSettings() || {};
          settings.licenseInfo = licenseInfo;

          // Note: defaultMessageLimitPerProfile is now controlled only from UI settings, not from API
          
          try { saveSettings(settings); } catch (e) { console.warn('Failed to save merged settings', e && e.message ? e.message : e); }
        } catch (e) {
          console.warn('Failed to merge licenseInfo into settings', e && e.message ? e.message : e);
        }

        try { event.reply('login-data', licenseInfo); } catch (e) {}
      }
    } catch (e) {
      console.warn('Failed to extract UserDetails from login response', e && e.message ? e.message : e);
    }

    if (result.Success) {
      const msg = result.Success;
      //console.log("Message from API:", msg);

      if (msg === "Profile is Active") {
          createMainWindow();

          // After successful login, attempt to sync daily_stats to Digitalsolutions
          (async () => {
            try {
              const userInfo = {
                email: (result && result.UserDetails && (result.UserDetails.Email || result.UserDetails.email)) || credentials.username,
                name: (result && result.UserDetails && (result.UserDetails.Name || result.UserDetails.name)) || null,
                id: (result && result.UserDetails && (result.UserDetails.ID || result.UserDetails.id)) || null
              };
              const syncRes = await sendDailyStatsToDigitalsolutions(userInfo);
              if (!syncRes || !syncRes.success) {
                console.warn('Daily stats sync did not succeed after login', syncRes && syncRes.error ? syncRes.error : syncRes);
              } else {
                console.log('Daily stats synced successfully after login');
              }
            } catch (e) {
              console.warn('Daily stats sync failed after login:', e && e.message ? e.message : e);
            }
          })();
        // After login success and main window created, trigger an update check
        try {
          if (autoUpdater && app.isPackaged) {
            // delay a bit to allow main window to finish loading
            setTimeout(() => {
              try { if (mainWin) mainWin.webContents.send('update-checking'); } catch (e) {}
              try { autoUpdater.checkForUpdatesAndNotify(); } catch (e) { log && log.error && log.error('AutoUpdater check failed:', e && e.message ? e.message : e); }
            }, 2000);
          } else {
            // In dev mode or if autoUpdater missing, notify renderer that update checks are disabled
            try { if (mainWin) mainWin.webContents.send('update-not-available', { message: 'Update checks disabled in development' }); } catch (e) {}
          }
        } catch (e) {
          console.warn('Failed to initiate update check after login:', e && e.message ? e.message : e);
        }
      } else if (msg === "Already Activated") {
        event.reply("login-reactivate", { message: "This User is registered on Other system Do you want to Activate here?",email: credentials.username });
      } else if (msg === "Already Activated within 24 hr") {
        event.reply("login-failed", { message: "Activated on Other Device, Cannot Change within 24 Hours." });
      } else {
        event.reply("login-notactivated", { message: "This System is not activated",email: credentials.username });
      }
    } else {
      event.reply("login-failed", { message: "Invalid response from server" });
    }
  } catch (err) {
    //console.error("Login error:", err.message);
    event.reply("login-failed", { message: err.message });
  }
});

ipcMain.handle("Activate-here", async  (event, patient) => {
  Email = patient.email;
  //console.log(Email)
  // console.log(patient)
  if (Email) {
    const serial = await getDiskSerial();
    //console.log("Disk Serial:", serial);
    const response = await axios.post("https://ticket.digitalsolutions.co.in/WinAppActivate", {
      Email: Email,

      HwrDtls: serial
    });
    // console.log(response);
    const result = response.data;
    //console.log(result);
    return result; // 👈 return to renderer
    
  } else {
    console.log("Email not found in controllers:");
  }
});

// IPC to get stored license info
ipcMain.handle('get-license-info', () => {
  try {
    const settings = loadSettings();
    return settings.licenseInfo || null;
  } catch (e) {
    console.error('get-license-info failed', e.message);
    return null;
  }
});



// Copy samples folder to userData directory on startup
// function copySamplesToUserData() {
//   try {
//     const userDataPath = getUserDataDir();
//     const samplesDestPath = path.join(userDataPath, 'samples');
    
//     // Source samples folder (project directory)
//     let samplesSourcePath;
//     if (app.isPackaged) {
//       // When packaged as EXE, samples are in resources
//       samplesSourcePath = path.join(process.resourcesPath, 'samples');
//     } else {
//       // Development mode - samples in project directory
//       samplesSourcePath = path.join(__dirname, 'samples');
//     }
    
//     console.log('📁 Copying samples folder...');
//     console.log('   From:', samplesSourcePath);
//     console.log('   To:', samplesDestPath);
    
//     // Check if source samples folder exists
//     if (!fs.existsSync(samplesSourcePath)) {
//       console.warn('⚠️ Samples folder not found at:', samplesSourcePath);
      
//       // Fallback: try alternative path for development
//       const altPath = path.join(__dirname, '..', 'samples');
//       if (fs.existsSync(altPath)) {
//         samplesSourcePath = altPath;
//         console.log('✅ Found samples at alternative path:', samplesSourcePath);
//       } else {
//         console.error('❌ Samples folder not found in any expected location');
//         return;
//       }
//     }
    
//     // Create destination directory if it doesn't exist
//     fs.mkdirSync(samplesDestPath, { recursive: true });
    
//     // Copy all files from samples folder
//     const files = fs.readdirSync(samplesSourcePath);
//     let copiedCount = 0;
    
//     for (const file of files) {
//       const sourcePath = path.join(samplesSourcePath, file);
//       const destPath = path.join(samplesDestPath, file);
      
//       try {
//         // Only copy if file doesn't exist or source is newer
//         let shouldCopy = true;
//         if (fs.existsSync(destPath)) {
//           const sourceStats = fs.statSync(sourcePath);
//           const destStats = fs.statSync(destPath);
//           shouldCopy = sourceStats.mtime > destStats.mtime;
//         }
        
//         if (shouldCopy) {
//           fs.copyFileSync(sourcePath, destPath);
//           copiedCount++;
//           console.log(`   ✅ Copied: ${file}`);
//         } else {
//           console.log(`   ⏭️ Skipped: ${file} (already up to date)`);
//         }
//       } catch (e) {
//         console.error(`   ❌ Failed to copy ${file}:`, e.message);
//       }
//     }
    
//     console.log(`📁 Samples copy completed: ${copiedCount}/${files.length} files copied`);
    
//   } catch (error) {
//     console.error('❌ Failed to copy samples folder:', error.message);
//   }
// }

// Copy samples folder to userData directory on startup
function copySamplesToUserData() {
  try {
    const userDataPath = getUserDataDir();
    const samplesDestPath = path.join(userDataPath, 'samples');

    // Resolve source path
    let samplesSourcePath;

    if (app.isPackaged) {
      // For packaged app (EXE)
      samplesSourcePath = path.join(process.resourcesPath, 'samples');
    } else {
      // For development mode
      samplesSourcePath = path.join(__dirname, 'samples');
    }

    console.log('📁 Looking for samples folder...');
    console.log('   Expected source:', samplesSourcePath);

    // Validate source folder exists
    if (!fs.existsSync(samplesSourcePath)) {
      console.error('❌ Samples folder NOT FOUND at:', samplesSourcePath);

      // Fallback for development
      const altPath = path.join(__dirname, '..', 'samples');
      if (fs.existsSync(altPath)) {
        samplesSourcePath = altPath;
        console.log('🔄 Using fallback samples folder:', altPath);
      } else {
        console.error('🚫 No samples folder found in ANY path. Copy aborted.');
        return;
      }
    }

    // Ensure destination exists
    fs.mkdirSync(samplesDestPath, { recursive: true });

    // Get files (skip directories)
    const entries = fs.readdirSync(samplesSourcePath, { withFileTypes: true });

    const files = entries.filter(e => e.isFile()).map(e => e.name);

    console.log(`📦 Found ${files.length} sample file(s). Copying...`);

    let copiedCount = 0;

    for (const file of files) {
      const source = path.join(samplesSourcePath, file);
      const dest = path.join(samplesDestPath, file);

      try {
        let shouldCopy = true;

        if (fs.existsSync(dest)) {
          const srcStat = fs.statSync(source);
          const dstStat = fs.statSync(dest);
          shouldCopy = srcStat.mtime > dstStat.mtime;
        }

        if (shouldCopy) {
          fs.copyFileSync(source, dest);
          console.log(`   ✔ Copied: ${file}`);
          copiedCount++;
        } else {
          console.log(`   ↪ Skipped (up to date): ${file}`);
        }
      } catch (err) {
        console.error(`   ❌ Failed: ${file} → ${err.message}`);
      }
    }

    console.log(`🎉 Samples copy completed → ${copiedCount}/${files.length} files copied`);

  } catch (err) {
    console.error('🔥 Fatal error copying samples:', err.message);
  }
}


app.whenReady().then(() => {
    // Copy samples folder to userData directory first
    copySamplesToUserData();
    
    // Ensure default settings exist (non-destructive). If user hasn't set a custom
    // findChatRetryDelayMs, persist a sensible default so retry timing can be adjusted
    // from settings later without changing code.
    try {
      const current = (typeof loadSettings === 'function') ? loadSettings() : {};
      if (!current || typeof current.findChatRetryDelayMs === 'undefined' || current.findChatRetryDelayMs === null) {
        try {
          // Save just this key without overwriting others
          saveSettings({ findChatRetryDelayMs: 3000 }); // default 3000 ms
          console.log('Default setting applied: findChatRetryDelayMs = 3000 ms');
        } catch (e) {
          console.warn('Failed to persist default findChatRetryDelayMs setting:', e && e.message ? e.message : e);
        }
      } else {
        console.log(`findChatRetryDelayMs already set: ${current.findChatRetryDelayMs} ms`);
      }
    } catch (e) {
      console.warn('Error while ensuring default settings:', e && e.message ? e.message : e);
    }

    //createMainWindow();
    createLoginWindow();
    // 🔄 Auto-load saved profiles on startup
    // const profiles = loadProfiles();
    // profiles.forEach(p => {
    //     if (p.is_active) {
    //         console.log(`Reconnecting profile: ${p.name}`);
    //         startWhatsAppClient(p.name);
    //     }
    // });
});

// Auto-updater setup (GitHub releases via electron-updater)
if (autoUpdater) {
  try {
    // Track download state to prevent premature install
    let isUpdateDownloaded = false;
    let latestUpdateInfo = null;
    autoUpdater.logger = log;
    log.transports.file.level = 'info';
    autoUpdater.autoDownload = true; // let updates download automatically when found

    autoUpdater.on('checking-for-update', () => {
      console.log('AutoUpdater: checking for update...');
      try { if (mainWin) mainWin.webContents.send('update-checking'); } catch (e) {}
    });

    autoUpdater.on('update-available', (info) => {
      console.log('AutoUpdater: update available', info);
      // reset flag when a new update is found
      isUpdateDownloaded = false;
      latestUpdateInfo = info || null;
      try { if (mainWin) mainWin.webContents.send('update-available', info); } catch (e) {}
    });

    autoUpdater.on('update-not-available', (info) => {
      console.log('AutoUpdater: update not available', info);
      try { if (mainWin) mainWin.webContents.send('update-not-available', info); } catch (e) {}
    });

    autoUpdater.on('error', (err) => {
      console.error('AutoUpdater error:', err);
      try { if (mainWin) mainWin.webContents.send('update-error', (err && err.message) ? err.message : String(err)); } catch (e) {}
    });

    autoUpdater.on('download-progress', (progressObj) => {
      // progressObj: { bytesPerSecond, percent, total, transferred }
      try {
        if (mainWin) mainWin.webContents.send('update-progress', progressObj);
        // also log a concise progress line
        console.log(`AutoUpdater: download-progress percent=${progressObj.percent} transferred=${progressObj.transferred}/${progressObj.total}`);
      } catch (e) {}
    });

    autoUpdater.on('update-downloaded', (info) => {
      console.log('AutoUpdater: update downloaded', info);
      isUpdateDownloaded = true;
      latestUpdateInfo = info || latestUpdateInfo;
      try { if (mainWin) mainWin.webContents.send('update-downloaded', info); } catch (e) {}
      try { log && log.info && log.info('AutoUpdater: update-downloaded event received', { info }); } catch (e) {}
    });

    // Expose IPC handlers for renderer to trigger checks or install
    ipcMain.handle('check-for-updates', async () => {
      try {
        if (!app.isPackaged) return { success: false, error: 'App not packaged - skip update check in dev' };
        await autoUpdater.checkForUpdates();
        return { success: true };
      } catch (e) {
        return { success: false, error: e && e.message ? e.message : String(e) };
      }
    });

    ipcMain.handle('install-update', () => {
      try {
        // Prevent install until update is fully downloaded
        if (!isUpdateDownloaded) {
          const msg = 'Update not downloaded yet. Install disabled until download completes.';
          console.warn('install-update blocked:', msg);
          try { if (mainWin) mainWin.webContents.send('install-blocked', { message: msg, info: latestUpdateInfo }); } catch (e) {}
          return { success: false, error: msg };
        }

        // This will restart the app and install the update
        autoUpdater.quitAndInstall();
        return { success: true };
      } catch (e) {
        return { success: false, error: e && e.message ? e.message : String(e) };
      }
    });

    // Optionally check right away in packaged apps
    if (app.isPackaged) {
      setTimeout(() => {
        try { autoUpdater.checkForUpdatesAndNotify(); } catch (e) { console.warn('AutoUpdater initial check failed:', e && e.message ? e.message : e); }
      }, 5000);
    }

    console.log('AutoUpdater initialized');
  } catch (e) {
    console.warn('Failed to initialize autoUpdater:', e && e.message ? e.message : e);
  }
}

// =============================================================================
// ADVANCED MONITORING & SAFETY SYSTEMS
// =============================================================================

// Auto-log safety status every 10 minutes during active operations
setInterval(() => {
  if (sessionHealth.size > 0) {
    logSafetyStatus();
  }
}, 10 * 60 * 1000);

// ADVANCED: Automated profile recovery and rotation
function checkProfileHealth() {
  for (const [profileName, health] of sessionHealth) {
    const timeSinceLastMessage = Date.now() - (health.lastMessageTime || 0);
    const sessionDuration = Date.now() - health.sessionStartTime;
    
    // Auto-recovery for stuck sessions (no activity for 30+ minutes)
    if (timeSinceLastMessage > 30 * 60 * 1000 && health.messagesSent > 0) {
      console.warn(`[${profileName}] Session appears stuck - recommending restart`);
      // Could trigger automatic client restart here if needed
    }
    
    // Recommend rotation for long-running sessions (4+ hours)
    if (sessionDuration > 4 * 60 * 60 * 1000) {
      console.warn(`[${profileName}] Long session detected - consider rotation for optimal safety`);
    }
    
    // Auto-cooldown for suspicious patterns
    if (health.consecutiveFailures >= 3 && health.rateLimitHits >= 2) {
      setCooldown(profileName, 20); // Preemptive cooldown
      console.warn(`[${profileName}] Suspicious pattern detected - preemptive cooldown activated`);
    }
  }
}

// Run health checks every 5 minutes
setInterval(checkProfileHealth, 5 * 60 * 1000);

// Emergency brake - system-wide pause if too many profiles in trouble
function emergencyBrakeCheck() {
  const totalProfiles = sessionHealth.size;
  const problemProfiles = Array.from(sessionHealth.values()).filter(h => 
    h.consecutiveFailures >= 3 || h.rateLimitHits >= 2
  ).length;
  
  if (totalProfiles > 0 && problemProfiles / totalProfiles > 0.6) {
    console.error('🚨 EMERGENCY: 60%+ profiles showing issues - SYSTEM PAUSE RECOMMENDED');
    // Could implement automatic system-wide pause here
  }
}

// Run emergency checks every 2 minutes during operations
setInterval(() => {
  if (sessionHealth.size > 2) {
    emergencyBrakeCheck();
  }
}, 2 * 60 * 1000);

//console.log('🛡️ WhatsApp Multi with MAXIMUM STEALTH MODE - Ready!');
//console.log('✅ All anti-detection measures activated');
//console.log('✅ Advanced monitoring systems online');

// Send messages to profile patients using template sequence logic with full safety features
ipcMain.on('send-profile-messages-with-sequence', async (event, data) => {
  try {
    const { profileName, patients } = data;
    
    if (!profileName || !Array.isArray(patients) || patients.length === 0) {
      event.reply('profile-message-response', { success: false, error: 'Invalid data provided' });
      return;
    }

    // Load settings for safety limits
    const settings = loadSettings();
    const rateLimitPerMinute = settings.rateLimitPerMinute || 10;
    const perProfileLimitPerMinute = settings.perProfileLimitPerMinute || 5;
    const maxPerProfilePerDay = settings.maxPerProfilePerDay || 50;
    const minWaitPeriodDays = settings.minWaitPeriodDays || 7;
    const safeMode = settings.safeMode !== false;

    console.log(`[Safety Settings] Rate: ${rateLimitPerMinute}/min, Per Profile: ${perProfileLimitPerMinute}/min, Daily: ${maxPerProfilePerDay}, Wait: ${minWaitPeriodDays}d, Safe Mode: ${safeMode}`);

    // Declare template sequence variable
    let templateSequence = null;
    
    // Load template sequence
    try {
      const sequenceRow = db.prepare('SELECT * FROM template_sequences ORDER BY created_at DESC LIMIT 1').get();
      if (sequenceRow) {
        let steps = [];
        try {
          steps = JSON.parse(sequenceRow.steps || '[]');
          // Ensure steps are ordered by stepNumber (not sequenceNumber)
          if (Array.isArray(steps)) {
            steps = steps
              .map((step, index) => ({
                ...step,
                stepNumber: step.stepNumber || (index + 1) // Use existing stepNumber or assign sequential
              }))
              .sort((a, b) => a.stepNumber - b.stepNumber);
          }
        } catch (e) {
          console.error('Failed to parse sequence steps:', e.message);
          steps = [];
        }
        
        templateSequence = {
          id: sequenceRow.id,
          name: sequenceRow.name,
          description: sequenceRow.description,
          steps: steps
        };
        console.log(`✅ Template sequence loaded: "${templateSequence.name}" with ${templateSequence.steps.length} steps`);
        console.log(`📋 Sequence steps (ordered by stepNumber): ${templateSequence.steps.map((s, i) => `Step ${s.stepNumber}: ${s.templateName}`).join(', ')}`);
      } else {
        console.warn(`⚠️ No template sequence found in database`);
      }
    } catch (e) {
      console.error('❌ Failed to load template sequence:', e.message);
    }

    // Helper function to get next template from sequence for individual patient
    function getNextTemplateFromSequence(patient, templateSequence) {
      if (!templateSequence || !templateSequence.steps || templateSequence.steps.length === 0) {
        return { templateName: null, stepNumber: 0 }; // No sequence defined
      }
      
      const lastTemplate = patient.last_template || patient.lastTemplate || '';
      const lastScheduleDays = Number(patient.last_schedule_days) || 0;
      
      console.log(`🔍 [Template Logic] Patient: ${patient.name}, Last Template: "${lastTemplate}", Last Step: ${lastScheduleDays}`);
      
      // Case 1: If last_schedule_days = 0 or last_template = 'welcome', start from sequence step 1
      if (lastScheduleDays === 0 || lastTemplate.toLowerCase() === 'welcome') {
        const firstStep = templateSequence.steps[0];
        if (firstStep) {
          console.log(`📋 [Template Logic] Starting from first step: ${firstStep.templateName} (Step 1)`);
          return { templateName: firstStep.templateName, stepNumber: 1 };
        }
      }
      
      // Case 2: Based on last step number, find next available template
      if (lastScheduleDays > 0) {
        // Look for next step number that exists in sequence
        for (let stepNum = lastScheduleDays + 1; stepNum <= templateSequence.steps.length + 5; stepNum++) {
          const stepTemplate = templateSequence.steps.find(step => step.stepNumber === stepNum);
          if (stepTemplate) {
            console.log(`📋 [Template Logic] Found next step ${stepNum}: ${stepTemplate.templateName} (skipped missing steps)`);
            return { templateName: stepTemplate.templateName, stepNumber: stepNum };
          }
        }
        
        // If no higher step found, loop back to first
        const firstStep = templateSequence.steps[0];
        if (firstStep) {
          console.log(`📋 [Template Logic] No higher steps found, looping to first: ${firstStep.templateName} (Step 1)`);
          return { templateName: firstStep.templateName, stepNumber: 1 };
        }
      }
      
      // Case 3: Find current template in sequence and move to next available
      const currentStepIndex = templateSequence.steps.findIndex(step => 
        step.templateName === lastTemplate
      );
      
      if (currentStepIndex >= 0) {
        const currentStep = templateSequence.steps[currentStepIndex];
        const currentStepNumber = currentStep.stepNumber || (currentStepIndex + 1);
        
        console.log(`📋 [Template Logic] Found current template "${lastTemplate}" at position ${currentStepIndex}, step number ${currentStepNumber}`);
        
        // Look for next available step number
        for (let stepNum = currentStepNumber + 1; stepNum <= currentStepNumber + 10; stepNum++) {
          const nextStepTemplate = templateSequence.steps.find(step => step.stepNumber === stepNum);
          if (nextStepTemplate) {
            console.log(`📋 [Template Logic] Found next available step ${stepNum}: ${nextStepTemplate.templateName}`);
            return { templateName: nextStepTemplate.templateName, stepNumber: stepNum };
          }
        }
        
        // If no next step found, try next index in array
        const nextStepIndex = currentStepIndex + 1;
        if (nextStepIndex < templateSequence.steps.length) {
          const nextStep = templateSequence.steps[nextStepIndex];
          const nextStepNumber = nextStep.stepNumber || (nextStepIndex + 1);
          console.log(`📋 [Template Logic] Using next array position: ${nextStep.templateName} (Step ${nextStepNumber})`);
          return { templateName: nextStep.templateName, stepNumber: nextStepNumber };
        } else {
          // Reached end of sequence, loop back to first
          const firstStep = templateSequence.steps[0];
          const firstStepNumber = firstStep.stepNumber || 1;
          console.log(`📋 [Template Logic] End of sequence, looping to first: ${firstStep.templateName} (Step ${firstStepNumber})`);
          return { templateName: firstStep.templateName, stepNumber: firstStepNumber };
        }
      } else {
        // Current template not found in sequence, start from beginning
        const firstStep = templateSequence.steps[0];
        if (firstStep) {
          const firstStepNumber = firstStep.stepNumber || 1;
          console.log(`📋 [Template Logic] Template not found in sequence, starting from first: ${firstStep.templateName} (Step ${firstStepNumber})`);
          return { templateName: firstStep.templateName, stepNumber: firstStepNumber };
        }
      }
      
      return { templateName: null, stepNumber: 0 };
    }

    // Get all active clients for multi-profile support
    const allActiveClients = Object.keys(activeClients).filter(name => activeClients[name]);
    console.log(`[Multi-Profile] Available active profiles: ${allActiveClients.join(', ')}`);

    // If specific profile requested, use only that one; otherwise use all active profiles
    const useProfiles = profileName === 'ALL_ACTIVE' ? allActiveClients : [profileName];
    
    // Validate requested profiles are active
    const validProfiles = useProfiles.filter(p => activeClients[p]);
    if (validProfiles.length === 0) {
      event.reply('profile-message-response', { 
        success: false, 
        error: `No active profiles found. Requested: ${useProfiles.join(', ')}` 
      });
      return;
    }

    console.log(`[Multi-Profile] Using profiles: ${validProfiles.join(', ')}`);

    // Enhanced validation: Check all profiles are properly loaded before starting
    console.log(`🔍 Performing enhanced validation on all profiles...`);
    const profileValidationResults = [];
    
    for (const profile of validProfiles) {
      const validation = await validateProfileReadiness(profile);
      profileValidationResults.push({ profile, validation });
      
      if (!validation.ready) {
        console.warn(`❌ Profile ${profile} failed validation: ${validation.error}`);
      } else {
        console.log(`✅ Profile ${profile} validation passed`);
      }
    }
    
    // Filter to only truly ready profiles
    const readyProfiles = profileValidationResults
      .filter(result => result.validation.ready)
      .map(result => result.profile);
    
    const failedProfiles = profileValidationResults
      .filter(result => !result.validation.ready)
      .map(result => ({ profile: result.profile, error: result.validation.error }));
    
    if (readyProfiles.length === 0) {
      const errorMsg = `No profiles are ready to send messages. Failed validations: ${failedProfiles.map(f => `${f.profile} (${f.error})`).join(', ')}`;
      console.error(`❌ ${errorMsg}`);
      event.reply('profile-message-response', { 
        success: false, 
        error: errorMsg,
        failedProfiles: failedProfiles
      });
      return;
    }
    
    if (failedProfiles.length > 0) {
      console.warn(`⚠️ Some profiles failed validation and will be skipped: ${failedProfiles.map(f => f.profile).join(', ')}`);
    }
    
    console.log(`✅ Proceeding with ${readyProfiles.length} ready profiles: ${readyProfiles.join(', ')}`);

    console.log(`\n🚀 [TEMPLATE SEQUENCE MESSAGING] Starting job...`);
    console.log(`📋 Input data:`, {
      profileName,
      requestedPatients: patients.length,
      readyProfiles: readyProfiles.length,
      profileNames: readyProfiles
    });

    const jobId = 'profilejob_' + getLocalISOString().replace(/[-:.TZ]/g, '');
    
    // ✅ Initialize job controller for pause/resume/cancel support
    jobControllers[jobId] = { paused: false, cancelled: false };
    console.log(`✅ Job controller initialized: ${jobId}`);
    
    // Add job level logging to JSON file
    try {
      const jsonLogPath = getJsonLogsPath();
      const nowStr = getLocalISOString().replace(/[:.]/g, '-');
      const jobMeta = {
        id: jobId,
        timestamp: nowStr,
        profiles: readyProfiles,
        template_name: '(template sequence)',
        record_total: patients.length,
        record_sent: 0,
        record_failed: 0,
        status: 'scheduled'
      };
      
      const existingLogs = readJsonLog(jsonLogPath);
      existingLogs.push(jobMeta);
      writeJsonLog(jsonLogPath, existingLogs);
      console.log(`✅ Job logged to JSON file: ${jobId}`);
    } catch (e) {
      console.warn('⚠️ Failed to log job to JSON file:', e.message);
    }
    
    event.reply('profile-message-response', { 
      success: true, 
      message: 'Job started', 
      jobId, 
      profiles: readyProfiles, 
      total: patients.length 
    });

    // Load template sequence (loading logic moved up)
    try {
      const sequenceRow = db.prepare('SELECT * FROM template_sequences ORDER BY created_at DESC LIMIT 1').get();
      if (sequenceRow) {
        templateSequence = {
          id: sequenceRow.id,
          name: sequenceRow.name,
          description: sequenceRow.description,
          steps: JSON.parse(sequenceRow.steps || '[]')
        };
        console.log(`[Template Sequence] Found sequence: ${templateSequence.name} with ${templateSequence.steps.length} steps`);
      }
    } catch (e) {
      console.error('Failed to load template sequence:', e.message);
    }

    // Check daily limits for each profile
    const today = getLocalISOString().slice(0, 10);
    const dailyCounts = {};
    for (const profile of readyProfiles) {
      try {
        const dailyRow = db.prepare('SELECT per_profile FROM daily_stats WHERE day = ?').get(today);
        const perProfileData = dailyRow ? JSON.parse(dailyRow.per_profile || '{}') : {};
        dailyCounts[profile] = perProfileData[profile] || 0;
        console.log(`[Daily Limit] ${profile}: ${dailyCounts[profile]}/${maxPerProfilePerDay} sent today`);
      } catch (e) {
        dailyCounts[profile] = 0;
      }
    }

    // Filter patients by min wait period
    const now = new Date();
    const eligiblePatients = patients.filter(patient => {
      if (!patient.Last_Msgsent_date || patient.Last_Msgsent_date === 'null' || patient.Last_Msgsent_date === '') {
        return true; // No previous message, eligible
      }
      
      // Skip wait period check if last template was 'welcome'
      const lastTemplate = patient.last_template || patient.lastTemplate || '';
      if (lastTemplate.toLowerCase() === 'welcome') {
        console.log(`Patient ${patient.name || patient.phone} eligible: Last template was 'welcome' - skipping wait period check`);
        return true; // Welcome template - no wait period required
      }
      
      try {
        const lastMsgDate = new Date(patient.Last_Msgsent_date);
        if (isNaN(lastMsgDate.getTime())) return true;
        
        const daysSinceLastMsg = Math.floor((now - lastMsgDate) / (1000 * 60 * 60 * 24));
        return daysSinceLastMsg >= minWaitPeriodDays;
      } catch (e) {
        return true; // Error parsing, allow
      }
    });

    console.log(`[Wait Period Filter] ${patients.length} → ${eligiblePatients.length} patients (${patients.length - eligiblePatients.length} filtered by ${minWaitPeriodDays}d wait period)`);

    // Update job status to in_progress before starting main processing
    try {
      const jsonLogPath = getJsonLogsPath();
      const logs = readJsonLog(jsonLogPath);
      const jobIndex = logs.findIndex(job => job.id === jobId);
      if (jobIndex !== -1) {
        logs[jobIndex].status = 'in_progress';
        writeJsonLog(jsonLogPath, logs);
        console.log(`✅ Job status updated to in_progress: ${jobId}`);
        
        // Send updated reports to refresh UI
        try {
          event.reply("reports-updated", logs);
        } catch (e) {
          console.warn("Failed to send reports update after status change:", e.message);
        }
      }
    } catch (e) {
      console.warn('⚠️ Failed to update job status to in_progress:', e.message);
    }

    // Start async processing with round-robin distribution
    let sent = 0;
    let failed = 0;
    const allLogs = []; // Collect all message logs for Excel export
    const updateLastStmt = db.prepare('UPDATE patients SET Last_Msgsent_date = ?, last_template = ?, last_schedule_days = ?, mod_date = ? WHERE unique_id = ? OR phone = ?');
    
    // Track per-profile message counts for rate limiting
    const profileMessageCounts = {};
    const profileSentWindow = {}; // Track messages sent in last minute
    const globalSentWindow = []; // Track global messages sent in last minute
    
    readyProfiles.forEach(profile => {
      profileMessageCounts[profile] = 0;
      profileSentWindow[profile] = [];
    });

    for (let i = 0; i < eligiblePatients.length; i++) {
      const patient = eligiblePatients[i];
      
      // ✅ Check job controller for pause/cancel status
      const controller = jobControllers[jobId];
      if (!controller) {
        console.warn(`❌ Job controller removed for ${jobId}, stopping`);
        break;
      }
      
      if (controller.cancelled) {
        console.warn(`❌ Job ${jobId} cancelled, stopping`);
        break;
      }
      
      // Handle pause - wait until resumed
      while (controller.paused) {
        console.log(`⏸️ Job ${jobId} paused, waiting...`);
        await new Promise(resolve => setTimeout(resolve, 1000));
      }
      
      console.log(`\n🔄 [${i + 1}/${eligiblePatients.length}] Processing patient: ${patient.name} (${patient.phone})`);
      
      try {
        // Round-robin profile selection
        const currentProfile = readyProfiles[i % readyProfiles.length];
        const client = activeClients[currentProfile];
        
        console.log(`📋 Selected profile: ${currentProfile} for patient ${patient.name}`);
        
        if (!client) {
          console.warn(`❌ FAIL REASON: Profile ${currentProfile} no longer active, skipping patient ${patient.name}`);
          
          // Log failure to message_logs table
          try {
            const messageId = 'msg_' + getLocalISOString().replace(/[-:.TZ]/g, '') + '_' + Math.random().toString(36).substr(2, 6);
            insertMessageLog.run({
              id: messageId,
              job_id: jobId,
              unique_id: patient.unique_id || null,
              name: patient.name || null,
              phone: patient.phone || null,
              profile: currentProfile,
              template: null,
              message: '',
              status: 'Failed',
              sent_at: getLocalISOString(),
              error: `Profile ${currentProfile} no longer active`,
              media_path: null,
              media_filename: null
            });
          } catch (e) {
            console.warn('Failed to log profile inactive to message_logs:', e.message);
          }
          
          // Add to Excel logs collection
          allLogs.push({
            Profile: currentProfile,
            Phone: patient.phone,
            Status: 'Failed',
            Timestamp: getLocalISOString(),
            Message: '',
            Template: '',
            CustomerName: patient.name || '',
            Error: `Profile ${currentProfile} no longer active`
          });
          
          failed++;
          
          // ✅ Send progress update for profile inactive failure
          try {
            event.reply('profile-message-progress', { 
              id: jobId, 
              profile: currentProfile, 
              number: patient.phone, 
              status: 'failed', 
              sent, 
              failed, 
              total: eligiblePatients.length,
              patient: patient.name || 'Unknown',
              error: `Profile ${currentProfile} no longer active`
            });
          } catch (e) {
            console.warn('Failed to send progress update for profile inactive:', e.message);
          }
          
          // ✅ Update job data in JSON file
          try {
            const jsonLogPath = getJsonLogsPath();
            const logs = readJsonLog(jsonLogPath);
            const jobIndex = logs.findIndex(job => job.id === jobId);
            if (jobIndex !== -1) {
              logs[jobIndex].record_sent = sent;
              logs[jobIndex].record_failed = failed;
              writeJsonLog(jsonLogPath, logs);
              event.reply("reports-updated", logs);
            }
          } catch (e) {
            console.warn('⚠️ Failed to update job progress in JSON file:', e.message);
          }
          
          continue;
        }
        console.log(`✅ Profile ${currentProfile} is active`);

        // Check daily limit for this profile
        if (dailyCounts[currentProfile] >= maxPerProfilePerDay) {
          console.warn(`❌ FAIL REASON: Profile ${currentProfile} daily limit reached (${dailyCounts[currentProfile]}/${maxPerProfilePerDay}), skipping patient ${patient.name}`);
          
          // Log failure to message_logs table
          try {
            const messageId = 'msg_' + getLocalISOString().replace(/[-:.TZ]/g, '') + '_' + Math.random().toString(36).substr(2, 6);
            insertMessageLog.run({
              id: messageId,
              job_id: jobId,
              unique_id: patient.unique_id || null,
              name: patient.name || null,
              phone: patient.phone || null,
              profile: currentProfile,
              template: null,
              message: '',
              status: 'Skipped',
              sent_at: getLocalISOString(),
              error: `Daily limit reached (${dailyCounts[currentProfile]}/${maxPerProfilePerDay})`,
              media_path: null,
              media_filename: null
            });
          } catch (e) {
            console.warn('Failed to log daily limit to message_logs:', e.message);
          }
          
          // Add to Excel logs collection
          allLogs.push({
            Profile: currentProfile,
            Phone: patient.phone,
            Status: 'Skipped',
            Timestamp: getLocalISOString(),
            Message: '',
            Template: '',
            CustomerName: patient.name || '',
            Error: `Daily limit reached (${dailyCounts[currentProfile]}/${maxPerProfilePerDay})`
          });
          
          failed++;
          
          // ✅ Send progress update for daily limit failure
          try {
            event.reply('profile-message-progress', { 
              id: jobId, 
              profile: currentProfile, 
              number: patient.phone, 
              status: 'failed', 
              sent, 
              failed, 
              total: eligiblePatients.length,
              patient: patient.name || 'Unknown',
              error: `Daily limit reached (${dailyCounts[currentProfile]}/${maxPerProfilePerDay})`
            });
          } catch (e) {
            console.warn('Failed to send progress update for daily limit:', e.message);
          }
          
          // ✅ Update job data in JSON file
          try {
            const jsonLogPath = getJsonLogsPath();
            const logs = readJsonLog(jsonLogPath);
            const jobIndex = logs.findIndex(job => job.id === jobId);
            if (jobIndex !== -1) {
              logs[jobIndex].record_sent = sent;
              logs[jobIndex].record_failed = failed;
              writeJsonLog(jsonLogPath, logs);
              event.reply("reports-updated", logs);
            }
          } catch (e) {
            console.warn('⚠️ Failed to update job progress in JSON file:', e.message);
          }
          
          continue;
        }
        console.log(`✅ Daily limit check passed: ${dailyCounts[currentProfile]}/${maxPerProfilePerDay}`);

        // Rate limiting check (both global and per-profile)
        const currentTime = Date.now();
        
        // Clean up old entries from both windows
        globalSentWindow.splice(0, globalSentWindow.length, ...globalSentWindow.filter(time => currentTime - time < 60000));
        profileSentWindow[currentProfile] = profileSentWindow[currentProfile].filter(time => currentTime - time < 60000);
        
        // Check global rate limit first
        if (globalSentWindow.length >= rateLimitPerMinute) {
          console.warn(`⏳ GLOBAL RATE LIMIT: Global rate limit reached (${globalSentWindow.length}/${rateLimitPerMinute}/min), waiting 15 seconds...`);
          
          // Wait 15 seconds for some messages to age out of the rate limit window
          await new Promise(resolve => setTimeout(resolve, 30000));
          
          // Re-check the global rate limit after waiting
          const newTime = Date.now();
          globalSentWindow.splice(0, globalSentWindow.length, ...globalSentWindow.filter(time => newTime - time < 60000));
          console.log(`✅ Global rate limit recheck. Current: ${globalSentWindow.length}/${rateLimitPerMinute}/min`);
        }
        
        // Check per-profile rate limit
        if (profileSentWindow[currentProfile].length >= perProfileLimitPerMinute) {
          console.warn(`⏳ PROFILE RATE LIMIT: Profile ${currentProfile} rate limit reached (${profileSentWindow[currentProfile].length}/${perProfileLimitPerMinute}/min), waiting 15 seconds...`);
          
          // Wait 15 seconds for some messages to age out of the rate limit window
          await new Promise(resolve => setTimeout(resolve, 40000));
          
          // Re-check the rate limit after waiting
          const newTime = Date.now();
          profileSentWindow[currentProfile] = profileSentWindow[currentProfile].filter(time => newTime - time < 60000);
          console.log(`✅ Profile rate limit recheck. Current: ${profileSentWindow[currentProfile].length}/${perProfileLimitPerMinute}/min`);
          
          // DO NOT continue - process the same patient after waiting
        }
        console.log(`✅ Rate limit checks passed: Global ${globalSentWindow.length}/${rateLimitPerMinute}/min, Profile ${profileSentWindow[currentProfile].length}/${perProfileLimitPerMinute}/min`);

        // Determine next template for this patient
        console.log(`🔍 Patient data: last_template="${patient.last_template}", last_schedule_days=${patient.last_schedule_days}`);
        const templateInfo = getNextTemplateFromSequence(patient, templateSequence);
        console.log(`📋 Template sequence result:`, templateInfo);
        
        if (!templateInfo.templateName) {
          console.warn(`❌ FAIL REASON: No template determined for patient ${patient.name || patient.unique_id}`);
          console.warn(`   - Last template: "${patient.last_template}"`);
          console.warn(`   - Last schedule days: ${patient.last_schedule_days}`);
          console.warn(`   - Template sequence available: ${templateSequence ? 'YES' : 'NO'}`);
          if (templateSequence) {
            console.warn(`   - Sequence steps: ${templateSequence.steps.length}`);
            console.warn(`   - Available templates: ${templateSequence.steps.map(s => s.templateName).join(', ')}`);
          }
          
          // Log failure to message_logs table
          try {
            const messageId = 'msg_' + getLocalISOString().replace(/[-:.TZ]/g, '') + '_' + Math.random().toString(36).substr(2, 6);
            insertMessageLog.run({
              id: messageId,
              job_id: jobId,
              unique_id: patient.unique_id || null,
              name: patient.name || null,
              phone: patient.phone || null,
              profile: currentProfile,
              template: null,
              message: '',
              status: 'Failed',
              sent_at: getLocalISOString(),
              error: `No template determined. Last: ${patient.last_template}, Step: ${patient.last_schedule_days}`,
              media_path: null,
              media_filename: null
            });
          } catch (e) {
            console.warn('Failed to log template determination failure to message_logs:', e.message);
          }
          
          // Add to Excel logs collection
          allLogs.push({
            Profile: currentProfile,
            Phone: patient.phone,
            Status: 'Failed',
            Timestamp: getLocalISOString(),
            Message: '',
            Template: '',
            CustomerName: patient.name || '',
            Error: `No template determined. Last: ${patient.last_template}, Step: ${patient.last_schedule_days}`
          });
          
          failed++;
          
          // ✅ Send progress update for template determination failure
          try {
            event.reply('profile-message-progress', { 
              id: jobId, 
              profile: currentProfile, 
              number: patient.phone, 
              status: 'failed', 
              sent, 
              failed, 
              total: eligiblePatients.length,
              patient: patient.name || 'Unknown',
              error: `No template determined. Last: ${patient.last_template}, Step: ${patient.last_schedule_days}`
            });
          } catch (e) {
            console.warn('Failed to send progress update for template determination:', e.message);
          }
          
          // ✅ Update job data in JSON file
          try {
            const jsonLogPath = getJsonLogsPath();
            const logs = readJsonLog(jsonLogPath);
            const jobIndex = logs.findIndex(job => job.id === jobId);
            if (jobIndex !== -1) {
              logs[jobIndex].record_sent = sent;
              logs[jobIndex].record_failed = failed;
              writeJsonLog(jsonLogPath, logs);
              event.reply("reports-updated", logs);
            }
          } catch (e) {
            console.warn('⚠️ Failed to update job progress in JSON file:', e.message);
          }
          
          continue;
        }
        console.log(`✅ Template determined: ${templateInfo.templateName} (Step ${templateInfo.stepNumber})`);

        // Get complete template data including media info
        let templateData = null;
        try { 
          templateData = getTemplateData(templateInfo.templateName); 
          console.log(`✅ Template data loaded: Type=${templateData.type}, Message=${templateData.message.length} chars, Media=${templateData.media_path ? 'Yes' : 'No'}`);
        } catch (e) { 
          console.error(`❌ FAIL REASON: Template "${templateInfo.templateName}" not found:`, e.message);
          console.error(`   - Available templates should be checked in templates.json`);
          
          // Log failure to message_logs table
          try {
            const messageId = 'msg_' + getLocalISOString().replace(/[-:.TZ]/g, '') + '_' + Math.random().toString(36).substr(2, 6);
            insertMessageLog.run({
              id: messageId,
              job_id: jobId,
              unique_id: patient.unique_id || null,
              name: patient.name || null,
              phone: patient.phone || null,
              profile: currentProfile,
              template: templateInfo.templateName,
              message: '',
              status: 'Failed',
              sent_at: getLocalISOString(),
              error: `Template "${templateInfo.templateName}" not found: ${e.message}`,
              media_path: null,
              media_filename: null
            });
          } catch (logError) {
            console.warn('Failed to log template not found to message_logs:', logError.message);
          }
          
          // Add to Excel logs collection
          allLogs.push({
            Profile: currentProfile,
            Phone: patient.phone,
            Status: 'Failed',
            Timestamp: getLocalISOString(),
            Message: '',
            Template: templateInfo.templateName,
            CustomerName: patient.name || '',
            Error: `Template "${templateInfo.templateName}" not found: ${e.message}`
          });
          
          failed++;
          continue;
        }

        // Render template message for patient (if any)
        let messageToSend = '';
        if (templateData.message) {
          messageToSend = renderTemplateForPatient(templateData.message, patient);
          console.log(`✅ Message rendered: ${messageToSend.substring(0, 100)}...`);
        }
        
        // VERIFY PHONE BEFORE SENDING
        const phoneCheck = await verifyAndFormatPhone(client, patient.phone);
        if (!phoneCheck.valid) {
          const reason = phoneCheck.reason || 'invalid';
          console.warn(`❌ FAIL REASON: Invalid phone for patient ${patient.name}: ${patient.phone} (${reason})`);

          // If the phone is not a WhatsApp number, mark patient last message date as now so
          // the system won't retry sending to them immediately. This helps avoid repeated
          // attempts for numbers that are known not to be on WhatsApp.
          if (reason === 'not_whatsapp') {
            const ts = getLocalISOString();
            try {
              // Update patient record: set Last_Msgsent_date to now and advance last template/step
              updateLastStmt.run(ts, templateInfo.templateName || '', templateInfo.stepNumber || 0, ts, patient.unique_id || '', patient.phone || '');
              console.log(`Marked patient ${patient.name} (${patient.phone}) as Last_Msgsent_date=${ts} due to not_whatsapp`);
            } catch (e) {
              console.warn('Failed to update patient Last_Msgsent_date for not_whatsapp:', e.message);
            }

            // Log as skipped/marked so UI and exports show this decision
            try {
              const messageId = 'msg_' + getLocalISOString().replace(/[-:.TZ]/g, '') + '_' + Math.random().toString(36).substr(2, 6);
              insertMessageLog.run({
                id: messageId,
                job_id: jobId,
                unique_id: patient.unique_id || null,
                name: patient.name || null,
                phone: patient.phone || null,
                profile: currentProfile,
                template: templateInfo.templateName,
                message: messageToSend ? messageToSend.substring(0, 500) : '',
                status: 'Skipped',
                sent_at: ts,
                error: 'Not WhatsApp - marked as sent date',
                media_path: templateData?.media_path || null,
                media_filename: templateData?.media_filename || null
              });
            } catch (logError) { console.warn('Failed to log not_whatsapp to message_logs:', logError && logError.message ? logError.message : logError); }

            allLogs.push({
              Profile: currentProfile,
              Phone: patient.phone,
              Status: 'Skipped',
              Timestamp: getLocalISOString(),
              Message: messageToSend ? messageToSend.substring(0, 500) : '',
              Template: templateInfo.templateName,
              CustomerName: patient.name || '',
              Error: 'Not WhatsApp - marked as sent date'
            });

            try { event.reply('profile-message-progress', { id: jobId, profile: currentProfile, number: patient.phone, status: 'skipped', sent, failed, total: eligiblePatients.length, patient: patient.name || 'Unknown', error: 'Not WhatsApp - marked as sent date' }); } catch (e) {}
            try { const jsonLogPath = getJsonLogsPath(); const logs = readJsonLog(jsonLogPath); const jobIndex = logs.findIndex(job => job.id === jobId); if (jobIndex !== -1) { logs[jobIndex].record_sent = sent; logs[jobIndex].record_failed = failed; writeJsonLog(jsonLogPath, logs); event.reply("reports-updated", logs); } } catch (e) { console.warn('Failed to update job progress in JSON file:', e.message); }

            // Do not count this as a failure to avoid noisy failure metrics; skip to next patient
            continue;
          }

          // Default behavior for other invalid-phone reasons: log as failure
          try {
            const messageId = 'msg_' + getLocalISOString().replace(/[-:.TZ]/g, '') + '_' + Math.random().toString(36).substr(2, 6);
            insertMessageLog.run({
              id: messageId,
              job_id: jobId,
              unique_id: patient.unique_id || null,
              name: patient.name || null,
              phone: patient.phone || null,
              profile: currentProfile,
              template: templateInfo.templateName,
              message: messageToSend.substring(0, 500),
              status: 'Failed',
              sent_at: getLocalISOString(),
              error: `Invalid phone (${reason})`,
              media_path: templateData?.media_path || null,
              media_filename: templateData?.media_filename || null
            });
          } catch (logError) { console.warn('Failed to log invalid phone to message_logs:', logError && logError.message ? logError.message : logError); }

          allLogs.push({
            Profile: currentProfile,
            Phone: patient.phone,
            Status: 'Failed',
            Timestamp: getLocalISOString(),
            Message: messageToSend.substring(0, 500),
            Template: templateInfo.templateName,
            CustomerName: patient.name || '',
            Error: `Invalid phone (${reason})`
          });

          failed++;
          try { event.reply('profile-message-progress', { id: jobId, profile: currentProfile, number: patient.phone, status: 'failed', sent, failed, total: eligiblePatients.length, patient: patient.name || 'Unknown', error: `Invalid phone (${reason})` }); } catch (e) {}
          try { const jsonLogPath = getJsonLogsPath(); const logs = readJsonLog(jsonLogPath); const jobIndex = logs.findIndex(job => job.id === jobId); if (jobIndex !== -1) { logs[jobIndex].record_sent = sent; logs[jobIndex].record_failed = failed; writeJsonLog(jsonLogPath, logs); event.reply("reports-updated", logs); } } catch (e) { console.warn('Failed to update job progress in JSON file:', e.message); }
          continue;
        }
        const jid = phoneCheck.jid;
        // Ensure JID looks reasonable: must contain more than 10 digits (country+number)
        const jidDigits = String(jid || '').replace(/\D/g, '');
        if (!jid || jidDigits.length <= 10) {
          const reason = 'invalid_jid_length';
          console.warn(`❌ FAIL REASON: Invalid/short JID for patient ${patient.name}: ${jid} (${reason})`);
          try {
            const messageId = 'msg_' + getLocalISOString().replace(/[-:.TZ]/g, '') + '_' + Math.random().toString(36).substr(2, 6);
            insertMessageLog.run({
              id: messageId,
              job_id: jobId,
              unique_id: patient.unique_id || null,
              name: patient.name || null,
              phone: patient.phone || null,
              profile: currentProfile,
              template: templateInfo.templateName,
              message: messageToSend ? messageToSend.substring(0, 500) : '',
              status: 'Failed',
              sent_at: getLocalISOString(),
              error: `Invalid JID (${reason}): ${jid}`,
              media_path: templateData?.media_path || null,
              media_filename: templateData?.media_filename || null
            });
          } catch (logError) {
            console.warn('Failed to log invalid JID to message_logs:', logError && logError.message ? logError.message : logError);
          }

          allLogs.push({
            Profile: currentProfile,
            Phone: patient.phone,
            Status: 'Failed',
            Timestamp: getLocalISOString(),
            Message: messageToSend ? messageToSend.substring(0, 500) : '',
            Template: templateInfo.templateName,
            CustomerName: patient.name || '',
            Error: `Invalid JID (${reason}): ${jid}`
          });

          failed++;
          try { event.reply('profile-message-progress', { id: jobId, profile: currentProfile, number: patient.phone, status: 'failed', sent, failed, total: eligiblePatients.length, patient: patient.name || 'Unknown', error: `Invalid JID (${reason})` }); } catch (e) {}
          try { const jsonLogPath = getJsonLogsPath(); const logs = readJsonLog(jsonLogPath); const jobIndex = logs.findIndex(job => job.id === jobId); if (jobIndex !== -1) { logs[jobIndex].record_sent = sent; logs[jobIndex].record_failed = failed; writeJsonLog(jsonLogPath, logs); event.reply("reports-updated", logs); } } catch (e) { console.warn('Failed to update job progress in JSON file:', e.message); }
          continue;
        }
        console.log(`✅ Phone number formatted: ${patient.phone} → ${jid}`);

        console.log(`📤 Sending message to ${patient.name} via ${currentProfile}...`);
        
        // Pre-send profile validation - ensure client is ready
        console.log(`🔍 Validating profile readiness: ${currentProfile}`);
        const profileValidation = await validateProfileReadiness(currentProfile, client);
        if (!profileValidation.ready) {
          throw new Error(`Profile validation failed: ${profileValidation.error}`);
        }
        console.log(`✅ Profile ${currentProfile} is ready and connected`);
        
        // Send message based on template type
        if (templateData.type === 'Media' && templateData.media_path) {
          // Send media with caption
          const fs = require('fs');
          const path = require('path');
          
          let mediaPath = templateData.media_path;
          
          // If media_path is just a filename, construct full path
          if (!path.isAbsolute(mediaPath)) {
            const userDataPath = getUserDataDir();
            mediaPath = path.join(userDataPath, 'media', templateData.media_filename || templateData.media_path);
          }
          
          if (fs.existsSync(mediaPath)) {
            // Read media file and send with caption
            const { MessageMedia } = require('whatsapp-web.js');
            const mediaData = MessageMedia.fromFilePath(mediaPath);
            
            if (messageToSend) {
              // Send media with caption
              await sendMessageSafely(client, jid, mediaData, { caption: messageToSend });
              console.log(`✅ Media message sent with caption: ${messageToSend.substring(0, 50)}...`);
            } else {
              // Send media without caption
              await sendMessageSafely(client, jid, mediaData);
              console.log(`✅ Media message sent without caption`);
            }
          } else {
            console.warn(`❌ Media file not found: ${mediaPath}`);
            // Fallback to text message if media not found
            if (messageToSend) {
              await sendMessageSafely(client, jid, messageToSend);
              console.log(`✅ Fallback text message sent (media not found)`);
            } else {
              throw new Error(`Media file not found and no text message available: ${mediaPath}`);
            }
          }
        } else {
          // Send text message
          if (!messageToSend) {
            throw new Error('No message content to send for text template');
          }
          await sendMessageSafely(client, jid, messageToSend);
          console.log(`✅ Text message sent: ${messageToSend.substring(0, 50)}...`);
        }
        
        console.log(`✅ Message sent successfully!`);
        
        // Update patient record with the template that was actually sent
        const timestamp = getLocalISOString();
        try {
          updateLastStmt.run(timestamp, templateInfo.templateName, templateInfo.stepNumber, timestamp, patient.unique_id || '', patient.phone || '');
          console.log(`✅ Database updated: last_template="${templateInfo.templateName}", last_schedule_days=${templateInfo.stepNumber}`);
        } catch (e) { 
          console.error('⚠️ Database update failed (message was sent though):', e.message); 
        }

        // Log successful message to message_logs table
        try {
          const messageId = 'msg_' + getLocalISOString().replace(/[-:.TZ]/g, '') + '_' + Math.random().toString(36).substr(2, 6);
          
          // Prepare message content for logging
          let logMessage = messageToSend || '';
          if (templateData.type === 'Media' && templateData.media_path) {
            logMessage = `[MEDIA: ${templateData.media_filename || 'media file'}]${messageToSend ? ' - ' + messageToSend : ''}`;
          }
          
          insertMessageLog.run({
            id: messageId,
            job_id: jobId,
            unique_id: patient.unique_id || null,
            name: patient.name || null,
            phone: patient.phone || null,
            profile: currentProfile,
            template: templateInfo.templateName,
            message: logMessage.substring(0, 500), // Limit message length
            status: 'Sent',
            sent_at: timestamp,
            error: null,
            media_path: templateData?.media_path || null,
            media_filename: templateData?.media_filename || null
          });
          console.log(`✅ Message logged to message_logs table: ${messageId}`);
        } catch (e) {
          console.warn('⚠️ Failed to log message to message_logs table:', e.message);
        }

        // Add successful message to Excel logs collection
        let logMessage = messageToSend || '';
        if (templateData.type === 'Media' && templateData.media_path) {
          logMessage = `[MEDIA: ${templateData.media_filename || 'media file'}]${messageToSend ? ' - ' + messageToSend : ''}`;
        }
        allLogs.push({
          Profile: currentProfile,
          Phone: patient.phone,
          Status: 'Sent',
          Timestamp: timestamp,
          Message: logMessage.substring(0, 500),
          Template: templateInfo.templateName,
          CustomerName: patient.name || '',
          Error: ''
        });

        // Update counters
        sent++;
        dailyCounts[currentProfile]++;
        profileMessageCounts[currentProfile]++;
        profileSentWindow[currentProfile].push(currentTime);
        globalSentWindow.push(currentTime); // Add to global rate limit window
        
        // ✅ Send progress update after successful message
        try {
          event.reply('profile-message-progress', { 
            id: jobId, 
            profile: currentProfile, 
            number: patient.phone, 
            status: 'sent', 
            sent, 
            failed, 
            total: eligiblePatients.length,
            patient: patient.name || 'Unknown',
            template: templateInfo.templateName
          });
        } catch (e) {
          console.warn('Failed to send progress update:', e.message);
        }
        
        // ✅ Update job data in JSON file for real-time UI updates
        try {
          const jsonLogPath = getJsonLogsPath();
          const logs = readJsonLog(jsonLogPath);
          const jobIndex = logs.findIndex(job => job.id === jobId);
          if (jobIndex !== -1) {
            logs[jobIndex].record_sent = sent;
            logs[jobIndex].record_failed = failed;
            writeJsonLog(jsonLogPath, logs);
            
            // Send updated reports to refresh UI
            try {
              event.reply("reports-updated", logs);
            } catch (e) {
              console.warn("Failed to send reports update:", e.message);
            }
          }
        } catch (e) {
          console.warn('⚠️ Failed to update job progress in JSON file:', e.message);
        }
        
        // Update session health
        try { updateSessionHealth(currentProfile, 'messagesSent', profileMessageCounts[currentProfile]); } catch (e) {}
        try { updateSessionHealth(currentProfile, 'lastMessageTime', currentTime); } catch (e) {}
        try { updateSessionHealth(currentProfile, 'consecutiveFailures', 0); } catch (e) {}
        
        // Update daily stats
        try { incStat(currentProfile); } catch (e) {}

        console.log(`✅ Sent to ${patient.name} (${patient.phone}) via ${currentProfile} using template: ${templateInfo.templateName} (Step ${templateInfo.stepNumber})`);

        // Send progress update
        try {
          event.reply('message-progress', { 
            id: jobId, 
            profile: currentProfile, 
            number: patient.phone,
            name: patient.name, 
            template: templateInfo.templateName,
            step: templateInfo.stepNumber,
            status: 'sent', 
            sent, 
            failed, 
            total: eligiblePatients.length 
          });
        } catch (e) {}

        // Also send profile-specific progress for compatibility
        try {
          event.reply('profile-message-progress', { 
            jobId, 
            profileName: currentProfile, 
            CustomerName: patient.name, 
            phone: patient.phone, 
            templateUsed: templateInfo.templateName,
            stepNumber: templateInfo.stepNumber,
            status: 'sent', 
            sent, 
            failed, 
            total: eligiblePatients.length 
          });
        } catch (e) {}

        // Optimized delay system - use getHumanLikeDelay for faster, safer delays
        const delayTime = getHumanLikeDelay();
        console.log(`⏱️ Waiting ${delayTime}ms before next message...`);
        await new Promise(resolve => setTimeout(resolve, delayTime));

      } catch (error) {
        failed++;
        const currentProfile = readyProfiles[i % readyProfiles.length];
        console.error(`\n❌ CRITICAL ERROR for patient ${patient.name} (${patient.phone}) via ${currentProfile}:`);
        console.error(`   - Error type: ${error.name || 'Unknown'}`);
        console.error(`   - Error message: ${error.message}`);
        console.error(`   - Error stack: ${error.stack}`);
        console.error(`   - Patient data:`, {
          name: patient.name,
          phone: patient.phone,
          unique_id: patient.unique_id,
          last_template: patient.last_template,
          last_schedule_days: patient.last_schedule_days
        });
        console.error(`   - Profile: ${currentProfile}`);
        console.error(`   - Client active: ${!!activeClients[currentProfile]}`);

        
        // Log critical error to message_logs table
        try {
          const messageId = 'msg_' + getLocalISOString().replace(/[-:.TZ]/g, '') + '_' + Math.random().toString(36).substr(2, 6);
          insertMessageLog.run({
            id: messageId,
            job_id: jobId,
            unique_id: patient.unique_id || null,
            name: patient.name || null,
            phone: patient.phone || null,
            profile: currentProfile,
            template: null,
            message: '',
            status: 'Failed',
            sent_at: getLocalISOString(),
            error: `${error.name || 'Error'}: ${error.message}`,
            media_path: null,
            media_filename: null
          });
          console.log(`✅ Critical error logged to message_logs table: ${messageId}`);
        } catch (logError) {
          console.warn('⚠️ Failed to log critical error to message_logs table:', logError.message);
        }
        
        // Add critical error to Excel logs collection
        allLogs.push({
          Profile: currentProfile,
          Phone: patient.phone,
          Status: 'Failed',
          Timestamp: getLocalISOString(),
          Message: '',
          Template: null,
          CustomerName: patient.name || '',
          Error: `${error.name || 'Error'}: ${error.message}`
        });
        
        // Update failure count for profile
        try {
          const currentFailures = getSessionHealth(currentProfile)?.consecutiveFailures || 0;
          updateSessionHealth(currentProfile, 'consecutiveFailures', currentFailures + 1);
          console.log(`   - Updated failure count for ${currentProfile}: ${currentFailures + 1}`);
        } catch (e) {
          console.error(`   - Failed to update session health:`, e.message);
        }
        
        failed++;
        
        // ✅ Send progress update after failure
        try {
          event.reply('profile-message-progress', { 
            id: jobId, 
            profile: currentProfile, 
            number: patient.phone, 
            status: 'failed', 
            sent, 
            failed, 
            total: eligiblePatients.length,
            patient: patient.name || 'Unknown',
            error: `${error.name || 'Error'}: ${error.message}`
          });
        } catch (e) {
          console.warn('Failed to send progress update for failure:', e.message);
        }
        
        // ✅ Update job data in JSON file for real-time UI updates
        try {
          const jsonLogPath = getJsonLogsPath();
          const logs = readJsonLog(jsonLogPath);
          const jobIndex = logs.findIndex(job => job.id === jobId);
          if (jobIndex !== -1) {
            logs[jobIndex].record_sent = sent;
            logs[jobIndex].record_failed = failed;
            writeJsonLog(jsonLogPath, logs);
            
            // Send updated reports to refresh UI
            try {
              event.reply("reports-updated", logs);
            } catch (e) {
              console.warn("Failed to send reports update:", e.message);
            }
          }
        } catch (e) {
          console.warn('⚠️ Failed to update job progress in JSON file:', e.message);
        }
      }
    }

    // Send completion notification with both event types for compatibility
    try {
      event.reply('message-finished', { 
        id: jobId, 
        profiles: validProfiles, 
        sent, 
        failed, 
        total: eligiblePatients.length,
        dailyCounts 
      });
      
      event.reply('profile-message-finished', { 
        jobId, 
        profiles: validProfiles, 
        sent, 
        failed, 
        total: eligiblePatients.length,
        dailyCounts 
      });
    } catch (e) {}

    // Clean up job controller
    delete jobControllers[jobId];
    console.log(`✅ Job controller cleaned up: ${jobId}`);

    // Update job completion in JSON log
    try {
      const jsonLogPath = getJsonLogsPath();
      const logs = readJsonLog(jsonLogPath);
      const jobIndex = logs.findIndex(job => job.id === jobId);
      if (jobIndex !== -1) {
        logs[jobIndex] = { 
          ...logs[jobIndex], 
          record_sent: sent, 
          record_failed: failed, 
          status: 'completed' 
        };
        writeJsonLog(jsonLogPath, logs);
        console.log(`✅ Job completion logged to JSON file: ${sent} sent, ${failed} failed`);
      }
    } catch (e) {
      console.warn('⚠️ Failed to update job completion in JSON file:', e.message);
    }

    // Generate Excel log file
    try {
      const logDir = path.join(getUserDataDir(), 'logs');
      try { fs.mkdirSync(logDir, { recursive: true }); } catch (e) {}
      const ts = getLocalISOString().replace(/[:.]/g, '-');
      const fname = `log-profile-sequence-${ts}.xlsx`;
      const fpath = path.join(logDir, fname);
      const worksheet = XLSX.utils.json_to_sheet(allLogs);
      const logWorkbook = XLSX.utils.book_new();
      XLSX.utils.book_append_sheet(logWorkbook, worksheet, 'Message Log');
      XLSX.writeFile(logWorkbook, fpath);
      
      // Update job with Excel file path
      try {
        const jsonLogPath = getJsonLogsPath();
        const logs = readJsonLog(jsonLogPath);
        const jobIndex = logs.findIndex(job => job.id === jobId);
        if (jobIndex !== -1) {
          logs[jobIndex].path = fpath;
          writeJsonLog(jsonLogPath, logs);
        }
      } catch (e) {
        console.warn('⚠️ Failed to update job with Excel file path:', e.message);
      }
      
      console.log(`✅ Excel log file created: ${fpath}`);
      
      // Send completion notification with file path - both event types
      try {
        event.reply('message-finished', { 
          id: jobId, 
          profiles: validProfiles, 
          sent, 
          failed, 
          total: eligiblePatients.length,
          dailyCounts,
          path: fpath
        });
        
        event.reply('profile-message-finished', { 
          jobId, 
          profiles: validProfiles, 
          sent, 
          failed, 
          total: eligiblePatients.length,
          dailyCounts,
          path: fpath
        });
      } catch (e) {}
      
    } catch (e) {
      console.warn('⚠️ Failed to create Excel log file:', e.message);
      // Still send completion notification without path - both event types
      try {
        event.reply('message-finished', { 
          id: jobId, 
          profiles: validProfiles, 
          sent, 
          failed, 
          total: eligiblePatients.length,
          dailyCounts 
        });
        
        event.reply('profile-message-finished', { 
          jobId, 
          profiles: validProfiles, 
          sent, 
          failed, 
          total: eligiblePatients.length,
          dailyCounts 
        });
      } catch (e) {}
    }

    console.log(`Multi-profile messaging completed: ${sent} sent, ${failed} failed across ${validProfiles.length} profiles`);
    console.log(`Final daily counts:`, dailyCounts);

  } catch (error) {
    console.error('send-profile-messages-with-sequence error:', error);
    try {
      event.reply('profile-message-response', { 
        success: false, 
        error: error.message 
      });
    } catch (e) {}
  }
});

// Helper function to render template for patient (duplicate from above for this handler)
// Uses the same rules as other handlers: {{key}} exact-match only; {key} allowed only for a whitelist.
function renderTemplateForPatient(tmpl, patient) {
  if (!tmpl) return '';
  const p = patient || {};
  const normalized = Object.keys(p).reduce((acc, k) => {
    acc[k] = p[k];
    acc[k.toLowerCase()] = p[k];
    acc[k.replace(/[_\s]/g,'').toLowerCase()] = p[k];
    return acc;
  }, {});

  const nameVal = normalized['name'] || normalized['fullname'] || normalized['full name'] || p.name || p.Name || '';
  const firstName = (String(nameVal || '').trim().split(/\s+/)[0]) || '';

  return String(tmpl).replace(/(\{\{\s*([^}]+?)\s*\}\}|\{\s*([^}]+?)\s*\})/g, (m, _all, g2, g3) => {
    const isDouble = !!g2;
    const rawKey = String((g2 || g3) || '').trim();
    const k = rawKey;
    const kl = k.toLowerCase();

    if (isDouble) {
      // exact match only (case-insensitive)
      const keys = Object.keys(p || {});
      for (const candidate of keys) {
        if (String(candidate || '').trim().toLowerCase() === rawKey.toLowerCase()) {
          return p[candidate] == null ? '' : String(p[candidate]);
        }
      }
      return m; // leave placeholder as-is if exact not found
    }

    // Single-brace: restrict to allowed keys only
    const allowed = new Set(['name','phone','var1','var2','var3','var4','var5','var6']);
    const normKey = k.replace(/[_\s]/g,'').toLowerCase();
    if (allowed.has(normKey)) {
      if (Object.prototype.hasOwnProperty.call(normalized, normKey)) return normalized[normKey] == null ? '' : String(normalized[normKey]);
      if (Object.prototype.hasOwnProperty.call(normalized, k)) return normalized[k] == null ? '' : String(normalized[k]);
      if (Object.prototype.hasOwnProperty.call(normalized, kl)) return normalized[kl] == null ? '' : String(normalized[kl]);
      if (k.includes('.')) { const parts = k.split('.'); let cur = p; for (const part of parts) { if (cur && Object.prototype.hasOwnProperty.call(cur, part)) cur = cur[part]; else { cur = ''; break; } } return cur == null ? '' : String(cur); }
      if (Object.prototype.hasOwnProperty.call(p, k)) return p[k] == null ? '' : String(p[k]);
      if (Object.prototype.hasOwnProperty.call(p, kl)) return p[kl] == null ? '' : String(p[kl]);
      return '';
    }

    return '';
  });
}
//console.log('✅ Parallel processing with safety features enabled');

// Excel export handler for single profile
ipcMain.handle('export-to-excel', async (event, { data, fileName, sheetName }) => {
  try {
    const path = require('path');
    const os = require('os');
    
    // Create a new workbook
    const wb = XLSX.utils.book_new();
    
    // Convert data array to worksheet
    const ws = XLSX.utils.aoa_to_sheet(data);
    
    // Auto-size columns
    const range = XLSX.utils.decode_range(ws['!ref']);
    const colWidths = [];
    
    for (let C = range.s.c; C <= range.e.c; ++C) {
      let maxWidth = 10; // minimum width
      for (let R = range.s.r; R <= range.e.r; ++R) {
        const cellAddress = XLSX.utils.encode_cell({ r: R, c: C });
        const cell = ws[cellAddress];
        if (cell && cell.v) {
          const cellValueLength = String(cell.v).length;
          maxWidth = Math.max(maxWidth, cellValueLength + 2);
        }
      }
      colWidths.push({ width: Math.min(maxWidth, 50) }); // max width 50
    }
    ws['!cols'] = colWidths;
    
    // Add worksheet to workbook
    XLSX.utils.book_append_sheet(wb, ws, sheetName || 'Data');
    
    // Define export path
    const downloadsPath = path.join(os.homedir(), 'Downloads');
    const filePath = path.join(downloadsPath, `${fileName}.xlsx`);
    
    // Write the file
    XLSX.writeFile(wb, filePath);
    
    console.log(`Excel file exported: ${filePath}`);
    
    return {
      success: true,
      filePath: filePath,
      message: 'Excel file exported successfully'
    };
    
  } catch (error) {
    console.error('Excel export error:', error);
    return {
      success: false,
      error: error.message
    };
  }
});

// Excel export handler for multiple profiles (multi-sheet)
ipcMain.handle('export-multi-sheet-excel', async (event, { sheets, fileName }) => {
  try {
    const path = require('path');
    const os = require('os');
    
    // Create a new workbook
    const wb = XLSX.utils.book_new();
    
    // Add each profile as a separate sheet
    sheets.forEach(sheet => {
      // Convert data array to worksheet
      const ws = XLSX.utils.aoa_to_sheet(sheet.data);
      
      // Auto-size columns
      if (ws['!ref']) {
        const range = XLSX.utils.decode_range(ws['!ref']);
        const colWidths = [];
        
        for (let C = range.s.c; C <= range.e.c; ++C) {
          let maxWidth = 10; // minimum width
          for (let R = range.s.r; R <= range.e.r; ++R) {
            const cellAddress = XLSX.utils.encode_cell({ r: R, c: C });
            const cell = ws[cellAddress];
            if (cell && cell.v) {
              const cellValueLength = String(cell.v).length;
              maxWidth = Math.max(maxWidth, cellValueLength + 2);
            }
          }
          colWidths.push({ width: Math.min(maxWidth, 50) }); // max width 50
        }
        ws['!cols'] = colWidths;
      }
      
      // Add worksheet to workbook (sheet name limited to 31 chars for Excel compatibility)
      const sheetName = sheet.name.length > 31 ? sheet.name.substring(0, 31) : sheet.name;
      XLSX.utils.book_append_sheet(wb, ws, sheetName);
    });
    
    // Define export path
    const downloadsPath = path.join(os.homedir(), 'Downloads');
    const filePath = path.join(downloadsPath, `${fileName}.xlsx`);
    
    // Write the file
    XLSX.writeFile(wb, filePath);
    
    console.log(`Multi-sheet Excel file exported: ${filePath}`);
    
    return {
      success: true,
      filePath: filePath,
      message: `Excel file with ${sheets.length} sheets exported successfully`
    };
    
  } catch (error) {
    console.error('Multi-sheet Excel export error:', error);
    return {
      success: false,
      error: error.message
    };
  }
});

// File selection dialog handler
ipcMain.handle('select-media-file', async (event) => {
  try {
    // Use the add template window as parent for proper z-index
    const parentWindow = addTemplateWin || templateWin || mainWin;
    
    const { canceled, filePaths } = await dialog.showOpenDialog(parentWindow, {
      title: 'Select Media File',
      filters: [
        { name: 'Images', extensions: ['jpg', 'jpeg', 'png', 'gif', 'bmp', 'webp'] },
        { name: 'Videos', extensions: ['mp4', 'avi', 'mov', 'wmv', 'flv', 'webm'] },
        { name: 'Documents', extensions: ['pdf', 'doc', 'docx', 'txt'] },
        { name: 'All Files', extensions: ['*'] }
      ],
      properties: ['openFile']
    });

    if (canceled || filePaths.length === 0) {
      return { success: false, canceled: true };
    }

    const filePath = filePaths[0];
    const fs = require('fs');
    const path = require('path');
    
    // Read file and convert to base64
    const fileBuffer = fs.readFileSync(filePath);
    const fileData = fileBuffer.toString('base64');
    const fileName = path.basename(filePath);
    const fileSize = fs.statSync(filePath).size;
    
    return {
      success: true,
      file: {
        name: fileName,
        data: fileData,
        size: fileSize,
        path: filePath
      }
    };
  } catch (error) {
    console.error('File selection error:', error);
    return { success: false, error: error.message };
  }
});

// Media file upload handler
ipcMain.handle('upload-media-file', async (event, fileData) => {
  try {
    const path = require('path');
    const fs = require('fs');
    // Create userdata/media folder if it doesn't exist
    const userDataPath = getUserDataDir();
    const mediaFolderPath = path.join(userDataPath, 'media');
    
    if (!fs.existsSync(userDataPath)) {
      fs.mkdirSync(userDataPath, { recursive: true });
    }
    if (!fs.existsSync(mediaFolderPath)) {
      fs.mkdirSync(mediaFolderPath, { recursive: true });
    }
    
    // Generate unique filename
    const timestamp = Date.now();
    const originalName = fileData.name;
    const extension = path.extname(originalName);
    const baseName = path.basename(originalName, extension);
    const uniqueFileName = `${timestamp}_${baseName}${extension}`;
    const filePath = path.join(mediaFolderPath, uniqueFileName);
    
    // Save file
    const buffer = Buffer.from(fileData.data, 'base64');
    fs.writeFileSync(filePath, buffer);
    
    console.log(`Media file uploaded: ${filePath}`);
    
    return {
      success: true,
      filePath: filePath,
      fileName: uniqueFileName,
      originalName: originalName,
      message: 'File uploaded successfully'
    };
    
  } catch (error) {
    console.error('Media upload error:', error);
    return {
      success: false,
      error: error.message
    };
  }
});

// Enhanced template save handler with media support
ipcMain.handle('save-template-with-media', async (event, templateData) => {
  try {
    console.log('Received template data for saving:', templateData);
    const timestamp = getLocalISOString();
    
    // Prepare template data
    const dbData = {
      name: templateData.name,
      type: templateData.type || 'Text',
      message: templateData.message || '',
      media_path: templateData.media_path || null,
      media_filename: templateData.media_filename || null,
      sendOption: templateData.sendOption || 'instant',
      afterDays: templateData.afterDays || 0,
      is_active: 1,
      is_delete: 0,
      created: timestamp,
      modified: timestamp
    };
    
    console.log('Database data being saved:', dbData);
    
    // Save to database
    upsertTemplate.run(dbData);
    
    console.log(`Template saved: ${templateData.name} (Type: ${templateData.type})`);
    
    return {
      success: true,
      message: 'Template saved successfully'
    };
    
  } catch (error) {
    console.error('Template save error:', error);
    return {
      success: false,
      error: error.message
    };
  }
});

// Get media file preview handler
ipcMain.handle('get-media-preview', async (event, filePath) => {
  try {
    const fs = require('fs');
    const path = require('path');
    
    console.log('get-media-preview requested for:', filePath);
    console.log('File exists check:', fs.existsSync(filePath));
    
    if (!fs.existsSync(filePath)) {
      // Try to find the file in the media directory if it's just a filename
      const fileName = path.basename(filePath);
      const mediaFolderPath = path.join(getUserDataDir(), 'media');
      const possiblePath = path.join(mediaFolderPath, fileName);
      
      console.log('Trying alternative path:', possiblePath);
      console.log('Alternative path exists:', fs.existsSync(possiblePath));
      
      if (fs.existsSync(possiblePath)) {
        // Update the path to the correct location
        filePath = possiblePath;
        console.log('Using alternative path:', filePath);
      } else {
        return {
          success: false,
          error: `File not found: ${filePath}. Also checked: ${possiblePath}`
        };
      }
    }
    
    const stats = fs.statSync(filePath);
    const extension = path.extname(filePath).toLowerCase();
    const isImage = ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp'].includes(extension);
    const isVideo = ['.mp4', '.avi', '.mov', '.wmv', '.flv', '.webm'].includes(extension);
    const isAudio = ['.mp3', '.wav', '.ogg', '.m4a', '.aac'].includes(extension);
    
    let mediaType = 'document';
    if (isImage) mediaType = 'image';
    else if (isVideo) mediaType = 'video';
    else if (isAudio) mediaType = 'audio';
    
    // For images, return base64 data for preview
    let previewData = null;
    if (isImage && stats.size < 5 * 1024 * 1024) { // Max 5MB for preview
      const fileBuffer = fs.readFileSync(filePath);
      previewData = `data:image/${extension.substring(1)};base64,${fileBuffer.toString('base64')}`;
    }
    
    return {
      success: true,
      mediaType: mediaType,
      fileName: path.basename(filePath),
      fileSize: stats.size,
      previewData: previewData,
      filePath: filePath
    };
    
  } catch (error) {
    console.error('Media preview error:', error);
    return {
      success: false,
      error: error.message
    };
  }
});

// Debug handler to list media files and check database
ipcMain.handle('debug-media-files', async () => {
  try {
    const fs = require('fs');
    const path = require('path');
    
    const mediaFolderPath = path.join(getUserDataDir(), 'media');
    console.log('Media folder path:', mediaFolderPath);
    
    if (!fs.existsSync(mediaFolderPath)) {
      return {
        success: false,
        error: 'Media folder does not exist',
        mediaPath: mediaFolderPath
      };
    }
    
    const files = fs.readdirSync(mediaFolderPath);
    console.log('Media files found:', files);
    
    // Also check templates in database
    let dbTemplates = [];
    try {
      if (db) {
        dbTemplates = db.prepare("SELECT name, type, media_path, media_filename FROM templates WHERE type = 'Media' AND is_delete = 0").all();
      }
    } catch (e) {
      console.error('Error reading templates from DB:', e.message);
    }
    
    return {
      success: true,
      mediaPath: mediaFolderPath,
      files: files,
      mediaTemplates: dbTemplates
    };
    
  } catch (error) {
    console.error('Debug media files error:', error);
    return {
      success: false,
      error: error.message
    };
  }
});

// Bulk update patient profiles by phone (from Excel upload)
// Accepts: { name, data } where data is an array of bytes
ipcMain.handle('bulk-update-patient-profiles', async (event, payload) => {
  try {
    if (!payload || !payload.data || !payload.name) {
      return { success: false, error: 'No file data provided' };
    }
    const buffer = Buffer.from(payload.data);
    // Save file to userdata/bulkprofilechange
    const userDataDir = getUserDataDir();
    const saveDir = path.join(userDataDir, 'bulkprofilechange');
    if (!fs.existsSync(saveDir)) {
      fs.mkdirSync(saveDir, { recursive: true });
    }
    // Save with timestamp to avoid overwrite
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const savePath = path.join(saveDir, `${timestamp}_${payload.name}`);
    fs.writeFileSync(savePath, buffer);

    let rows = [];
    if (payload.name.toLowerCase().endsWith('.csv')) {
      // Parse CSV
      const text = buffer.toString('utf8');
      const lines = text.split(/\r?\n/).filter(line => line.trim());
      rows = lines.map(line => {
        const cells = [];
        let current = '';
        let inQuotes = false;
        for (let i = 0; i < line.length; i++) {
          const char = line[i];
          const nextChar = line[i + 1];
          if (char === '"') {
            if (inQuotes && nextChar === '"') { current += '"'; i++; }
            else { inQuotes = !inQuotes; }
          } else if (char === ',' && !inQuotes) { cells.push(current.trim()); current = ''; }
          else { current += char; }
        }
        cells.push(current.trim());
        return cells;
      });
    } else {
      // Excel: use Node.js XLSX
      const workbook = XLSX.read(buffer, { type: 'buffer' });
      const firstSheet = workbook.Sheets[workbook.SheetNames[0]];
      rows = XLSX.utils.sheet_to_json(firstSheet, { header: 1 });
    }
    if (!rows || rows.length < 2) return { success: false, error: 'No data found in file.' };
    // Find columns
    const headers = rows[0].map(h => (h || '').toString().toLowerCase().trim());
    const phoneIdx = headers.indexOf('phone');
    const profileIdx = headers.indexOf('profile');
    if (phoneIdx === -1 || profileIdx === -1) {
      return { success: false, error: 'Both "phone" and "profile" columns are mandatory.' };
    }
    // Prepare update data
    const updates = [];
    for (let i = 1; i < rows.length; i++) {
      const row = rows[i];
      if (!row) continue;
      const phone = row[phoneIdx] ? row[phoneIdx].toString().trim() : '';
      let profile = row[profileIdx] ? row[profileIdx].toString() : '';
      profile = profile.trim();
      // Skip if profile is null/blank/whitespace
      if (!phone || !profile) continue;
      updates.push({ phone, profile });
    }
    if (updates.length === 0) {
      return { success: false, error: 'No valid rows with both phone and profile found.' };
    }
    // Bulk update
    const now = getLocalISOString();
    const stmt = db.prepare('UPDATE patients SET profile = ? WHERE phone = ?');
    let count = 0;
    const tx = db.transaction((rows) => {
      for (const row of rows) {
        try {
          const r = stmt.run(row.profile, row.phone);
          count += (r && r.changes) ? r.changes : 0;
        } catch (e) {
          console.warn('bulk-update-patient-profiles row failed for', row.phone, e && e.message ? e.message : e);
        }
      }
    });
    tx(updates);
    return { success: true, count, saved: savePath };
  } catch (e) {
    console.error('bulk-update-patient-profiles failed:', e && e.message ? e.message : e);
    return { success: false, error: e && e.message ? e.message : String(e) };
  }
});


