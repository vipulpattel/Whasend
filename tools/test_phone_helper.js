// Simple test harness for formatNumber and verifyAndFormatPhone
// This file intentionally copies the logic (do not import main.js to avoid Electron boot)

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
    if (digits.length === 11 && digits.startsWith('91')) {
      out.reason = 'invalid_eleven_91';
      return out;
    }

    let jid;
    try {
      jid = formatNumber(digits);
    } catch (e) {
      out.reason = 'format_error';
      return out;
    }
    out.jid = jid;

    if (client && typeof client.isRegisteredUser === 'function') {
      try {
        const registered = await client.isRegisteredUser(jid);
        out.isWhatsApp = !!registered;
        if (!registered) {
          out.reason = 'not_whatsapp';
          return out;
        }
      } catch (e) {
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

// Mock clients
const clientRegistered = { isRegisteredUser: async (jid) => true };
const clientNotRegistered = { isRegisteredUser: async (jid) => false };
const clientThrows = { isRegisteredUser: async (jid) => { throw new Error('network'); } };

const cases = [
  { label: '10-digit', value: '9876543210' },
  { label: '11-digit starts 91', value: '91987654321' },
  { label: '11-digit not 91', value: '12345678901' },
  { label: '12-digit', value: '911234567890' },
  { label: 'too short', value: '123456789' },
  { label: 'too long', value: '1234567890123' },
  { label: 'with spaces and symbols', value: '+91 98765-43210' }
];

(async () => {
  console.log('Running phone helper tests...');
  for (const c of cases) {
    const res = await verifyAndFormatPhone(clientRegistered, c.value);
    console.log(c.label.padEnd(22), '->', JSON.stringify(res));
  }

  console.log('\nTesting registration negatives...');
  const resNotReg = await verifyAndFormatPhone(clientNotRegistered, '9876543210');
  console.log('not-registered 10-digit ->', JSON.stringify(resNotReg));

  const resThrows = await verifyAndFormatPhone(clientThrows, '9876543210');
  console.log('throws 10-digit ->', JSON.stringify(resThrows));
})();
