# Message Types Support in WhatsApp Multi-Client

## Overview
The `send-message-proc` function now supports comprehensive message type handling for different scenarios including text, media, and template-based messaging.

## Supported Message Types

### 1. **Text Messages** (`messageType: "text"`)
- **Use Case**: Pure text messages only
- **Template Support**: Yes - uses templates with type "Text"
- **Behavior**: 
  - Sends text message with placeholder replacement
  - Supports all template placeholders like `{name}`, `{phone}`, etc.
  - Automatically validates template type matches message type

### 2. **Image Messages** (`messageType: "image"`)
- **Use Case**: Media files without any text
- **Template Support**: No - uses uploaded image file
- **Behavior**:
  - Sends only the image/media file
  - No caption or text included
  - Requires image file to be uploaded

### 3. **Template Media Messages** (`messageType: "template-media"`)
- **Use Case**: Templates configured as "Media" type in database
- **Template Support**: Yes - uses templates with type "Media"
- **Behavior**:
  - **Media Only**: If template has media_path but no message text
  - **Media + Caption**: If template has both media_path and message text
  - **Fallback to Text**: If media file not found but template has message text
  - Automatically switches to this mode when text template is actually Media type

### 4. **Image with Text** (`messageType: "textWithImage"` or `messageType: "imageWithText"`)
- **Use Case**: Uploaded image with custom text caption
- **Template Support**: Partial - can use template placeholders in caption
- **Behavior**:
  - Sends uploaded image with rendered text caption
  - Supports placeholder replacement in caption text
  - Requires both image file and caption text

## Template Type Auto-Detection

The system now automatically detects template types and adjusts message handling:

```javascript
// Example: User selects messageType "text" but template is actually "Media"
if (templateData.type === 'Media') {
  messageType = 'template-media'; // Auto-switch to media mode
  templateText = templateData.message || ''; // Caption text
}
```

## Template Scenarios Supported

### Scenario 1: Text-Only Template
```
Template Type: "Text"
Template Message: "Hello {name}, your appointment is confirmed!"
Template Media: (none)
Result: Sends text message with placeholders replaced
```

### Scenario 2: Media-Only Template  
```
Template Type: "Media"
Template Message: (empty or null)
Template Media: "welcome_image.jpg"
Result: Sends image without caption
```

### Scenario 3: Media + Caption Template
```
Template Type: "Media"
Template Message: "Hello {name}, welcome to our clinic!"
Template Media: "welcome_image.jpg" 
Result: Sends image with personalized caption
```

### Scenario 4: Media Template with Missing File
```
Template Type: "Media"
Template Message: "Hello {name}, welcome!"
Template Media: "missing_file.jpg" (file not found)
Result: Falls back to sending text message only
```

## Error Handling & Validation

### Pre-Send Validation
- Checks if template exists in database
- Validates template type matches expected message type
- Verifies media files exist before attempting to send
- Ensures required text content is available

### Fallback Mechanisms
- Media template with missing file → Falls back to text if available
- Invalid template → Shows clear error message
- Empty rendered text → Prevents sending empty messages

### Auto-Correction
- Text messageType with Media template → Auto-switches to template-media
- Missing required content → Clear error messages with suggestions

## Usage Examples

### Frontend Usage
```javascript
// Text message with template
ipcRenderer.send('send-message-proc', {
  messageType: 'text',
  template: 'welcome_template',
  profiles: ['Profile1'],
  excelFile: 'patients.xlsx'
});

// Media template (auto-detected)
ipcRenderer.send('send-message-proc', {
  messageType: 'text', // Will auto-switch to template-media
  template: 'welcome_media_template',
  profiles: ['Profile1'], 
  excelFile: 'patients.xlsx'
});

// Image with custom caption
ipcRenderer.send('send-message-proc', {
  messageType: 'textWithImage',
  textMessage: 'Hello {name}!',
  imageFile: 'uploaded_image.jpg',
  profiles: ['Profile1'],
  excelFile: 'patients.xlsx'
});
```

## Benefits

1. **Intelligent Template Handling**: Auto-detects template type and adjusts sending behavior
2. **Robust Error Handling**: Graceful fallbacks when media files are missing
3. **Flexible Content**: Supports text-only, media-only, or media+caption combinations  
4. **Consistent Placeholders**: All message types support template placeholder replacement
5. **Clear Feedback**: Detailed error messages help users understand issues

## Database Schema

Templates table supports these message types:

```sql
CREATE TABLE templates (
  name TEXT NOT NULL UNIQUE,
  type TEXT DEFAULT 'Text',           -- 'Text' or 'Media'
  message TEXT,                       -- Text content/caption  
  media_path TEXT,                    -- Path to media file
  media_filename TEXT,                -- Original filename
  -- other fields...
);
```

This comprehensive support ensures that all messaging scenarios work correctly whether sending text, media, or template-based content with proper error handling and fallbacks.