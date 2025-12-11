# MLB Analytics Chatbot - Frontend

A modern React + TypeScript frontend for the MLB Analytics Chatbot.

## Features

- 🎨 Modern, responsive UI with Tailwind CSS
- 💬 Real-time chat interface with message history
- 📚 Source citations for each answer
- 🔄 Connection status indicators
- ⚡ Fast development with Vite
- 📱 Mobile-friendly design

## Prerequisites

- Node.js 18+ and npm
- Backend API running on `http://localhost:8000`

## Setup

1. **Install dependencies:**
   ```bash
   cd chatbot/frontend
   npm install
   ```

2. **Start the development server:**
   ```bash
   npm run dev
   ```

3. **Open in browser:**
   The app will be available at `http://localhost:3000`

## Configuration

### API URL

By default, the frontend connects to `http://localhost:8000`. To change this:

1. Create a `.env` file in the `frontend/` directory:
   ```
   VITE_API_URL=http://localhost:8000
   ```

2. Or modify `src/api/client.ts` directly.

## Development

### Available Scripts

- `npm run dev` - Start development server
- `npm run build` - Build for production
- `npm run preview` - Preview production build
- `npm run lint` - Run ESLint

### Project Structure

```
frontend/
├── src/
│   ├── api/
│   │   └── client.ts          # API client for backend
│   ├── components/
│   │   ├── ChatInput.tsx      # Message input component
│   │   ├── ChatMessage.tsx    # Message display component
│   │   └── StatusIndicator.tsx # Connection status
│   ├── types.ts               # TypeScript interfaces
│   ├── App.tsx                # Main app component
│   ├── main.tsx               # Entry point
│   └── index.css              # Global styles
├── index.html
├── package.json
├── tsconfig.json
├── vite.config.ts
└── tailwind.config.js
```

## Building for Production

```bash
npm run build
```

The built files will be in the `dist/` directory. You can serve them with any static file server:

```bash
npm run preview
```

## Integration with Backend

The frontend communicates with the FastAPI backend via:

- `GET /health` - Health check endpoint
- `POST /chat` - Chat endpoint for sending messages

See `src/api/client.ts` for the API client implementation.

## Features in Detail

### Chat Interface
- Real-time message display
- User and assistant messages styled differently
- Source citations shown below assistant messages
- Timestamps for each message
- Auto-scroll to latest message

### Connection Status
- Visual indicator showing API connection status
- Checks health every 30 seconds
- Shows Ollama and Chroma connection status

### Example Queries
- Quick-start buttons with example questions
- One-click to send common queries

### Error Handling
- Graceful error messages
- Retry capability
- Loading states during API calls

## Troubleshooting

### Frontend won't connect to backend
- Make sure the backend is running on `http://localhost:8000`
- Check the browser console for CORS errors
- Verify `VITE_API_URL` in `.env` matches your backend URL

### Build errors
- Make sure all dependencies are installed: `npm install`
- Check Node.js version: `node --version` (should be 18+)
- Clear node_modules and reinstall: `rm -rf node_modules && npm install`

### Styling issues
- Make sure Tailwind CSS is properly configured
- Check that `postcss.config.js` exists
- Verify `tailwind.config.js` content paths are correct

## Next Steps

- [ ] Add authentication if needed
- [ ] Implement rate limiting UI feedback
- [ ] Add dark mode toggle
- [ ] Implement message search/filter
- [ ] Add export chat history feature
- [ ] Improve mobile responsiveness
