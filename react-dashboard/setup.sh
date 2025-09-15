#!/bin/bash

# NSE React Dashboard Setup Script
echo "=== NSE React Dashboard Setup ==="
echo ""

# Check if Node.js is installed
if ! command -v node &> /dev/null; then
    echo "❌ Node.js is not installed"
    echo "📋 Please install Node.js from: https://nodejs.org/"
    echo "   Recommended version: Node.js 18 or later"
    echo ""
    exit 1
fi

# Check if npm is available
if ! command -v npm &> /dev/null; then
    echo "❌ npm is not available"
    echo "📋 Please ensure npm is installed with Node.js"
    echo ""
    exit 1
fi

echo "✅ Node.js version: $(node --version)"
echo "✅ npm version: $(npm --version)"
echo ""

# Install dependencies
echo "📦 Installing dependencies..."
npm install

# Check if installation was successful
if [ $? -eq 0 ]; then
    echo ""
    echo "✅ Dependencies installed successfully!"
    echo ""
    echo "🚀 Available commands:"
    echo "   npm run dev     - Start development server"
    echo "   npm run build   - Build for production"
    echo "   npm run preview - Preview production build"
    echo ""
    echo "📋 To start development:"
    echo "   1. Ensure the Flask API is running on http://localhost:5000"
    echo "   2. Run: npm run dev"
    echo "   3. Open: http://localhost:5173"
    echo ""
else
    echo ""
    echo "❌ Installation failed!"
    echo "📋 Please check the error messages above"
    echo ""
    exit 1
fi