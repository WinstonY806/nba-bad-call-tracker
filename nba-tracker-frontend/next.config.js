// nba-tracker-frontend/next.config.js
/** @type {import('next').NextConfig} */
module.exports = {
  experimental: {
    allowedDevOrigins: [
      "http://localhost:3000",
      "http://localhost:3001",
      "http://localhost:3002",
      "http://localhost:3003",
    ],
  },
};