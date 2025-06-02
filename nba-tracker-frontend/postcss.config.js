/**
 * postcss.config.js – Tailwind 3 setup
 */
module.exports = {
  plugins: {
    tailwindcss: {},   // 👈 don't use '@tailwindcss/postcss' here
    autoprefixer: {},
  },
};