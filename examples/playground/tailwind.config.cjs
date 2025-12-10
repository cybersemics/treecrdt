/** @type {import('tailwindcss').Config} */
module.exports = {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      boxShadow: {
        inset: "inset 0 1px 0 rgba(255,255,255,0.05)",
      },
      colors: {
        surface: "#0f172a",
        accent: "#10b981",
      },
    },
  },
  plugins: [],
};
