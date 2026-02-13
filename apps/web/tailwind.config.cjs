const { heroui } = require("@heroui/theme");

/** @type {import('tailwindcss').Config} */
module.exports = {
  content: ["./index.html", "./src/**/*.{js,ts,jsx,tsx}", "./node_modules/@heroui/theme/dist/**/*.{js,ts,jsx,tsx}"],
  theme: {
    extend: {
      colors: {
        panel: "#0f172a"
      }
    }
  },
  darkMode: "class",
  plugins: [
    heroui({
      themes: {
        dark: {
          colors: {
            background: "#0b1020",
            foreground: "#e6ecff",
            primary: {
              DEFAULT: "#06b6d4",
              foreground: "#022c3a"
            },
            secondary: {
              DEFAULT: "#22c55e",
              foreground: "#052e16"
            }
          }
        }
      }
    })
  ]
};
