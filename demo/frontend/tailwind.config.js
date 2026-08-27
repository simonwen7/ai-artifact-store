/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{js,ts,jsx,tsx}"],
  theme: {
    extend: {
      colors: {
        ink: {
          950: "#0a0b0d",
          900: "#111214",
          850: "#16181c",
          800: "#1c1f24",
          700: "#262a31",
          600: "#343a44",
        },
        mist: {
          50: "#f5f6f7",
          100: "#e8eaed",
          300: "#c4c9d0",
          400: "#9aa3ad",
          500: "#7a8490",
        },
        accent: {
          DEFAULT: "#5ba4c9",
          soft: "#3d7a96",
          muted: "rgba(91, 164, 201, 0.15)",
        },
        healthy: "#3d9a6a",
        warning: "#c9953d",
        failure: "#c94b4b",
      },
      fontFamily: {
        sans: [
          "-apple-system",
          "BlinkMacSystemFont",
          '"Segoe UI"',
          "Roboto",
          "Helvetica",
          "Arial",
          "sans-serif",
        ],
        mono: [
          "ui-monospace",
          "SFMono-Regular",
          "Menlo",
          "Monaco",
          "Consolas",
          '"Liberation Mono"',
          '"Courier New"',
          "monospace",
        ],
      },
      boxShadow: {
        card: "0 1px 0 rgba(255,255,255,0.04), 0 8px 24px rgba(0,0,0,0.35)",
      },
      transitionDuration: {
        DEFAULT: "180ms",
      },
    },
  },
  plugins: [],
};
