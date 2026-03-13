/** @type {import('tailwindcss').Config} */
module.exports = {
  content: ["./src/**/*.{js,ts,jsx,tsx,mdx}"],
  theme: {
    extend: {
      colors: {
        navy: {
          50: "#f0f4fa",
          100: "#dce6f4",
          600: "#1e3a6e",
          700: "#172d57",
          800: "#112040",
          900: "#0b1529",
        },
        gold: {
          400: "#f5c842",
          500: "#e6b800",
          600: "#c9a000",
        },
      },
    },
  },
  plugins: [],
};
