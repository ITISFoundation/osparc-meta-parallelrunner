/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      animation: {
        'progress-mac': 'progress-mac 2s linear infinite',
      },
    },
  },
  plugins: [],
}
