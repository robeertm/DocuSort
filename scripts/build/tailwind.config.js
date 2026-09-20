/** Tailwind config for the precompiled DocuSort stylesheet.
 *  Used by scripts/build/build-css.sh; not consumed at runtime. */
module.exports = {
  content: [
    "../../docusort/web/templates/**/*.html",
  ],
  theme: {
    extend: {
      colors: {
        // Every ink tone is a CSS variable so the light theme flips the
        // whole palette by redefining the variables (see input.css) —
        // no per-class overrides, and new classes work in both modes.
        ink: {
          50:  'rgb(var(--ink-50) / <alpha-value>)',  100: 'rgb(var(--ink-100) / <alpha-value>)',
          200: 'rgb(var(--ink-200) / <alpha-value>)', 300: 'rgb(var(--ink-300) / <alpha-value>)',
          400: 'rgb(var(--ink-400) / <alpha-value>)', 500: 'rgb(var(--ink-500) / <alpha-value>)',
          600: 'rgb(var(--ink-600) / <alpha-value>)', 700: 'rgb(var(--ink-700) / <alpha-value>)',
          800: 'rgb(var(--ink-800) / <alpha-value>)', 900: 'rgb(var(--ink-900) / <alpha-value>)',
          950: 'rgb(var(--ink-950) / <alpha-value>)',
        },
      },
    },
  },
};
