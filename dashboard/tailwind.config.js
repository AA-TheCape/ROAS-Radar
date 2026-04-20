/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{ts,tsx}'],
  theme: {
    screens: {
      xs: '480px',
      sm: '640px',
      md: '768px',
      lg: '1040px',
      xl: '1280px',
      '2xl': '1440px',
      '3xl': '1680px'
    },
    extend: {
      maxWidth: {
        shell: '92rem'
      },
      colors: {
        canvas: '#f4f7f4',
        'canvas-tint': '#edf5f2',
        surface: '#ffffff',
        'surface-alt': '#fbf7f2',
        line: '#d7dfdb',
        'line-strong': '#b8c5c0',
        ink: {
          DEFAULT: '#17212b',
          soft: '#314051',
          muted: '#627180',
          inverse: '#f7f9f8'
        },
        brand: {
          DEFAULT: '#cb6332',
          strong: '#a94b1f',
          soft: '#f6dfd3'
        },
        teal: {
          DEFAULT: '#1f7a74',
          strong: '#145a55',
          soft: '#dcefed'
        },
        success: {
          DEFAULT: '#2f7d60',
          soft: '#dcefe6'
        },
        warning: {
          DEFAULT: '#b0711f',
          soft: '#f8ead7'
        },
        danger: {
          DEFAULT: '#b64c46',
          soft: '#f6dddb'
        }
      },
      backgroundImage: {
        'shell-glow':
          'radial-gradient(circle at top left, rgba(203, 99, 50, 0.18), transparent 28%), radial-gradient(circle at top right, rgba(31, 122, 116, 0.18), transparent 24%), linear-gradient(180deg, #f7f9f6 0%, #f4f7f4 55%, #edf5f2 100%)',
        'panel-tint': 'linear-gradient(180deg, rgba(255, 255, 255, 0.82), rgba(244, 233, 226, 0.8))'
      },
      spacing: {
        gutter: '1rem',
        section: '1.5rem',
        panel: '1.75rem',
        card: '2.25rem',
        'section-lg': '3rem',
        hero: '4.5rem'
      },
      borderRadius: {
        card: '1.5rem',
        panel: '1.75rem',
        shell: '2rem',
        pill: '999px'
      },
      borderWidth: {
        strong: '1.5px',
        heavy: '2px'
      },
      boxShadow: {
        panel: '0 20px 45px rgba(23, 33, 43, 0.08)',
        lift: '0 28px 68px rgba(23, 33, 43, 0.14)',
        'inset-soft': 'inset 0 1px 0 rgba(255, 255, 255, 0.8)'
      },
      fontFamily: {
        display: ['"Space Grotesk"', '"IBM Plex Sans"', 'ui-sans-serif', 'system-ui', 'sans-serif'],
        body: ['"IBM Plex Sans"', '"Segoe UI"', 'ui-sans-serif', 'system-ui', 'sans-serif'],
        mono: ['"IBM Plex Mono"', '"SFMono-Regular"', 'ui-monospace', 'monospace']
      },
      fontSize: {
        hero: ['clamp(3rem, 7vw, 5.5rem)', { lineHeight: '0.92', letterSpacing: '-0.05em', fontWeight: '700' }],
        display: ['clamp(2rem, 4vw, 3rem)', { lineHeight: '1', letterSpacing: '-0.04em', fontWeight: '700' }],
        title: ['1.375rem', { lineHeight: '1.1', letterSpacing: '-0.03em', fontWeight: '700' }],
        lead: ['1.0625rem', { lineHeight: '1.7', letterSpacing: '-0.01em', fontWeight: '400' }],
        body: ['0.9375rem', { lineHeight: '1.65', letterSpacing: '-0.01em', fontWeight: '400' }],
        caption: ['0.75rem', { lineHeight: '1.25rem', letterSpacing: '0.14em', fontWeight: '600' }]
      },
      keyframes: {
        'fade-up': {
          '0%': { opacity: '0', transform: 'translateY(10px)' },
          '100%': { opacity: '1', transform: 'translateY(0)' }
        }
      },
      animation: {
        'fade-up': 'fade-up 280ms ease-out'
      }
    }
  },
  plugins: []
};
