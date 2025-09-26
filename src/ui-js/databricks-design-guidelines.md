# Databricks Design Guidelines

## Design Approach

### Core Design Principles
- **Clarity**: Design should be clear and unambiguous
- **Simplicity**: Remove unnecessary complexity while maintaining functionality
- **Consistency**: Maintain visual and interaction consistency across all touchpoints
- **Accessibility**: Ensure designs are usable by all users
- **Data-First**: Prioritize data visualization and information hierarchy

### Design Philosophy
- Focus on developer and data professional needs
- Balance technical precision with visual appeal
- Emphasize performance and scalability in visual design
- Support both light and dark themes

## Logo Usage

### Primary Logo
- **Clear Space**: Maintain minimum clear space equal to the height of the 'D' in Databricks
- **Minimum Size**: 
  - Digital: 120px width minimum
  - Print: 1 inch width minimum

### Logo Variations
- **Full Logo**: Databricks wordmark with icon
- **Icon Only**: Standalone DB icon for small spaces
- **Horizontal Layout**: Standard configuration
- **Vertical Layout**: For narrow spaces

### Logo Don'ts
- ❌ Do not alter logo colors
- ❌ Do not rotate or skew the logo
- ❌ Do not add effects (shadows, gradients)
- ❌ Do not change proportions
- ❌ Do not place on busy backgrounds

### Co-Branding
- Maintain equal visual weight with partner logos
- Use divider line or adequate spacing between logos
- Follow partner brand guidelines as well

## Typography

### Primary Typefaces

#### Display/Headers
- **Font Family**: [Primary Display Font]
- **Weights**: Light (300), Regular (400), Medium (500), Bold (700)
- **Usage**: Headlines, feature titles, marketing materials

#### Body Text
- **Font Family**: [Primary Body Font]
- **Weights**: Regular (400), Medium (500), Semibold (600)
- **Usage**: Body copy, descriptions, documentation

#### Code/Monospace
- **Font Family**: [Monospace Font]
- **Weights**: Regular (400), Medium (500)
- **Usage**: Code snippets, terminal output, technical content

### Type Scale
```css
/* Heading Sizes */
--h1: 48px / 56px line-height
--h2: 36px / 44px line-height  
--h3: 28px / 36px line-height
--h4: 24px / 32px line-height
--h5: 20px / 28px line-height
--h6: 18px / 24px line-height

/* Body Sizes */
--body-large: 18px / 28px line-height
--body-regular: 16px / 24px line-height
--body-small: 14px / 20px line-height
--caption: 12px / 16px line-height
```

### Typography Guidelines
- **Line Length**: 45-75 characters for optimal readability
- **Paragraph Spacing**: 1.5x line height between paragraphs
- **Letter Spacing**: 
  - Headlines: -0.02em
  - Body: 0
  - Uppercase: 0.05em

## Colors

### Primary Colors
```css
/* Brand Colors */
--databricks-orange: #FF3621;
--databricks-blue: #00A1F1;
--databricks-dark: #1B3139;

/* Primary Palette */
--primary-100: /* Lightest */
--primary-200: 
--primary-300: 
--primary-400: 
--primary-500: /* Base */
--primary-600: 
--primary-700: 
--primary-800: 
--primary-900: /* Darkest */
```

### Secondary Colors
```css
/* Supporting Colors */
--secondary-blue: #0076D6;
--secondary-green: #00BFA5;
--secondary-purple: #7C4DFF;
--secondary-yellow: #FFC107;
```

### Neutral Colors
```css
/* Grayscale */
--gray-50: #FAFAFA;
--gray-100: #F5F5F5;
--gray-200: #EEEEEE;
--gray-300: #E0E0E0;
--gray-400: #BDBDBD;
--gray-500: #9E9E9E;
--gray-600: #757575;
--gray-700: #616161;
--gray-800: #424242;
--gray-900: #212121;

/* Black & White */
--black: #000000;
--white: #FFFFFF;
```

### Semantic Colors
```css
/* Status Colors */
--success: #4CAF50;
--warning: #FF9800;
--error: #F44336;
--info: #2196F3;

/* Backgrounds */
--background-primary: #FFFFFF;
--background-secondary: #F5F7FA;
--background-tertiary: #E8ECF0;

/* Dark Mode */
--dark-background-primary: #1A1A1A;
--dark-background-secondary: #2D2D2D;
--dark-background-tertiary: #3A3A3A;
```

### Color Usage Guidelines
- **Contrast Ratios**: 
  - Normal text: 4.5:1 minimum
  - Large text: 3:1 minimum
  - Interactive elements: 3:1 minimum
- **Color Accessibility**: Ensure WCAG AA compliance
- **Dark Mode**: Provide equivalent dark theme colors

## Component Guidelines

### Buttons
```css
/* Primary Button */
.btn-primary {
  background: var(--databricks-orange);
  color: var(--white);
  padding: 12px 24px;
  border-radius: 4px;
  font-weight: 500;
}

/* Secondary Button */
.btn-secondary {
  background: transparent;
  color: var(--databricks-orange);
  border: 2px solid var(--databricks-orange);
  padding: 10px 22px;
}

/* Button States */
:hover { opacity: 0.9; }
:active { transform: scale(0.98); }
:disabled { opacity: 0.5; cursor: not-allowed; }
```

### Forms
- **Input Height**: 40px standard
- **Border Radius**: 4px
- **Border Color**: var(--gray-300)
- **Focus State**: 2px solid var(--databricks-blue)

### Cards
```css
.card {
  background: var(--white);
  border-radius: 8px;
  box-shadow: 0 2px 8px rgba(0,0,0,0.08);
  padding: 24px;
}
```

### Data Visualization
- **Chart Colors**: Use sequential color palette for data series
- **Grid Lines**: var(--gray-200)
- **Axis Labels**: var(--gray-700)
- **Tooltips**: Dark background with white text

## Spacing System
```css
/* Base unit: 8px */
--space-xs: 4px;
--space-sm: 8px;
--space-md: 16px;
--space-lg: 24px;
--space-xl: 32px;
--space-2xl: 48px;
--space-3xl: 64px;
--space-4xl: 96px;
```

## Grid System
- **Container Max Width**: 1280px
- **Columns**: 12 column grid
- **Gutters**: 24px (desktop), 16px (mobile)
- **Breakpoints**:
  ```css
  --mobile: 320px;
  --tablet: 768px;
  --desktop: 1024px;
  --wide: 1440px;
  ```

## Icons
- **Style**: Outlined, 2px stroke
- **Sizes**: 16px, 20px, 24px, 32px
- **Color**: Inherit from parent or use brand colors
- **Spacing**: 8px from adjacent text

## Animation
```css
/* Timing Functions */
--ease-in-out: cubic-bezier(0.4, 0, 0.2, 1);
--ease-out: cubic-bezier(0.0, 0, 0.2, 1);
--ease-in: cubic-bezier(0.4, 0, 1, 1);

/* Durations */
--duration-fast: 150ms;
--duration-normal: 250ms;
--duration-slow: 350ms;
```

## Accessibility Guidelines
- **Focus Indicators**: Visible focus states for all interactive elements
- **Color Contrast**: Meet WCAG AA standards
- **Keyboard Navigation**: Full keyboard accessibility
- **Screen Readers**: Proper ARIA labels and semantic HTML
- **Motion**: Respect prefers-reduced-motion

## Implementation Notes

### CSS Variables Setup
```css
:root {
  /* Add all color, spacing, and typography variables here */
}

[data-theme="dark"] {
  /* Override with dark mode values */
}
```

### Utility Classes
```css
/* Text Utilities */
.text-primary { color: var(--databricks-orange); }
.text-secondary { color: var(--databricks-blue); }
.text-muted { color: var(--gray-600); }

/* Background Utilities */
.bg-primary { background-color: var(--databricks-orange); }
.bg-light { background-color: var(--gray-50); }
.bg-dark { background-color: var(--databricks-dark); }

/* Spacing Utilities */
.p-sm { padding: var(--space-sm); }
.p-md { padding: var(--space-md); }
.p-lg { padding: var(--space-lg); }

.m-sm { margin: var(--space-sm); }
.m-md { margin: var(--space-md); }
.m-lg { margin: var(--space-lg); }
```

## File Naming Conventions
- Components: `PascalCase` (e.g., `ButtonPrimary.jsx`)
- Styles: `kebab-case` (e.g., `button-primary.css`)
- Assets: `kebab-case` (e.g., `logo-horizontal.svg`)
- Documentation: `UPPERCASE` (e.g., `README.md`)

---

*Note: This document should be updated with actual values from the official Databricks brand guidelines. Placeholder values are indicated with [brackets] or generic color codes.*

*Last Updated: [Date]*
*Version: 1.0.0*