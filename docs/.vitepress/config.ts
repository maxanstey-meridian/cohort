import { defineConfig } from "vitepress";

export default defineConfig({
  title: "Cohort",
  description: "Annotation-driven data retention for EF Core and PostgreSQL",
  base: "/cohort/",

  themeConfig: {
    nav: [
      { text: "Get Started", link: "/getting-started" },
      { text: "Guides", link: "/guides/retention-rules" },
      { text: "Configuration", link: "/reference/configuration" },
      { text: "NuGet", link: "https://www.nuget.org/packages/Cohort" },
    ],

    sidebar: [
      {
        text: "Introduction",
        items: [
          { text: "What is Cohort?", link: "/" },
          { text: "Getting Started", link: "/getting-started" },
        ],
      },
      {
        text: "Guides",
        items: [
          { text: "Retention Rules", link: "/guides/retention-rules" },
          { text: "Running Retention", link: "/guides/running-retention" },
          { text: "Anonymisation", link: "/guides/anonymisation" },
          { text: "Right-to-Erasure", link: "/guides/erasure" },
          { text: "Legal Holds", link: "/guides/legal-holds" },
          { text: "Row Handlers", link: "/guides/row-handlers" },
          { text: "Audit Trail", link: "/guides/audit-trail" },
          { text: "History Pruning", link: "/guides/history-pruning" },
        ],
      },
      {
        text: "Reference",
        items: [
          { text: "Configuration", link: "/reference/configuration" },
          { text: "Attributes", link: "/reference/attributes" },
          { text: "Schema & Migrations", link: "/reference/schema" },
          { text: "Startup Validation", link: "/reference/startup-validation" },
        ],
      },
      {
        text: "Misc",
        items: [
          { text: "How It Works", link: "/misc/how-it-works" },
          { text: "Limitations", link: "/misc/limitations" },
        ],
      },
    ],

    socialLinks: [{ icon: "github", link: "https://github.com/maxanstey-meridian/cohort" }],

    search: {
      provider: "local",
    },
  },
});
