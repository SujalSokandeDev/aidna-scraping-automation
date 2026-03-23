# GitHub Repository Secrets Reference

## Repository: `aidna-scraping-automation`

Go to: **Settings → Secrets and variables → Actions → New repository secret**

Create each secret below. **Replace placeholder values with your actual credentials.**

---

| # | Secret Name | Value (placeholder) | Where to find it |
|---|---|---|---|
| 1 | `BUSINESSABC_STRAPI_GRAPHQL_URL` | `https://cms.businessabc.net/graphql` | CMS pipeline .env |
| 2 | `BUSINESSABC_STRAPI_BEARER_TOKEN` | `your-businessabc-bearer-token` | CMS pipeline .env |
| 3 | `SPORTSABC_STRAPI_GRAPHQL_URL` | `https://cms.sportsabc.org/graphql` | CMS pipeline .env |
| 4 | `SPORTSABC_STRAPI_BEARER_TOKEN` | `your-sportsabc-bearer-token` | CMS pipeline .env |
| 5 | `GCP_PROJECT_ID` | `your-gcp-project-id` | GCP Console |
| 6 | `GCP_SERVICE_ACCOUNT_JSON` | *(entire contents of service-account.json)* | GCP Console → IAM → Service Accounts |
| 7 | `SUPABASE_URL` | `https://your-project.supabase.co` | Supabase dashboard → Settings → API |
| 8 | `SUPABASE_KEY` | `your-supabase-anon-key` | Supabase dashboard → Settings → API |

**Total: 8 secrets**

> ⚠️ **IMPORTANT**: Never commit real secret values to this file or any file in the repo.
> The actual values should ONLY exist in GitHub Secrets.
