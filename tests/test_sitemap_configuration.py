import ast
from pathlib import Path
import unittest


CONFIG = Path(__file__).resolve().parents[1] / "wordpress_scraping_pipeline/backend/utils/config.py"


class SitemapConfigurationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        tree = ast.parse(CONFIG.read_text(encoding="utf-8"))
        definition = next(
            node for node in tree.body
            if isinstance(node, ast.AnnAssign)
            and isinstance(node.target, ast.Name)
            and node.target.id == "WORDPRESS_SITES"
        )
        cls.sites = ast.literal_eval(definition.value)

    def test_migrated_sites_use_confirmed_sitemap_endpoints(self):
        self.assertEqual(self.sites["FashionABC"]["sitemap"], "https://www.fashionabc.org/sitemap.xml")
        self.assertEqual(self.sites["FreedomX"]["sitemap"], "https://freedomx.com/sitemap.xml")

    def test_existing_wordpress_sitemap_endpoints_are_preserved(self):
        for name, url in {
            "HedgeThink": "http://www.hedgethink.com/sitemap_index.xml",
            "IntelligentHQ": "https://www.intelligenthq.com/sitemap_index.xml",
            "TradersDNA": "http://www.tradersdna.com/sitemap_index.xml",
        }.items():
            with self.subTest(site=name):
                self.assertEqual(self.sites[name]["sitemap"], url)

    def test_all_five_sources_retain_their_tracking_identity(self):
        self.assertEqual(set(self.sites), {"FashionABC", "FreedomX", "HedgeThink", "IntelligentHQ", "TradersDNA"})
        for name, site in self.sites.items():
            with self.subTest(site=name):
                self.assertEqual(site["source_tag"], f"WordPress/{name}")


if __name__ == "__main__":
    unittest.main()
