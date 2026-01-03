"""Unit tests for listing parser."""
import unittest
import os
from src.transform.parse_listing_html import parse_listing

class TestParser(unittest.TestCase):
    def setUp(self):
        self.fixture_dir = os.path.join(os.path.dirname(__file__), "fixtures")
    
    def load_fixture(self, filename: str) -> str:
        """Load HTML fixture file."""
        path = os.path.join(self.fixture_dir, filename)
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    
    def test_parse_normal_listing(self):
        """Test parsing a normal private listing."""
        html = self.load_fixture("listing_sample.html")
        result = parse_listing(html, "https://kolesa.kz/a/show/12345", 12345)
        
        self.assertEqual(result["listing_id"], 12345)
        self.assertEqual(result["price_kzt"], 12500000)
        self.assertEqual(result["make"], "Toyota")
        self.assertEqual(result["model"], "Camry")
        self.assertEqual(result["car_year"], 2020)
        self.assertEqual(result["city"], "Алматы")
        self.assertEqual(result["engine_volume_l"], 2.5)
        self.assertEqual(result["engine_type"], "бензин")
        self.assertTrue(result["customs_cleared"])
        self.assertIn(result["seller_type"], ["private", "dealer"])
        self.assertEqual(len(result["photos"]), 2)
        self.assertEqual(result["photo_count"], 2)
    
    def test_parse_dealer_listing(self):
        """Test parsing a dealer listing."""
        html = self.load_fixture("listing_dealer.html")
        result = parse_listing(html, "https://kolesa.kz/a/show/67890", 67890)
        
        self.assertEqual(result["listing_id"], 67890)
        self.assertEqual(result["price_kzt"], 35000000)
        self.assertEqual(result["make"], "Mercedes-Benz")
        self.assertEqual(result["model"], "E-Class")
        self.assertEqual(result["car_year"], 2023)
        self.assertEqual(result["city"], "Астана")
        self.assertEqual(result["region"], "Акмолинская область")
        self.assertEqual(result["mileage_km"], 15000)
        self.assertEqual(result["engine_volume_l"], 2.0)
        self.assertEqual(result["seller_type"], "dealer")
        self.assertEqual(result["seller_name"], "Premium Cars Алматы")
        self.assertGreaterEqual(len(result["photos"]), 5)
        self.assertIn("options_text", result)
        self.assertIsNotNone(result["options_text"])
    
    def test_parse_removed_listing(self):
        """Test parsing a removed/404 listing."""
        html = self.load_fixture("listing_removed.html")
        result = parse_listing(html, "https://kolesa.kz/a/show/99999", 99999)
        
        self.assertEqual(result["listing_id"], 99999)
        self.assertIsNotNone(result.get("title"))
        self.assertIsNone(result.get("price_kzt"))
    
    def test_parse_tolerance_to_missing_fields(self):
        """Test that parser handles missing fields gracefully."""
        minimal_html = """
        <html>
        <head><title>Test</title></head>
        <body>
            <h1>Test Car</h1>
            <div class="price">10 000 000 ₸</div>
        </body>
        </html>
        """
        result = parse_listing(minimal_html, "https://kolesa.kz/a/show/11111", 11111)
        
        self.assertEqual(result["listing_id"], 11111)
        self.assertEqual(result["price_kzt"], 10000000)
        self.assertEqual(result["title"], "Test Car")
        self.assertIsNone(result.get("city"))
        self.assertIsNone(result.get("mileage_km"))
        self.assertIsNone(result.get("engine_volume_l"))
    
    def test_parse_mileage_extraction(self):
        """Test mileage extraction from various formats."""
        html = """
        <html>
        <body>
            <h1>Test Car 2020</h1>
            <div>Пробег: 50 000 км</div>
        </body>
        </html>
        """
        result = parse_listing(html, "https://kolesa.kz/a/show/22222", 22222)
        self.assertEqual(result["mileage_km"], 50000)
    
    def test_parse_photo_extraction(self):
        """Test photo URL extraction."""
        html = """
        <html>
        <body>
            <h1>Test Car</h1>
            <img src="https://kolesa.kz/photo1.jpg">
            <img data-src="https://kolesa.kz/photo2.jpg">
            <img src="/relative/photo3.jpg">
        </body>
        </html>
        """
        result = parse_listing(html, "https://kolesa.kz/a/show/33333", 33333)
        self.assertGreaterEqual(len(result["photos"]), 2)
        photo_urls = result["photos"]
        self.assertTrue(any("photo1.jpg" in url for url in photo_urls))
        self.assertTrue(any("photo2.jpg" in url for url in photo_urls))
    
    def test_parse_options_extraction(self):
        """Test options/features extraction."""
        html = """
        <html>
        <body>
            <h1>Test Car</h1>
            <div>Опции и характеристики</div>
            <div>Кожаный салон, Панорамная крыша, Подогрев сидений</div>
        </body>
        </html>
        """
        result = parse_listing(html, "https://kolesa.kz/a/show/44444", 44444)
        self.assertIsNotNone(result.get("options_text"))
        self.assertIn("Кожаный салон", result["options_text"])

if __name__ == "__main__":
    unittest.main()
