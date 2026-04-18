import warnings
warnings.filterwarnings("ignore")

import io
import json
import time
import re
import datetime
import math
import random
import subprocess
import urllib.request
import zipfile
import pytz
import pandas as pd
from bs4 import BeautifulSoup
from pathlib import Path
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    StaleElementReferenceException,
    InvalidSessionIdException,
    WebDriverException,
    TimeoutException,
)

CHROME_PATH = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
_CHROMEDRIVER_CACHE = Path.home() / ".local" / "share" / "chromedriver_arm64" / "chromedriver"

TIPOS = (
    "condominio_residencial,"
    "apartamento_residencial,"
    "studio_residencial,"
    "kitnet_residencial,"
    "casa_residencial,"
    "casa-vila_residencial,"
    "cobertura_residencial,"
    "flat_residencial,"
    "loft_residencial,"
    "lote-terreno_residencial,"
    "sobrado_residencial,"
    "granja_residencial"
)


def _ensure_arm64_chromedriver():
    if _CHROMEDRIVER_CACHE.exists():
        out = subprocess.run(["file", str(_CHROMEDRIVER_CACHE)], capture_output=True, text=True).stdout
        if "arm64" in out:
            return str(_CHROMEDRIVER_CACHE)

    result = subprocess.run([CHROME_PATH, "--version"], capture_output=True, text=True)
    major = result.stdout.strip().split()[-1].split(".")[0]

    print(f"[*] Downloading ARM64 chromedriver for Chrome {major}...")
    api = "https://googlechromelabs.github.io/chrome-for-testing/known-good-versions-with-downloads.json"
    with urllib.request.urlopen(api) as resp:
        data = json.load(resp)

    matches = [v for v in data["versions"] if v["version"].startswith(f"{major}.")]
    if not matches:
        raise RuntimeError(f"No chromedriver found for Chrome {major}")

    arm64_url = next(
        d["url"] for d in matches[-1]["downloads"]["chromedriver"]
        if d["platform"] == "mac-arm64"
    )

    with urllib.request.urlopen(arm64_url) as resp:
        zf = zipfile.ZipFile(io.BytesIO(resp.read()))

    _CHROMEDRIVER_CACHE.parent.mkdir(parents=True, exist_ok=True)
    for name in zf.namelist():
        if name.endswith("/chromedriver"):
            _CHROMEDRIVER_CACHE.write_bytes(zf.read(name))
            _CHROMEDRIVER_CACHE.chmod(0o755)
            break

    # Pre-patch so undetected_chromedriver won't re-patch the binary and invalidate
    # the code signature (which causes macOS to SIGKILL the process).
    binary = _CHROMEDRIVER_CACHE.read_bytes()
    binary = binary.replace(b"cdc_", b"abc_")
    _CHROMEDRIVER_CACHE.write_bytes(binary)

    subprocess.run(["codesign", "--force", "--deep", "--sign", "-", str(_CHROMEDRIVER_CACHE)], check=False)
    subprocess.run(["xattr", "-c", str(_CHROMEDRIVER_CACHE)], check=False)

    print(f"[*] Chromedriver ARM64 ready at {_CHROMEDRIVER_CACHE}")
    return str(_CHROMEDRIVER_CACHE)


class ScraperZap:

    def __init__(self, transacao="aluguel", tipo=TIPOS, local="rs+porto-alegre", precomin=0, precomax=999999999):
        self.base_url = "https://www.zapimoveis.com.br"
        self.transacao = transacao
        self.tipo = tipo
        self.local = local
        self.precomin = precomin
        self.precomax = precomax
        self.timestamp_now = datetime.datetime.now(tz=pytz.timezone("America/Sao_Paulo"))
        self.driver = self._get_driver()
        print(f"[*] Initialized: {transacao} | {local} | R${precomin}-R${precomax}")

    # ── Driver ────────────────────────────────────────────────────────────────

    def _get_driver(self, headless=False):
        options = uc.ChromeOptions()
        if headless:
            options.add_argument("--headless=new")
        options.add_argument(f"--window-size={random.randint(1200,1920)},{random.randint(800,1080)}")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--no-sandbox")
        options.add_argument("--start-minimized")
        options.add_experimental_option("prefs", {"profile.managed_default_content_settings.images": 2})

        driver = uc.Chrome(
            options=options,
            use_subprocess=True,
            browser_executable_path=CHROME_PATH,
            driver_executable_path=_ensure_arm64_chromedriver(),
        )
        driver.minimize_window()
        time.sleep(random.uniform(1, 3))
        return driver

    def safe_get(self, url, retries=3):
        for attempt in range(retries):
            try:
                self.driver.get(url)
                return
            except WebDriverException as e:
                if "ERR_NAME_NOT_RESOLVED" in str(e):
                    print(f"[!] DNS error (attempt {attempt+1})")
                    time.sleep(random.uniform(2, 5))
                    try:
                        self.safe_quit()
                    except:
                        pass
                    self.driver = self._get_driver()
                else:
                    raise
        raise Exception("Failed to load page after retries")

    def safe_quit(self):
        try:
            if self.driver:
                self.driver.quit()
        except Exception as e:
            print(f"[!] Ignored quit error: {e}")
        finally:
            self.driver = None

    # ── Pagination ────────────────────────────────────────────────────────────

    def _build_url(self, page):
        return (
            f"{self.base_url}/{self.transacao}/apartamentos/{self.local}"
            f"/?pagina={page}&tipos={self.tipo}"
            f"&precoMinimo={self.precomin}&precoMaximo={self.precomax}&ordem=LOWEST_PRICE"
        )

    def get_total_listings(self):
        for _ in range(5):
            try:
                self.safe_get(self._build_url(1))
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.TAG_NAME, "h1"))
                )
                for _ in range(3):
                    try:
                        text = self.driver.find_element(By.TAG_NAME, "h1").text
                        break
                    except StaleElementReferenceException:
                        time.sleep(1)
                else:
                    raise Exception("Failed to read stable h1")

                numbers = re.findall(r"\d+", text.replace(".", "").replace(",", ""))
                total = int(numbers[0]) if numbers else 0
                if total == 0:
                    print("[!] No listings found")
                    return 0
                print(f"[*] Found {total} listings")
                return total

            except InvalidSessionIdException:
                self.safe_quit()
                print("[!] Driver died, restarting in 30s...")
                time.sleep(30)
                self.driver = self._get_driver()

        raise Exception("Failed after retries")

    def _click_next_page(self):
        css_selectors = [
            "[data-cy='rp-pagination-go-to-next-page']",
            "[data-cy='pagination-go-to-next-page']",
            "[data-cy='pagination-next-button']",
            "button[aria-label*='róxima']",
            "a[aria-label*='róxima']",
        ]
        for sel in css_selectors:
            try:
                for el in self.driver.find_elements(By.CSS_SELECTOR, sel):
                    if el.is_displayed() and el.is_enabled():
                        self.driver.execute_script("arguments[0].click();", el)
                        return True
            except:
                continue
        for xpath in ["//button[contains(.,'róxima')]", "//a[contains(.,'róxima')]"]:
            try:
                for el in self.driver.find_elements(By.XPATH, xpath):
                    if el.is_displayed() and el.is_enabled():
                        self.driver.execute_script("arguments[0].click();", el)
                        return True
            except:
                continue
        return False

    # ── Parse ─────────────────────────────────────────────────────────────────

    def parse_page(self, html):
        soup = BeautifulSoup(html, "html.parser")
        cards = soup.find_all(True, {"data-cy": "rp-property-cd"})
        results = []
        for card in cards:
            try:
                link_tag = card.find("a")
                url = link_tag.get("href") if link_tag else None
                id_match = re.search(r"id-(\d+)", url or "")
                imo_id = id_match.group(1) if id_match else None
                if not imo_id:
                    continue

                bairro, cidade = self._extract_bairro_cidade(card)
                preco, periodo_aluguel = self._extract_price_and_period(card, "rp-cardProperty-price-txt")
                condominio, iptu = self._extract_cond_iptu(card)

                results.append({
                    "id": imo_id,
                    "url": url,
                    "transacao": self.transacao,
                    "descricao": self._get_text(card, "h2", "rp-cardProperty-location-txt"),
                    "bairro": bairro,
                    "cidade": cidade,
                    "endereco": self._get_text(card, "p", "rp-cardProperty-street-txt"),
                    "area": self._extract_feature(card, "rp-cardProperty-propertyArea-txt"),
                    "quartos": self._extract_feature(card, "rp-cardProperty-bedroomQuantity-txt"),
                    "banheiros": self._extract_feature(card, "rp-cardProperty-bathroomQuantity-txt"),
                    "garagens": self._extract_feature(card, "rp-cardProperty-parkingSpacesQuantity-txt"),
                    "preco": preco,
                    "periodo_aluguel": periodo_aluguel,
                    "condominio": condominio,
                    "iptu": iptu,
                })
            except Exception as e:
                print(f"[!] Parse error: {e}")
        return results

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _get_text(self, card, tag, data_cy):
        el = card.find(tag, {"data-cy": data_cy})
        return el.text.strip() if el else None

    def _extract_bairro_cidade(self, card):
        try:
            h2 = card.find("h2", {"data-cy": "rp-cardProperty-location-txt"})
            if not h2:
                return None, None
            span = h2.find("span")
            if span:
                span.extract()
            parts = [p.strip() for p in h2.get_text(strip=True).split(",")]
            return parts[0] if parts else None, parts[1] if len(parts) > 1 else None
        except:
            return None, None

    def _extract_price_and_period(self, card, data_cy):
        el = card.find("div", {"data-cy": data_cy})
        if not el:
            return 0.0, None
        text = el.get_text(" ", strip=True).lower()
        match = re.search(r"r\$\s*([\d\.]+)", text)
        price = float(match.group(1).replace(".", "")) if match else 0.0
        period = "mensal" if "mês" in text else "diario" if "dia" in text else "unknown"
        return price, period

    def _extract_cond_iptu(self, card):
        try:
            text = card.get_text(" ", strip=True).lower()
            cond_match = re.search(r"cond\.\s*r\$\s*([\d\.]+)", text)
            iptu_match = re.search(r"iptu\s*r\$\s*([\d\.]+)", text)
            cond = float(cond_match.group(1).replace(".", "")) if cond_match else 0.0
            iptu = float(iptu_match.group(1).replace(".", "")) if iptu_match else 0.0
            return cond, iptu
        except:
            return 0.0, 0.0

    def _extract_feature(self, card, data_cy):
        try:
            el = card.find("li", {"data-cy": data_cy})
            if not el:
                return 0.0
            match = re.search(r"\d+", el.get_text(strip=True))
            return float(match.group()) if match else 0.0
        except:
            return 0.0

    def _p98_price(self, listings):
        prices = sorted(item["preco"] for item in listings if item.get("preco"))
        if not prices:
            return 0
        return prices[max(int(0.98 * len(prices)) - 1, 0)]

    # ── Run ───────────────────────────────────────────────────────────────────

    def run(self):
        scraped_at = self.timestamp_now.strftime("%Y-%m-%d %H:%M:%S")
        all_data = []
        remaining = 501

        while remaining > 500:
            remaining = self.get_total_listings()
            if remaining == 0:
                break

            pages = min(math.ceil(remaining / 28), 50)
            print(f"[*] R${self.precomin:,} → R${self.precomax:,} | {remaining} listings | {pages} pages")

            try:
                WebDriverWait(self.driver, 25).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "[data-cy='rp-property-cd']"))
                )
            except TimeoutException:
                print("[!] Listings didn't load on page 1")
                break

            time.sleep(random.uniform(4, 8))
            for i in range(1, 5):
                self.driver.execute_script(f"window.scrollTo(0, document.body.scrollHeight * {i/4});")
                time.sleep(0.5)

            batch = self.parse_page(self.driver.page_source)
            print(f"[*] Page 1/{pages}  — {len(batch)} listings")

            for page in range(2, pages + 1):
                time.sleep(random.uniform(8, 15))

                if not self._click_next_page():
                    print(f"[!] Next page button not found at page {page}")
                    break

                try:
                    WebDriverWait(self.driver, 25).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "[data-cy='rp-property-cd']"))
                    )
                except TimeoutException:
                    print(f"[!] No listings on page {page}")
                    break

                time.sleep(random.uniform(4, 8))
                for i in range(1, 5):
                    self.driver.execute_script(f"window.scrollTo(0, document.body.scrollHeight * {i/4});")
                    time.sleep(0.5)

                page_data = self.parse_page(self.driver.page_source)
                if not page_data:
                    print(f"[!] Empty parse on page {page}")
                    break

                batch.extend(page_data)
                print(f"[*] Page {page}/{pages}  — {len(page_data)} listings")

            all_data.extend(batch)
            max_price = self._p98_price(batch)
            if max_price <= self.precomin:
                max_price = max_price * 1.02
            self.precomin = int(max_price)

        self.safe_quit()

        if not all_data:
            return pd.DataFrame()

        df = pd.DataFrame(all_data)
        df["scraped_at"] = scraped_at
        return df


def main():
    Path("data/scrape").mkdir(parents=True, exist_ok=True)
    start = datetime.datetime.now()
    timestamp = start.strftime("%Y-%m-%d_%H-%M-%S")
    all_dfs = []

    for transacao in ["venda", "aluguel"]:
        t0 = datetime.datetime.now()
        print(f"\n[{t0.strftime('%H:%M:%S')}] {'─'*44}")
        print(f"[{t0.strftime('%H:%M:%S')}]  {transacao.upper()} | Porto Alegre")
        print(f"[{t0.strftime('%H:%M:%S')}] {'─'*44}")

        scraper = ScraperZap(transacao=transacao, local="rs+porto-alegre", tipo=TIPOS)
        df = scraper.run()

        elapsed = (datetime.datetime.now() - t0).seconds
        if not df.empty:
            print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}]  {transacao}: {len(df)} listings  ({elapsed}s)")
            all_dfs.append(df)
        else:
            print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}]  {transacao}: no data  ({elapsed}s)")

    if not all_dfs:
        print("\n[!] No data scraped")
        return

    df_final = pd.concat(all_dfs, ignore_index=True)
    df_final = df_final.drop_duplicates(subset=["id"], keep="last")

    path = Path("data/scrape") / f"{timestamp}.parquet"
    df_final.to_parquet(path, index=False)

    total_elapsed = (datetime.datetime.now() - start).seconds
    print(f"\n[{datetime.datetime.now().strftime('%H:%M:%S')}]  Saved → {path}  ({len(df_final)} rows, {total_elapsed}s total)")
    return path


if __name__ == "__main__":
    main()
