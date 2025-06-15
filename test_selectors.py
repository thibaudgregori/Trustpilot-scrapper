from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import json
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def setup_driver():
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    return webdriver.Chrome(options=options)

def analyze_trustpilot_page(driver, url):
    """Analyser la structure d'une page Trustpilot pour identifier les sélecteurs"""
    driver.get(url)
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, "h1")))
    
    print(f"\n=== ANALYSE DE LA PAGE: {url} ===\n")
    
    # 1. Nom de l'entreprise
    try:
        name_element = driver.find_element(By.CSS_SELECTOR, "h1")
        print(f"✅ NOM: {name_element.text}")
        print(f"   Sélecteur: h1")
    except:
        print("❌ NOM: Non trouvé")
    
    # 2. Note - Essayer plusieurs sélecteurs
    rating_selectors = [
        "div[data-rating]",
        "p[data-rating-typography]", 
        "span[data-rating]",
        ".typography_display-l__gUWQR",
        "p.typography_display-l__gUWQR"
    ]
    
    rating_found = False
    for selector in rating_selectors:
        try:
            rating_element = driver.find_element(By.CSS_SELECTOR, selector)
            if rating_element.get_attribute("data-rating"):
                print(f"✅ NOTE: {rating_element.get_attribute('data-rating')}")
                print(f"   Sélecteur: {selector} (attribut data-rating)")
                rating_found = True
                break
            elif rating_element.text:
                print(f"✅ NOTE: {rating_element.text}")
                print(f"   Sélecteur: {selector} (texte)")
                rating_found = True
                break
        except:
            continue
    
    if not rating_found:
        print("❌ NOTE: Non trouvée avec les sélecteurs testés")
    
    # 3. Nombre de reviews - Essayer plusieurs sélecteurs
    reviews_selectors = [
        "span[data-reviews-count-typography]",
        "p[data-reviews-count-typography]",
        "div[data-reviews-count-typography]",
        "*[data-reviews-count-typography]"
    ]
    
    reviews_found = False
    for selector in reviews_selectors:
        try:
            reviews_element = driver.find_element(By.CSS_SELECTOR, selector)
            print(f"✅ REVIEWS: {reviews_element.text}")
            print(f"   Sélecteur: {selector}")
            reviews_found = True
            break
        except:
            continue
    
    if not reviews_found:
        print("❌ REVIEWS: Non trouvées avec les sélecteurs testés")
    
    # 4. Catégorie
    category_selectors = [
        "span[data-category-typography]",
        "p[data-category-typography]",
        "div[data-category-typography]"
    ]
    
    category_found = False
    for selector in category_selectors:
        try:
            category_element = driver.find_element(By.CSS_SELECTOR, selector)
            print(f"✅ CATÉGORIE: {category_element.text}")
            print(f"   Sélecteur: {selector}")
            category_found = True
            break
        except:
            continue
    
    if not category_found:
        print("❌ CATÉGORIE: Non trouvée avec les sélecteurs testés")
    
    # 5. Site web
    website_selectors = [
        "a[data-business-unit-website]",
        "a[href*='http']:not([href*='trustpilot'])"
    ]
    
    website_found = False
    for selector in website_selectors:
        try:
            website_element = driver.find_element(By.CSS_SELECTOR, selector)
            print(f"✅ SITE WEB: {website_element.get_attribute('href')}")
            print(f"   Sélecteur: {selector}")
            website_found = True
            break
        except:
            continue
    
    if not website_found:
        print("❌ SITE WEB: Non trouvé avec les sélecteurs testés")
    
    # 6. Adresse
    address_selectors = [
        "address",
        "div[data-address]",
        "span[data-address]"
    ]
    
    address_found = False
    for selector in address_selectors:
        try:
            address_element = driver.find_element(By.CSS_SELECTOR, selector)
            print(f"✅ ADRESSE: {address_element.text}")
            print(f"   Sélecteur: {selector}")
            address_found = True
            break
        except:
            continue
    
    if not address_found:
        print("❌ ADRESSE: Non trouvée avec les sélecteurs testés")
    
    # 7. Pourcentages d'étoiles - Essayer plusieurs approches
    print("\n--- POURCENTAGES D'ÉTOILES ---")
    star_selectors = [
        "div[data-star-rating='{}']",
        "span[data-star-rating='{}']",
        "*[data-star-rating='{}']"
    ]
    
    for star in range(1, 6):
        star_found = False
        for selector_template in star_selectors:
            selector = selector_template.format(star)
            try:
                star_element = driver.find_element(By.CSS_SELECTOR, selector)
                print(f"✅ {star} ÉTOILES: {star_element.text}")
                print(f"   Sélecteur: {selector}")
                star_found = True
                break
            except:
                continue
        
        if not star_found:
            print(f"❌ {star} ÉTOILES: Non trouvé")
    
    # 8. Analyser le JSON-LD (données structurées)
    print("\n--- DONNÉES STRUCTURÉES (JSON-LD) ---")
    try:
        json_scripts = driver.find_elements(By.CSS_SELECTOR, "script[type='application/ld+json']")
        for i, script in enumerate(json_scripts):
            try:
                data = json.loads(script.get_attribute('innerHTML'))
                print(f"✅ JSON-LD #{i+1} trouvé:")
                if isinstance(data, dict):
                    if 'name' in data:
                        print(f"   - Nom: {data['name']}")
                    if 'aggregateRating' in data:
                        rating_data = data['aggregateRating']
                        print(f"   - Note: {rating_data.get('ratingValue', 'N/A')}")
                        print(f"   - Reviews: {rating_data.get('reviewCount', 'N/A')}")
                    if 'address' in data:
                        print(f"   - Adresse: {data['address']}")
                    if 'url' in data:
                        print(f"   - Site: {data['url']}")
            except json.JSONDecodeError:
                continue
    except:
        print("❌ Aucun JSON-LD trouvé")

def main():
    driver = setup_driver()
    
    # Tester sur plusieurs pages d'entreprises
    test_urls = [
        "https://www.trustpilot.com/review/vestiairecollective.com",
        "https://www.trustpilot.com/review/vinted.fr",
        "https://www.trustpilot.com/review/24s.com"
    ]
    
    for url in test_urls:
        try:
            analyze_trustpilot_page(driver, url)
            print("\n" + "="*80 + "\n")
        except Exception as e:
            print(f"Erreur lors de l'analyse de {url}: {str(e)}")
    
    driver.quit()

if __name__ == "__main__":
    main() 