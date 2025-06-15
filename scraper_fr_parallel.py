from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import pandas as pd
import time
import re
import json
from urllib.parse import urlparse
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from queue import Queue
import os

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Lock pour l'écriture du CSV
csv_lock = threading.Lock()

def setup_driver():
    logging.info("Configuration du driver Chrome...")
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--no-first-run')
    options.add_argument('--disable-extensions')
    return webdriver.Chrome(options=options)

def extract_json_ld_data(driver):
    """Extraire les données du JSON-LD"""
    try:
        json_scripts = driver.find_elements(By.CSS_SELECTOR, "script[type='application/ld+json']")
        for script in json_scripts:
            try:
                data = json.loads(script.get_attribute('innerHTML'))
                if isinstance(data, dict):
                    return data
                elif isinstance(data, list) and len(data) > 0:
                    return data[0]
            except json.JSONDecodeError:
                continue
    except:
        pass
    return {}

def get_company_links_from_page(driver, page_url):
    try:
        driver.get(page_url)
        time.sleep(0.3)  # Réduit encore plus pour la parallélisation
        company_links = set()
        
        WebDriverWait(driver, 2).until(  # Réduit à 2 secondes
            EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/review/']"))
        )
        
        links = driver.find_elements(By.CSS_SELECTOR, "a[href*='/review/']")
        for link in links:
            href = link.get_attribute("href")
            if href and "trustpilot.com/review/" in href:
                company_links.add(href)
        
        logging.info(f"Nombre d'entreprises trouvées sur la page: {len(company_links)}")
        return list(company_links)
    except Exception as e:
        logging.error(f"Erreur lors de la récupération des liens sur {page_url}: {str(e)}")
        return []

def scrape_company_data(url):
    """Version thread-safe du scraping d'une entreprise"""
    driver = None
    try:
        driver = setup_driver()
        logging.info(f"Worker - Tentative de scraping pour l'URL: {url}")
        driver.get(url)
        time.sleep(0.3)  # Réduit pour la parallélisation
        
        WebDriverWait(driver, 2).until(  # Réduit à 2 secondes
            EC.presence_of_element_located((By.CSS_SELECTOR, "h1"))
        )
        
        # Extraire les données JSON-LD
        json_data = extract_json_ld_data(driver)
        
        # Récupérer les données de base - NETTOYER LE NOM
        name_raw = driver.find_element(By.CSS_SELECTOR, "h1").text
        name = re.sub(r'\s*Reviews\s+[\d,]+.*$', '', name_raw).strip()
        
        # Note de l'entreprise sur 5
        try:
            rating_element = driver.find_element(By.CSS_SELECTOR, "p[data-rating-typography]")
            rating = rating_element.text.strip()
        except NoSuchElementException:
            rating = json_data.get('aggregateRating', {}).get('ratingValue', "0")
        
        # Nombre de reviews
        try:
            reviews_element = driver.find_element(By.CSS_SELECTOR, "p[data-reviews-count-typography]")
            reviews_count = reviews_element.text.replace(" total", "").strip()
        except NoSuchElementException:
            reviews_count = json_data.get('aggregateRating', {}).get('reviewCount', "0")
        
        # Catégorie - Version simplifiée pour la parallélisation
        category = "Clothing Store"  # Par défaut puisqu'on est sur cette catégorie
        try:
            page_text = driver.page_source.lower()
            if any(word in page_text for word in ['jewelry store', 'bijouterie']):
                category = "Jewelry Store"
            elif any(word in page_text for word in ['shoe store', 'chaussure']):
                category = "Shoe Store"
            elif any(word in page_text for word in ['beauty', 'cosmetic']):
                category = "Beauty Store"
        except:
            pass
        
        # Site de l'entreprise - Version simplifiée
        website = ""
        try:
            visit_elements = driver.find_elements(By.XPATH, "//*[contains(text(), 'Visit website')]")
            for element in visit_elements:
                try:
                    parent = element.find_element(By.XPATH, "..")
                    if parent.tag_name == 'a':
                        href = parent.get_attribute("href")
                        if href and 'http' in href and not any(social in href.lower() for social in ['facebook', 'twitter', 'instagram', 'linkedin', 'youtube', 'trustpilot.com/review']):
                            website = href
                            break
                except:
                    continue
        except:
            pass
        
        # Adresse - Version ultra-simplifiée pour la parallélisation
        address = ""
        try:
            address_elements = driver.find_elements(By.XPATH, "//*[contains(text(), 'rue') or contains(text(), 'Rue') or contains(text(), 'avenue') or contains(text(), 'Avenue')]")
            
            for element in address_elements:
                text = element.text.strip()
                if text and 10 < len(text) < 100:
                    if ',' in text or 'france' in text.lower():
                        if not any(bad in text.lower() for bad in ['http', '@', 'www.', 'review', 'trustpilot', 'go to', 'looks like']):
                            address = text
                            break
        except:
            pass
        
        # Vérifier si l'entreprise est française - Logique stricte
        non_french_countries = ['united states', 'usa', 'canada', 'uk', 'united kingdom', 'germany', 'spain', 'italy', 'belgium', 'netherlands']
        
        if address and any(country in address.lower() for country in non_french_countries):
            is_french = "Non"
        elif not address or address.strip() == "":
            is_french = "Oui"
        elif "france" in address.lower():
            is_french = "Oui"
        else:
            french_indicators = ['paris', 'lyon', 'marseille', 'bordeaux', 'lille', 'toulouse', 'nantes', 'strasbourg', 'montpellier', 'rennes']
            if any(city in address.lower() for city in french_indicators):
                is_french = "Oui"
            else:
                is_french = "Non"
        
        # Pourcentages d'étoiles - Version simplifiée pour la parallélisation
        star_percentages = {}
        try:
            for i in range(1, 6):
                percentage = "0%"
                try:
                    star_elements = driver.find_elements(By.XPATH, f"//*[contains(text(), '{i}-star')]")
                    
                    for star_element in star_elements:
                        parent = star_element.find_element(By.XPATH, "..")
                        parent_text = parent.text
                        
                        if "0%" in parent_text:
                            percentage = "0%"
                            break
                        elif "<1%" in parent_text:
                            percentage = "1%"
                            break
                        else:
                            percent_matches = re.findall(r'(\d+)%', parent_text)
                            if percent_matches:
                                for percent in percent_matches:
                                    if 0 <= int(percent) <= 100:
                                        percentage = f"{percent}%"
                                        break
                                if percentage != "0%":
                                    break
                except:
                    pass
                
                star_percentages[f"{i}_stars"] = percentage
        except:
            for i in range(1, 6):
                star_percentages[f"{i}_stars"] = "0%"
        
        result = {
            "Nom de l'entreprise": name,
            "Note": rating,
            "Nombre de reviews": reviews_count,
            "Catégorie": category,
            "Site": website,
            "Adresse": address,
            "En France": is_french,
            "Pourcentage 5 étoiles": star_percentages["5_stars"],
            "Pourcentage 4 étoiles": star_percentages["4_stars"],
            "Pourcentage 3 étoiles": star_percentages["3_stars"],
            "Pourcentage 2 étoiles": star_percentages["2_stars"],
            "Pourcentage 1 étoile": star_percentages["1_stars"]
        }
        
        logging.info(f"✅ Worker terminé pour: {name}")
        return result
        
    except Exception as e:
        logging.error(f"❌ Erreur lors du scraping de {url}: {str(e)}")
        return None
    finally:
        if driver:
            driver.quit()

def save_company_data(company_data, csv_file):
    """Sauvegarde thread-safe d'une entreprise"""
    with csv_lock:
        try:
            # Vérifier si le fichier existe
            file_exists = os.path.exists(csv_file)
            
            df = pd.DataFrame([company_data])
            df.to_csv(csv_file, mode='a', header=not file_exists, index=False, encoding='utf-8-sig')
            logging.info(f"💾 Sauvegardé: {company_data['Nom de l\'entreprise']}")
        except Exception as e:
            logging.error(f"❌ Erreur sauvegarde: {str(e)}")

def main():
    logging.info("🚀 Démarrage du script de scraping PARALLÈLE...")
    
    # Créer le CSV avec les en-têtes
    columns = [
        "Nom de l'entreprise", "Note", "Nombre de reviews", "Catégorie", "Site", "Adresse", "En France",
        "Pourcentage 5 étoiles", "Pourcentage 4 étoiles", "Pourcentage 3 étoiles", "Pourcentage 2 étoiles", "Pourcentage 1 étoile"
    ]
    csv_file = "entreprises_vetements_trustpilot.csv"
    pd.DataFrame(columns=columns).to_csv(csv_file, index=False, encoding='utf-8-sig')
    logging.info("📄 Fichier CSV créé avec les en-têtes")
    
    # URL de base
    base_url = "https://www.trustpilot.com/categories/clothing_store?country=FR"
    
    # Driver principal pour récupérer les liens
    main_driver = setup_driver()
    
    try:
        # Récupérer tous les liens d'entreprises de toutes les pages
        all_company_links = []
        
        for page in range(1, 106):  # 105 pages
            page_url = f"{base_url}&page={page}"
            logging.info(f"📄 Récupération des liens - Page {page}")
            
            company_links = get_company_links_from_page(main_driver, page_url)
            all_company_links.extend(company_links)
            
            if page % 10 == 0:
                logging.info(f"📊 Total de liens récupérés: {len(all_company_links)}")
        
        logging.info(f"🎯 Total final de liens: {len(all_company_links)}")
        
        # Traitement parallèle avec pool de 10 workers
        processed_count = 0
        french_count = 0
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            # Soumettre toutes les tâches
            future_to_url = {executor.submit(scrape_company_data, url): url for url in all_company_links}
            
            # Traiter les résultats dès qu'ils arrivent
            for future in as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    company_data = future.result()
                    processed_count += 1
                    
                    if company_data:
                        if company_data['En France'] == "Oui":
                            # Sauvegarder immédiatement les entreprises françaises
                            save_company_data(company_data, csv_file)
                            french_count += 1
                            logging.info(f"🇫🇷 Entreprises françaises: {french_count} | Total traité: {processed_count}/{len(all_company_links)}")
                        else:
                            logging.info(f"❌ Ignorée (non-française): {company_data['Nom de l\'entreprise']}")
                    
                    # Log de progression toutes les 50 entreprises
                    if processed_count % 50 == 0:
                        logging.info(f"📊 PROGRESSION: {processed_count}/{len(all_company_links)} ({processed_count/len(all_company_links)*100:.1f}%) - Françaises: {french_count}")
                        
                except Exception as e:
                    logging.error(f"❌ Erreur pour {url}: {str(e)}")
        
        logging.info(f"🎉 Script terminé! Entreprises françaises sauvegardées: {french_count}")
        
    except Exception as e:
        logging.error(f"❌ Erreur générale: {str(e)}")
    finally:
        main_driver.quit()
        logging.info("🏁 Script terminé.")

if __name__ == "__main__":
    main() 