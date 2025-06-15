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

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def setup_driver():
    logging.info("Configuration du driver Chrome...")
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
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
        time.sleep(0.5)
        company_links = set()
        
        # Attendre que les liens des entreprises soient chargés
        WebDriverWait(driver, 3).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/review/']"))
        )
        
        # Récupérer tous les liens d'entreprises de la page
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

def scrape_company_data(driver, url):
    try:
        logging.info(f"Tentative de scraping pour l'URL: {url}")
        driver.get(url)
        time.sleep(0.5)
        
        # Attendre que les éléments principaux soient chargés
        WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "h1"))
        )
        
        # Extraire les données JSON-LD
        json_data = extract_json_ld_data(driver)
        
        # Nom de l'entreprise (nettoyer)
        name_raw = driver.find_element(By.CSS_SELECTOR, "h1").text
        name = re.sub(r'\s*Reviews\s+[\d,]+.*$', '', name_raw).strip()
        logging.info(f"Nom de l'entreprise trouvé: {name}")
        
        # Note
        try:
            rating_element = driver.find_element(By.CSS_SELECTOR, "p[data-rating-typography]")
            rating = rating_element.text.strip()
            logging.info(f"Note trouvée: {rating}")
        except NoSuchElementException:
            rating = json_data.get('aggregateRating', {}).get('ratingValue', "0")
            logging.warning(f"Note récupérée depuis JSON-LD: {rating}")
        
        # Nombre de reviews
        try:
            reviews_element = driver.find_element(By.CSS_SELECTOR, "p[data-reviews-count-typography]")
            reviews_count = reviews_element.text.replace(" total", "").strip()
            logging.info(f"Nombre de reviews trouvé: {reviews_count}")
        except NoSuchElementException:
            reviews_count = json_data.get('aggregateRating', {}).get('reviewCount', "0")
            logging.warning(f"Nombre de reviews récupéré depuis JSON-LD: {reviews_count}")
        
        # Catégorie - Détecter le type d'entreprise
        try:
            category = "Non spécifié"
            page_text = driver.page_source.lower()
            
            # Détecter des catégories spécifiques basées sur le contenu
            if any(word in page_text for word in ['wig store', 'hair extension', 'perruque', 'cheveux postiches', 'hair salon']):
                category = "Hair & Beauty"
            elif any(word in page_text for word in ['sneaker', 'basket', 'chaussure de sport', 'running shoes']):
                category = "Shoe Store"
            elif any(word in page_text for word in ['jewelry store', 'bijouterie', 'watch store', 'montre de luxe']):
                category = "Jewelry Store"
            elif any(word in page_text for word in ['cosmetic store', 'beauty salon', 'makeup store', 'parfumerie']):
                category = "Beauty Store"
            else:
                category = "Clothing Store"  # Par défaut
            
            logging.info(f"Catégorie détectée via contenu: {category}")
            logging.info(f"Catégorie finale: {category}")
        except Exception as e:
            category = "Clothing Store"
            logging.warning(f"Erreur catégorie, utilisation par défaut: {str(e)}")
        
        # Site web
        try:
            website = ""
            # Chercher le bouton "Visit website"
            try:
                visit_elements = driver.find_elements(By.XPATH, "//*[contains(text(), 'Visit website')]")
                for element in visit_elements:
                    parent = element.find_element(By.XPATH, "..")
                    if parent.tag_name == 'a':
                        href = parent.get_attribute("href")
                        if href and 'http' in href and not any(social in href.lower() for social in ['facebook', 'twitter', 'instagram', 'linkedin', 'youtube', 'trustpilot.com/review']):
                            website = href
                            break
                    
                    try:
                        ancestor_link = element.find_element(By.XPATH, "./ancestor::a")
                        if ancestor_link:
                            href = ancestor_link.get_attribute("href")
                            if href and 'http' in href and not any(social in href.lower() for social in ['facebook', 'twitter', 'instagram', 'linkedin', 'youtube', 'trustpilot.com/review']):
                                website = href
                                break
                    except:
                        pass
                
                if website:
                    logging.info(f"Site web trouvé via 'Visit website': {website}")
            except Exception as e:
                logging.warning(f"Erreur lors de la recherche 'Visit website': {str(e)}")
            
            # Si pas trouvé, chercher tous les liens externes
            if not website:
                try:
                    external_links = driver.find_elements(By.CSS_SELECTOR, "a[href*='http']")
                    for link in external_links:
                        href = link.get_attribute("href")
                        if href and not any(exclude in href.lower() for exclude in ['facebook', 'twitter', 'instagram', 'linkedin', 'youtube', 'trustpilot.com']):
                            if any(domain in href.lower() for domain in ['.com', '.fr', '.net', '.org', '.co.uk']):
                                website = href
                                break
                    
                    if website:
                        logging.info(f"Site web trouvé via liens externes: {website}")
                except Exception as e:
                    logging.warning(f"Erreur lors de la recherche de liens externes: {str(e)}")
            
            if not website:
                website = json_data.get('url', "")
                if website:
                    logging.info(f"Site web trouvé via JSON-LD: {website}")
                    
        except Exception as e:
            website = ""
            logging.error(f"Erreur générale lors de la recherche du site web: {str(e)}")
        
        # Adresse
        try:
            address = ""
            
            # Chercher les éléments qui contiennent une adresse française
            try:
                address_elements = driver.find_elements(By.XPATH, "//*[contains(text(), 'rue') or contains(text(), 'Rue') or contains(text(), 'avenue') or contains(text(), 'Avenue') or contains(text(), 'boulevard') or contains(text(), 'Boulevard')]")
                
                for element in address_elements:
                    text = element.text.strip()
                    if text and 10 < len(text) < 100:
                        if ',' in text or 'france' in text.lower():
                            if not any(bad in text.lower() for bad in ['http', '@', 'www.', 'review', 'trustpilot', 'go to', 'looks like']):
                                address = text
                                break
                
                if address:
                    logging.info(f"Adresse trouvée: {address}")
            except Exception as e:
                logging.warning(f"Erreur: {str(e)}")
            
            # Si pas trouvé, chercher les codes postaux français
            if not address:
                try:
                    postal_elements = driver.find_elements(By.XPATH, "//*[contains(text(), '75') or contains(text(), '69') or contains(text(), '13') or contains(text(), '44') or contains(text(), '59')]")
                    
                    for element in postal_elements:
                        text = element.text.strip()
                        if text and 10 < len(text) < 80:
                            if re.search(r'\d{5}', text) and (',' in text or 'france' in text.lower()):
                                if not any(bad in text.lower() for bad in ['http', '@', 'www.', 'review']):
                                    address = text
                                    break
                    
                    if address:
                        logging.info(f"Adresse trouvée (code postal): {address}")
                except:
                    pass
            
            if not address:
                address = ""
                logging.warning("Aucune adresse trouvée")
                
        except Exception as e:
            address = ""
            logging.error(f"Erreur adresse: {str(e)}")
        
        # Vérifier si l'entreprise est française
        non_french_countries = ['united states', 'usa', 'canada', 'uk', 'united kingdom', 'germany', 'spain', 'italy', 'belgium', 'netherlands']
        
        if address and any(country in address.lower() for country in non_french_countries):
            is_french = "Non"
            logging.info(f"Entreprise rejetée (pays étranger détecté): {address}")
        elif not address or address.strip() == "":
            is_french = "Oui"  # Pas d'adresse = on assume France (filtre du site)
            logging.info(f"Entreprise en France: Oui (pas d'adresse, filtre FR du site)")
        elif "france" in address.lower():
            is_french = "Oui"
            logging.info(f"Entreprise en France: Oui (France dans l'adresse)")
        else:
            # Vérifier si l'adresse contient des indicateurs français
            french_indicators = ['paris', 'lyon', 'marseille', 'bordeaux', 'lille', 'toulouse', 'nantes', 'strasbourg', 'montpellier', 'rennes']
            if any(city in address.lower() for city in french_indicators):
                is_french = "Oui"
                logging.info(f"Entreprise en France: Oui (ville française détectée: {address})")
            else:
                is_french = "Non"
                logging.info(f"Entreprise en France: Non (adresse sans indicateur français: {address})")
        
        # Récupérer les pourcentages d'étoiles
        star_percentages = {}
        try:
            for i in range(1, 6):
                try:
                    percentage = "0%"
                    
                    # Chercher directement les éléments qui contiennent "X-star" et un pourcentage
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
                        
                        if percentage != "0%":
                            logging.info(f"Pourcentage {i} étoiles trouvé: {percentage}")
                    except Exception as e:
                        logging.warning(f"Erreur méthode 1 pour {i} étoiles: {str(e)}")
                    
                    # Si pas trouvé, chercher dans la structure du tableau
                    if percentage == "0%":
                        try:
                            table_rows = driver.find_elements(By.XPATH, f"//tr[contains(., '{i}-star')] | //div[contains(@class, 'review') and contains(., '{i}-star')]")
                            
                            for row in table_rows:
                                row_text = row.text
                                
                                if "0%" in row_text:
                                    percentage = "0%"
                                    break
                                elif "<1%" in row_text:
                                    percentage = "1%"
                                    break
                                else:
                                    percent_match = re.search(r'(\d+)%', row_text)
                                    if percent_match:
                                        found_percent = percent_match.group(1)
                                        if 0 <= int(found_percent) <= 100:
                                            percentage = f"{found_percent}%"
                                            break
                            
                            if percentage != "0%":
                                logging.info(f"Pourcentage {i} étoiles trouvé (tableau): {percentage}")
                        except Exception as e:
                            logging.warning(f"Erreur méthode 2 pour {i} étoiles: {str(e)}")
                    
                    star_percentages[f"{i}_stars"] = percentage
                    if percentage == "0%":
                        logging.info(f"Pourcentage {i} étoiles: 0% (pas de reviews ou non trouvé)")
                    
                except Exception as e:
                    star_percentages[f"{i}_stars"] = "0%"
                    logging.warning(f"Erreur générale pour pourcentage {i} étoiles: {str(e)}")
                    
        except Exception as e:
            logging.error(f"Erreur générale pour les pourcentages: {str(e)}")
            for i in range(1, 6):
                star_percentages[f"{i}_stars"] = "0%"
        
        return {
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
        
    except Exception as e:
        logging.error(f"Erreur lors du scraping de {url}: {str(e)}")
        return None

def main():
    logging.info("Démarrage du script de scraping...")
    driver = setup_driver()
    batch_data = []
    
    # Créer le CSV avec les en-têtes
    columns = [
        "Nom de l'entreprise", "Note", "Nombre de reviews", "Catégorie", "Site", "Adresse", "En France",
        "Pourcentage 5 étoiles", "Pourcentage 4 étoiles", "Pourcentage 3 étoiles", "Pourcentage 2 étoiles", "Pourcentage 1 étoile"
    ]
    
    # Créer un nouveau fichier CSV ou continuer l'existant
    csv_filename = "entreprises_vetements_trustpilot_sequential.csv"
    
    # Vérifier si le fichier existe déjà
    try:
        existing_df = pd.read_csv(csv_filename, encoding='utf-8-sig')
        start_count = len(existing_df)
        logging.info(f"Fichier existant trouvé avec {start_count} entreprises. Continuation...")
    except FileNotFoundError:
        pd.DataFrame(columns=columns).to_csv(csv_filename, index=False, encoding='utf-8-sig')
        start_count = 0
        logging.info("Nouveau fichier CSV créé avec les en-têtes")
    
    # URL de base
    base_url = "https://www.trustpilot.com/categories/clothing_store?country=FR"
    
    try:
        # Parcourir toutes les pages
        for page in range(1, 106):
            page_url = f"{base_url}&page={page}"
            logging.info(f"Traitement de la page {page}")
            
            company_links = get_company_links_from_page(driver, page_url)
            
            for company_url in company_links:
                company_data = scrape_company_data(driver, company_url)
                if company_data:
                    # Ignorer les entreprises non-françaises
                    if company_data['En France'] == "Non":
                        logging.info(f"❌ Entreprise ignorée (non-française): {company_data['Nom de l\'entreprise']}")
                        continue
                    
                    batch_data.append(company_data)
                    logging.info(f"✅ Données récupérées pour: {company_data['Nom de l\'entreprise']} (Batch: {len(batch_data)}/10)")
                
                    # Sauvegarder par batch de 10
                    if len(batch_data) >= 10:
                        pd.DataFrame(batch_data).to_csv(csv_filename, mode='a', header=False, index=False, encoding='utf-8-sig')
                        logging.info(f"✅ Batch de {len(batch_data)} entreprises françaises sauvegardé dans le CSV")
                        batch_data = []
        
        # Sauvegarder les dernières données restantes
        if batch_data:
            pd.DataFrame(batch_data).to_csv(csv_filename, mode='a', header=False, index=False, encoding='utf-8-sig')
            logging.info(f"✅ Dernier batch de {len(batch_data)} entreprises sauvegardé")
        
        logging.info("Script terminé avec succès!")
        
    except Exception as e:
        # Sauvegarder les données en cours en cas d'erreur
        if batch_data:
            pd.DataFrame(batch_data).to_csv(csv_filename, mode='a', header=False, index=False, encoding='utf-8-sig')
            logging.info(f"Sauvegarde d'urgence de {len(batch_data)} entreprises")
        logging.error(f"Erreur générale: {str(e)}")
    finally:
        driver.quit()
        logging.info("Script terminé.")

if __name__ == "__main__":
    main() 