import requests
from bs4 import BeautifulSoup
import json
import random
import time
import logging
from datetime import datetime, timedelta
import schedule
import sys
import os
import re
from urllib.parse import urlparse, parse_qs

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("scraper/scraper.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Lista de User-Agents para rotación
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/123.0.0.0 Safari/537.36"
]

def get_random_headers():
    """Genera headers HTTP aleatorios y realistas para evitar detección"""
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept-Language": "es-MX,es;q=0.9,en;q=0.8",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Cache-Control": "max-age=0",
        "Referer": "https://www.mercadolibre.com.mx/",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-User": "?1"
    }

def extract_product_id(url):
    """Extrae el ID del producto de la URL de Mercado Libre"""
    # Patrones comunes de URLs de Mercado Libre
    patterns = [
        r'MLA?M(\d+)',  # Formato MLM12345678
        r'/p/MLA?M(\d+)',  # Formato /p/MLM12345678
        r'-_JM#position=(\d+)',  # Formato posición en listado
        r'_ID=(\d+)',  # Formato ID en algunos enlaces
        r'/(\d+)-',  # Formato numérico en URLs amigables
    ]
    
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    
    # Si no encontramos un ID con los patrones, intentamos extraerlo del path
    parsed_url = urlparse(url)
    path_parts = parsed_url.path.split('/')
    
    # Buscamos partes numéricas en el path que podrían ser IDs
    for part in path_parts:
        if part.isdigit() and len(part) > 5:  # IDs suelen ser números largos
            return part
    
    return None

def clean_product_url(url):
    """Genera una URL limpia para el producto de Mercado Libre"""
    if not url:
        return None
    
    # Si la URL ya es limpia (no es de tracking), solo quitamos parámetros
    if "mercadolibre.com.mx" in url and "click" not in url:
        # Eliminamos parámetros de tracking
        clean_url = url.split("?")[0]
        # Eliminamos fragmentos
        clean_url = clean_url.split("#")[0]
        return clean_url
    
    # Para URLs de tracking, intentamos extraer el ID del producto
    product_id = extract_product_id(url)
    
    if product_id:
        # Construimos una URL directa usando el ID del producto
        # Formato: https://articulo.mercadolibre.com.mx/MLM-{ID}-{slug}
        logger.info(f"ID de producto extraído: {product_id}")
        
        # Intentamos obtener la URL real siguiendo la redirección
        try:
            session = requests.Session()
            response = session.get(url, headers=get_random_headers(), timeout=10, allow_redirects=True)
            
            if response.status_code == 200:
                final_url = response.url.split("?")[0].split("#")[0]
                
                # Verificamos que la URL final no sea una URL de tracking
                if "click" in final_url or "mclics" in final_url:
                    # Si seguimos en una URL de tracking, usamos una URL genérica
                    generic_url = f"https://articulo.mercadolibre.com.mx/MLM-{product_id}-item"
                    logger.info(f"URL final sigue siendo de tracking, usando URL genérica: {generic_url}")
                    return generic_url
                
                logger.info(f"URL final obtenida: {final_url}")
                return final_url
            else:
                # Si falla, construimos una URL genérica
                generic_url = f"https://articulo.mercadolibre.com.mx/MLM-{product_id}-item"
                logger.info(f"Usando URL genérica: {generic_url}")
                return generic_url
        except Exception as e:
            logger.error(f"Error siguiendo redirección: {str(e)}")
            # Si falla, construimos una URL genérica
            generic_url = f"https://articulo.mercadolibre.com.mx/MLM-{product_id}-item"
            logger.info(f"Usando URL genérica: {generic_url}")
            return generic_url
    
    # Si todo falla, devolvemos la URL original sin parámetros
    logger.warning(f"No se pudo limpiar la URL: {url}")
    return url.split("?")[0].split("#")[0]

def get_real_image_url(img_elem):
    """Obtiene la imagen HD (evita placeholders)"""
    if not img_elem:
        return None
        
    # Intentamos obtener la URL de la imagen, priorizando data-src (carga diferida)
    url = img_elem.get("data-src") or img_elem.get("src")
    
    # Verificamos que no sea un placeholder base64
    if not url or url.startswith("data:image"):
        return None
    
    # Convertimos a formato HD WebP
    hd_url = url.replace("O.jpg", "V.webp").replace("O.png", "V.webp")
    
    # Añadimos 2X para alta resolución si no existe
    if "2X" not in hd_url and "NP_" in hd_url:
        hd_url = hd_url.replace("NP_", "NP_2X_")
    
    return hd_url

def extract_number(text):
    """Extrae números de un texto"""
    if not text:
        return 0
    return int(''.join(filter(str.isdigit, text)) or 0)

def extract_category_from_url(url):
    """Extrae la categoría de la URL"""
    categories = {
        "videojuegos": ["videojuegos", "consolas", "gaming"],
        "electronica": ["electronica", "audio", "tv", "celulares", "smartphones"],
        "computacion": ["computacion", "laptops", "notebooks", "pc", "computadoras"],
        "tecnologia": ["tecnologia", "gadgets", "smartwatch", "tablets"]
    }
    
    url_lower = url.lower()
    for category, keywords in categories.items():
        if any(keyword in url_lower for keyword in keywords):
            return category
    
    return "tecnologia"  # Categoría por defecto

def load_published_offers():
    """Carga el historial de ofertas publicadas"""
    try:
        if os.path.exists("bot/published_offers.json"):
            with open("bot/published_offers.json", "r", encoding="utf-8") as f:
                return json.load(f)
        return {}
    except Exception as e:
        logger.error(f"Error cargando historial de ofertas: {str(e)}")
        return {}

def save_published_offer(offer):
    """Guarda una oferta en el historial de publicadas"""
    try:
        # Cargamos el historial existente
        published_offers = load_published_offers()
        
        # Generamos un ID único para la oferta basado en el título
        offer_id = generate_offer_id(offer)
        
        # Guardamos la oferta en el historial
        published_offers[offer_id] = {
            "title": offer["titulo"],
            "price": offer["precio_actual"],
            "original_price": offer["precio_original"],
            "discount": offer["descuento"],
            "url": offer["enlace"],
            "image": offer["imagen"],
            "published_at": datetime.now().isoformat()
        }
        
        # Guardamos el historial actualizado
        with open("bot/published_offers.json", "w", encoding="utf-8") as f:
            json.dump(published_offers, f, ensure_ascii=False, indent=2)
        
        logger.info(f"Oferta guardada en el historial: {offer['titulo']}")
        return True
    except Exception as e:
        logger.error(f"Error guardando oferta en el historial: {str(e)}")
        return False

def generate_offer_id(offer):
    """Genera un ID único para una oferta basado únicamente en su título"""
    # Usamos solo el título para generar el ID
    title_clean = ' '.join(offer['titulo'].lower().split())
    
    # Método simple de hashing para generar un ID corto
    import hashlib
    hash_object = hashlib.md5(title_clean.encode())
    return f"offer_{hash_object.hexdigest()[:8]}"

def is_offer_already_published(offer, published_offers):
    """Verifica si una oferta ya ha sido publicada en el último mes"""
    if not published_offers:
        logger.info("No hay historial de ofertas publicadas")
        return False, None
    
    # Fecha actual
    current_date = datetime.now()
    
    # Generamos el ID de la oferta actual basado solo en el título
    offer_id = generate_offer_id(offer)
    
    # Verificamos si el ID existe en el historial
    if offer_id in published_offers:
        published_offer = published_offers[offer_id]
        
        # Verificamos la fecha de publicación
        try:
            published_date = datetime.fromisoformat(published_offer["published_at"].replace('Z', '+00:00'))
            
            # Calculamos la diferencia en días
            days_since_published = (current_date - published_date).days
            
            # Si han pasado más de 30 días, permitimos volver a publicar
            if days_since_published > 30:
                logger.info(f"Oferta publicada hace más de un mes ({days_since_published} días), permitiendo republicación: {offer['titulo']}")
                return False, offer_id
            
            logger.info(f"Oferta publicada hace menos de un mes ({days_since_published} días), saltando: {offer['titulo']}")
            return True, offer_id
            
        except (ValueError, KeyError) as e:
            logger.warning(f"Error al procesar la fecha de publicación: {str(e)}")
            # Si hay un error con la fecha, asumimos que es reciente
            return True, offer_id
    
    # También verificamos por similitud en título
    for pub_id, pub_offer in published_offers.items():
        # Verificamos solo por similitud en el título
        if similar_titles(offer["titulo"], pub_offer["title"]):
            try:
                published_date = datetime.fromisoformat(pub_offer["published_at"].replace('Z', '+00:00'))
                days_since_published = (current_date - published_date).days
                
                if days_since_published > 30:
                    logger.info(f"Oferta similar publicada hace más de un mes ({days_since_published} días), permitiendo republicación: {offer['titulo']}")
                    return False, pub_id
                
                logger.info(f"Oferta similar publicada hace menos de un mes ({days_since_published} días), saltando: {offer['titulo']}")
                return True, pub_id
                
            except (ValueError, KeyError) as e:
                logger.warning(f"Error al procesar la fecha de publicación: {str(e)}")
                # Si hay un error con la fecha, asumimos que es reciente
                return True, pub_id
    
    logger.info(f"Oferta nueva, no publicada anteriormente: {offer['titulo']}")
    return False, None

def similar_titles(title1, title2):
    """Verifica si dos títulos son similares"""
    # Convertimos a minúsculas y eliminamos espacios extra
    t1 = ' '.join(title1.lower().split())
    t2 = ' '.join(title2.lower().split())
    
    # Si son idénticos, son similares
    if t1 == t2:
        return True
    
    # Verificamos si uno contiene al otro
    if t1 in t2 or t2 in t1:
        return True
    
    # Verificamos palabras clave comunes (más de 3 palabras en común)
    words1 = set(t1.split())
    words2 = set(t2.split())
    common_words = words1.intersection(words2)
    
    # Si hay más de 3 palabras en común y representan más del 50% de las palabras más cortas
    min_words = min(len(words1), len(words2))
    if len(common_words) > 3 and len(common_words) / min_words > 0.5:
        return True
    
    return False

def extract_offer(card, product_id=None):
    """Extrae datos de un producto con manejo robusto de errores"""
    try:
        # Extraemos el título y enlace
        title_elem = card.select_one("a.poly-component__title") or card.select_one("h2.ui-search-item__title a")
        
        if not title_elem:
            logger.warning(f"Selector no encontrado: título (Producto ID: {product_id})")
            return None
        
        title = title_elem.text.strip()
        raw_link = title_elem.get("href", "")
        
        if not raw_link:
            logger.warning(f"Enlace no encontrado para: {title}")
            return None
        
        # Limpiamos la URL (resolvemos tracking)
        clean_link = clean_product_url(raw_link)
        
        if not clean_link:
            logger.warning(f"No se pudo obtener un enlace válido para: {title}")
            return None
        
        # Extraemos los precios
        # Precio actual
        price_current_elem = (
            card.select_one("div.poly-component__price div.poly-price__current span.andes-money-amount__fraction") or
            card.select_one("span.andes-money-amount__fraction") or
            card.select_one("span.price-tag-fraction")
        )
        
        if not price_current_elem:
            logger.warning(f"Selector no encontrado: precio_actual (Producto: {title})")
            return None
        
        try:
            current_price = int(price_current_elem.text.replace(".", "").replace(",", ""))
        except ValueError:
            logger.warning(f"No se pudo convertir el precio actual a entero: {price_current_elem.text} (Producto: {title})")
            return None
        
        # Precio original
        price_original_elem = (
            card.select_one("span.andes-money-amount__fraction") or
            card.select_one("span.ui-search-price__original-value span.andes-money-amount__fraction") or
            card.select_one("span.price-tag-original-value")
        )
        
        if not price_original_elem:
            logger.warning(f"Selector no encontrado: precio_original (Producto: {title})")
            return None
        
        try:
            original_price = int(price_original_elem.text.replace(".", "").replace(",", ""))
        except ValueError:
            logger.warning(f"No se pudo convertir el precio original a entero: {price_original_elem.text} (Producto: {title})")
            return None
        
        # Verificamos que sea una oferta real
        if original_price <= current_price:
            logger.debug(f"No es una oferta real: {title} ({current_price} >= {original_price})")
            return None
        
        # Calculamos el descuento
        discount = round(((original_price - current_price) / original_price) * 100)
        
        # Solo consideramos ofertas con descuento >= 30%
        if discount < 30:
            logger.debug(f"Descuento insuficiente: {title} ({discount}%)")
            return None
        
        # Extraemos la imagen
        img_elem = (
            card.select_one("div.poly-card__portada img.poly-component__picture") or
            card.select_one("img.ui-search-result-image__element") or
            card.select_one("img[class*='ui-search-result-image']")
        )
        
        # Obtenemos la URL real de la imagen (evitando placeholders)
        image_url = get_real_image_url(img_elem)
        
        if not image_url:
            logger.warning(f"No se pudo obtener una imagen válida para: {title}")
            return None
        
        # Extraemos la cantidad de vendidos
        sold = 0
        sold_elem = (
            card.select_one("span.poly-component__sold") or
            card.select_one("span.ui-search-item__group__element") or
            card.select_one("span[class*='ui-search-item__highlight-label']")
        )
        
        if sold_elem and ("vendido" in sold_elem.text.lower() or "vendidos" in sold_elem.text.lower()):
            sold = extract_number(sold_elem.text)
        else:
            logger.info(f"Información de vendidos no encontrada para: {title}. Usando 0.")
        
        # Creamos y devolvemos el objeto de oferta
        return {
            "titulo": title,
            "precio_actual": current_price,
            "precio_original": original_price,
            "descuento": discount,
            "enlace": clean_link,
            "vendidos": sold,
            "imagen": image_url,
            "timestamp": datetime.now().isoformat()
        }
    
    except Exception as e:
        logger.error(f"Error extrayendo producto {product_id}: {str(e)}")
        # Guardamos el HTML para debug si hay un error
        try:
            debug_dir = "scraper/debug"
            if not os.path.exists(debug_dir):
                os.makedirs(debug_dir)
            with open(f"{debug_dir}/debug_product_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html", "w", encoding="utf-8") as f:
                f.write(str(card))
        except Exception as debug_error:
            logger.error(f"Error guardando HTML de debug: {str(debug_error)}")
        return None

def scrape_mercado_libre():
    """Función principal para scrapear Mercado Libre con manejo robusto de errores"""
    logger.info("Iniciando scraping de Mercado Libre...")
    
    # Cargamos el historial de ofertas publicadas
    published_offers = load_published_offers()
    logger.info(f"Historial de ofertas cargado: {len(published_offers)} ofertas publicadas anteriormente")
    
    # URLs de ofertas de tecnología (con filtros de descuento)
    urls = [
        "https://www.mercadolibre.com.mx/ofertas?category=MLM1144",  # Videojuegos
        "https://www.mercadolibre.com.mx/ofertas?category=MLM1000",  # Electrónica
        "https://www.mercadolibre.com.mx/ofertas?category=MLM1648",  # Computación
        "https://www.mercadolibre.com.mx/ofertas?filter=discount_over_30"  # Ofertas con +30% descuento
    ]
    
    # Lista para almacenar todas las ofertas válidas
    valid_offers = []
    total_products_analyzed = 0
    
    try:
        for url in urls:
            logger.info(f"Scrapeando URL: {url}")
            
            # Realizamos la petición con reintentos
            response = None
            for attempt in range(3):
                try:
                    headers = get_random_headers()
                    response = requests.get(url, headers=headers, timeout=15)
                    response.raise_for_status()
                    break
                except (requests.RequestException, Exception) as e:
                    logger.error(f"Error en intento {attempt+1}/3: {str(e)}")
                    if attempt == 2:  # Si es el último intento
                        logger.error(f"Fallaron todos los intentos para {url}. Pasando a la siguiente URL.")
                        break
                    time.sleep(random.uniform(2, 5))
            
            if not response:
                continue
            
            # Parseamos el HTML
            soup = BeautifulSoup(response.text, "lxml")
            
            # Buscamos el contenedor principal con la nueva estructura
            main_container = soup.select_one("div.items-with-smart-groups")
            
            if not main_container:
                logger.warning(f"No se encontró el contenedor principal en {url}. Probando selectores alternativos.")
                # Intentamos con selectores alternativos
                main_container = soup.select_one("div.ui-search-results") or soup.select_one("section.items_container")
                
                if not main_container:
                    logger.error(f"No se pudo encontrar ningún contenedor de productos en {url}")
                    continue
            
            # Buscamos las tarjetas de productos con la nueva estructura
            product_cards = main_container.select("div.poly-card") or main_container.select("li.ui-search-layout__item")
            
            if not product_cards:
                logger.warning(f"No se encontraron tarjetas de productos en {url}. Probando selectores alternativos.")
                # Intentamos con selectores alternativos
                product_cards = main_container.select("div[class*='promotion-item']") or main_container.select("div[class*='ui-search-result']")
                
                if not product_cards:
                    logger.error(f"No se pudo encontrar ninguna tarjeta de producto en {url}")
                    continue
            
            logger.info(f"Encontrados {len(product_cards)} productos en {url}")
            total_products_analyzed += len(product_cards)
            
            # Procesamos cada tarjeta de producto
            for i, card in enumerate(product_cards):
                product_id = f"{url.split('?')[0].split('/')[-1]}_{i}"
                
                # Extraemos los datos de la oferta
                offer = extract_offer(card, product_id)
                
                if not offer:
                    continue
                
                # Verificamos si la oferta ya ha sido publicada en el último mes
                is_published, _ = is_offer_already_published(offer, published_offers)
                
                if is_published:
                    logger.info(f"Oferta ya publicada en el último mes, saltando: {offer['titulo']}")
                    continue
                
                # Añadimos la oferta a la lista de ofertas válidas
                valid_offers.append(offer)
            
            # Esperamos un tiempo aleatorio entre peticiones para evitar bloqueos
            time.sleep(random.uniform(2, 5))
        
        # Resumen de la ejecución
        logger.info(f"Análisis completado: {total_products_analyzed} productos analizados, {len(valid_offers)} ofertas válidas encontradas")
        
        if not valid_offers:
            logger.warning("❌ No se encontraron ofertas válidas que no hayan sido publicadas anteriormente")
            # Creamos un archivo vacío para evitar errores en el bot
            with open("scraper/ofertas.json", "w", encoding="utf-8") as f:
                json.dump([], f)
            return
        
        # Ordenamos las ofertas por score (descuento * vendidos)
        scored_offers = []
        for offer in valid_offers:
            discount = offer["descuento"]
            sold = offer["vendidos"]
            
            # Fórmula de scoring: descuento * (vendidos + 1)
            # Con bonificaciones para ofertas con alto descuento y muchas ventas
            discount_multiplier = 1.5 if discount >= 50 else 1.0
            sales_multiplier = 1.5 if sold >= 100 else (1.2 if sold >= 50 else 1.0)
            
            score = discount * discount_multiplier * (sold + 1) * sales_multiplier
            scored_offers.append((offer, score))
        
        # Ordenamos por score de mayor a menor
        scored_offers.sort(key=lambda x: x[1], reverse=True)
        
        # Tomamos la mejor oferta
        best_offer = scored_offers[0][0]
        best_score = scored_offers[0][1]
        
        logger.info(f"Mejor oferta seleccionada: {best_offer['titulo']} - Descuento: {best_offer['descuento']}% - Score: {best_score}")
        
        # Verificación final de la oferta
        if "click1.mercadolibre.com.mx" in best_offer["enlace"] or "click.mercadolibre.com.mx" in best_offer["enlace"]:
            logger.warning(f"La mejor oferta tiene una URL de tracking no resuelta: {best_offer['enlace']}")
            best_offer["enlace"] = clean_product_url(best_offer["enlace"])
        
        if not best_offer["imagen"] or best_offer["imagen"].startswith("data:image"):
            logger.warning(f"La mejor oferta tiene una imagen placeholder: {best_offer['imagen']}")
            # Buscamos la siguiente mejor oferta
            if len(scored_offers) > 1:
                best_offer = scored_offers[1][0]
                best_score = scored_offers[1][1]
                logger.info(f"Usando oferta alternativa: {best_offer['titulo']} - Score: {best_score}")
            else:
                # No hay ofertas alternativas
                with open("scraper/ofertas.json", "w", encoding="utf-8") as f:
                    json.dump([], f)
                logger.warning("No se guardó ninguna oferta debido a imágenes inválidas")
                return
        
        # Guardamos la mejor oferta en el archivo JSON
        with open("scraper/ofertas.json", "w", encoding="utf-8") as f:
            json.dump([best_offer], f, ensure_ascii=False, indent=2)
        logger.info(f"✅ Oferta guardada exitosamente: {best_offer['titulo']} - Descuento: {best_offer['descuento']}%")
        
        # Guardamos la oferta en el historial de publicadas
        save_published_offer(best_offer)
        
        # Llamamos al generador de links de afiliados
        try:
            logger.info("Iniciando generador de links de afiliados...")
            import subprocess
            result = subprocess.run([sys.executable, "scraper/affiliate_generator.py"], 
                                   capture_output=True, text=True, check=False)
            
            if result.returncode == 0:
                logger.info("Generador de links de afiliados completado exitosamente")
            else:
                logger.error(f"Error en el generador de links de afiliados: {result.stderr}")
        except Exception as e:
            logger.error(f"Error ejecutando el generador de links de afiliados: {str(e)}")
    
    except Exception as e:
        logger.error(f"Error general en el scraping: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        # Aseguramos que el archivo exista para evitar errores en el bot
        if not os.path.exists("scraper/ofertas.json"):
            with open("scraper/ofertas.json", "w", encoding="utf-8") as f:
                json.dump([], f)

def run_scraper():
    """Ejecuta el scraper y maneja excepciones"""
    try:
        logger.info("Iniciando ejecución programada del scraper")
        scrape_mercado_libre()
    except Exception as e:
        logger.error(f"Error en la ejecución programada: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())

def main():
    """Función principal que configura la ejecución programada"""
    logger.info("Iniciando el scraper de Mercado Libre")
    
    # Ejecutamos inmediatamente al iniciar
    run_scraper()
    
    # Programamos ejecuciones cada 8 horas (00:00, 08:00, 16:00)
    schedule.every().day.at("00:00").do(run_scraper)
    schedule.every().day.at("08:00").do(run_scraper)
    schedule.every().day.at("16:00").do(run_scraper)
    
    logger.info("Scraper programado para ejecutarse cada 8 horas (00:00, 08:00, 16:00)")
    
    # Bucle principal
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # Verificamos cada minuto
    except KeyboardInterrupt:
        logger.info("Scraper detenido manualmente")
    except Exception as e:
        logger.error(f"Error en el bucle principal: {str(e)}")

if __name__ == "__main__":
    main()