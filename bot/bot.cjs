const fs = require('fs');
const path = require('path');
const axios = require('axios');
const cron = require('node-cron');
const dotenv = require('dotenv');
const FormData = require('form-data');
const chokidar = require('chokidar');


dotenv.config(( { path: path.join(__dirname, '../env/.env') } ));

const FACEBOOK_ACCESS_TOKEN = process.env.FACEBOOK_ACCESS_TOKEN;
const FACEBOOK_PAGE_ID = process.env.FACEBOOK_PAGE_ID;
const HISTORY_FILE = path.join(__dirname, 'published_offers.json'); // Ofertas ya publicadas

if (!FACEBOOK_ACCESS_TOKEN || !FACEBOOK_PAGE_ID) {
  console.error('Error: FACEBOOK_ACCESS_TOKEN and FACEBOOK_PAGE_ID must be set in .env file');
  process.exit(1);
}

//  log
function log(message, type = 'info') {
  const timestamp = new Date().toISOString();
  const logMessage = `[${timestamp}] [${type.toUpperCase()}] ${message}`;
  console.log(logMessage);
  fs.appendFileSync(path.join(__dirname, 'bot.log'), logMessage + '\n', 'utf8');
}

/**
 * Carga el historial de ofertas publicadas
 * @returns {Object} Objeto con IDs de ofertas publicadas como claves
 */
function loadPublishedHistory() {
  try {
    // Usamos directamente HISTORY_FILE que ya es una ruta absoluta
    if (!fs.existsSync(HISTORY_FILE)) {
      fs.writeFileSync(HISTORY_FILE, JSON.stringify({}, null, 2), 'utf8');
      return {};
    }
    
    const data = fs.readFileSync(HISTORY_FILE, 'utf8');
    return JSON.parse(data);
  } catch (error) {
    log(`Error loading published history: ${error.message}`, 'error');
    return {};
  }
}

/**
 * Guarda una oferta en el historial de publicaciones
 * @param {Object} offer La oferta a guardar en el historial
 * @param {String} postId El ID de la publicación en Facebook
 */
function saveToPublishedHistory(offer, postId) {
  try {
    // Usamos directamente HISTORY_FILE que ya es una ruta absoluta
    const history = loadPublishedHistory();
    
    // Id
    const offerId = generateOfferId(offer);
    
    // Oferta en historial
    history[offerId] = {
      title: offer.titulo,
      price: offer.precio_actual,
      link: offer.enlace,
      publishedAt: new Date().toISOString(),
      facebookPostId: postId
    };
    
    // Escribimos el historial actualizado
    fs.writeFileSync(HISTORY_FILE, JSON.stringify(history, null, 2), 'utf8');
    log(`Oferta guardada en historial: ${offer.titulo}`);
  } catch (error) {
    log(`Error saving to published history: ${error.message}`, 'error');
  }
}

/**
 * Genera un ID único para una oferta basado en su contenido
 * @param {Object} offer La oferta
 * @returns {String} ID único para la oferta
 */
function generateOfferId(offer) {
  // Combinamos título y enlace para crear un ID único
  // Esto ayuda a identificar ofertas duplicadas incluso si tienen pequeñas diferencias
  const baseString = `${offer.titulo.toLowerCase()}_${offer.enlace}`;
  
  // Método simple de hashing para generar un ID corto
  let hash = 0;
  for (let i = 0; i < baseString.length; i++) {
    const char = baseString.charCodeAt(i);
    hash = ((hash << 5) - hash) + char;
    hash = hash & hash; // Convertir a entero de 32 bits
  }
  return `offer_${Math.abs(hash).toString(16)}`;
}

/**
 * Verifica si una oferta ya ha sido publicada
 * @param {Object} offer La oferta a verificar
 * @returns {Boolean} true si la oferta ya fue publicada
 */
function isOfferAlreadyPublished(offer) {
  const history = loadPublishedHistory();
  const offerId = generateOfferId(offer);
  return !!history[offerId];
}

/**
 * Carga las ofertas del archivo JSON
 * @returns {Array} Array de objetos de ofertas
 */
function loadOffers() {
  try {
    const filePath = path.join(__dirname, '../scraper/ofertas.json');
    if (!fs.existsSync(filePath)) throw new Error(`File not found: ${filePath}`);
    
    // Usamos un bloqueo de archivo para lectura segura
    const fd = fs.openSync(filePath, 'r');
    
    // Leemos el contenido del archivo
    const data = fs.readFileSync(fd, 'utf8');
    fs.closeSync(fd);
    
    const offers = JSON.parse(data);
    
    if (!Array.isArray(offers)) {
      throw new Error('JSON file does not contain an array');
    }
    
    if (offers.length === 0) {
      log('No hay ofertas en el archivo JSON');
      return [];
    }
    
    // Validamos cada oferta
    offers.forEach((offer, index) => {
      const requiredFields = ['titulo', 'precio_actual', 'precio_original', 'descuento', 'enlace', 'vendidos', 'imagen'];
      for (const field of requiredFields) {
        if (!(field in offer)) throw new Error(`Offer at index ${index} is missing required field: ${field}`);
      }
    });
    
    log(`[✔] JSON cargado (${offers.length} ofertas encontradas)`);
    return offers;
  } catch (error) {
    log(`Error loading offers: ${error.message}`, 'error');
    return [];
  }
}

/**
 * Función para descargar y validar imágenes
 */
async function downloadAndValidateImage(imageUrl) {
  try {
    // Primero verificamos la URL con una petición HEAD
    const headResponse = await axios.head(imageUrl, {
      maxRedirects: 5,
      validateStatus: (status) => status >= 200 && status < 400
    });

    // Seguimos redirecciones si es necesario
    const finalUrl = headResponse.request.res.responseUrl || imageUrl;
    
    // Descargamos la imagen con verificación de tipo
    const response = await axios.get(finalUrl, {
      responseType: 'arraybuffer',
      maxContentLength: 4 * 1024 * 1024,
      headers: {
        'Accept': 'image/*'
      }
    });

    // Verificación exhaustiva del tipo de contenido
    const contentType = response.headers['content-type'];
    if (!contentType || !contentType.startsWith('image/')) {
      throw new Error(`URL no devuelve una imagen válida (Content-Type: ${contentType})`);
    }

    // Verificación del tamaño del archivo
    const contentLength = response.headers['content-length'];
    if (contentLength > 4 * 1024 * 1024) {
      throw new Error(`Imagen demasiado grande (${(contentLength / (1024 * 1024)).toFixed(2)} MB)`);
    }

    return {
      buffer: Buffer.from(response.data, 'binary'),
      contentType
    };
  } catch (error) {
    log(`Error al validar imagen (${imageUrl}): ${error.message}`, 'error');
    throw error;
  }
}

/**
 * Función para publicar ofertas en Facebook
 */
async function postOfferToFacebook(offer, attempt = 1) {
  try {
    log(`[✔] Publicando: ${offer.titulo} (Intento ${attempt})...`);
    
    const message = `
🔥 ${offer.titulo} (${offer.descuento}% OFF)
💵 $${offer.precio_actual} (Antes $${offer.precio_original})
🛒 https://www.mercadolibre.com.mx/social/coma20240425175052/lists

Oferta obtenida mediante búsqueda pública
    `.trim();
    
    let imageData;
    try {
      imageData = await downloadAndValidateImage(offer.imagen);
    } catch (imageError) {
      // Intento con imagen alternativa si está disponible
      if (offer.imagen_alternativa) {
        log(`Usando imagen alternativa para ${offer.titulo}`);
        imageData = await downloadAndValidateImage(offer.imagen_alternativa);
      } else {
        throw imageError;
      }
    }

    const formData = new FormData();
    formData.append('source', imageData.buffer, {
      filename: `offer_${Date.now()}.${imageData.contentType.split('/')[1] || 'jpg'}`,
      contentType: imageData.contentType
    });
    formData.append('message', message);

    const response = await axios.post(
      `https://graph.facebook.com/v16.0/${FACEBOOK_PAGE_ID}/photos`,
      formData,
      {
        headers: {
          ...formData.getHeaders(),
          'Authorization': `Bearer ${FACEBOOK_ACCESS_TOKEN}`
        },
        maxContentLength: 10 * 1024 * 1024,
        timeout: 30000
      }
    );

    if (response.data?.id) {
      const postUrl = `https://facebook.com/${response.data.id}`;
      log(`[✔] Post creado exitosamente: ${postUrl}`);
      
      // Guardamos la oferta en el historial
      saveToPublishedHistory(offer, response.data.id);
      
      return response.data.id;
    }
    
    throw new Error('Facebook no devolvió un ID de publicación válido');
  } catch (error) {
    if (attempt < 3) {
      const delay = attempt * 5000; // Retraso exponencial
      log(`Reintentando en ${delay/1000} segundos...`);
      await new Promise(resolve => setTimeout(resolve, delay));
      return postOfferToFacebook(offer, attempt + 1);
    }
    
    let errorDetails = error.message;
    if (error.response) {
      errorDetails = JSON.stringify({
        status: error.response.status,
        data: error.response.data,
        headers: error.response.headers
      });
    }
    
    log(`Error al publicar oferta después de 3 intentos: ${errorDetails}`, 'error');
    return null;
  }
}

/**
 * Limpia el archivo de ofertas después de procesarlas
 */
function clearOffersFile() {
  try {
    const filePath = path.join(__dirname, '../scraper/ofertas.json');
    fs.writeFileSync(filePath, '[]', 'utf8');
    log('Archivo de ofertas limpiado después de procesar');
  } catch (error) {
    log(`Error al limpiar archivo de ofertas: ${error.message}`, 'error');
  }
}

/**
 * Procesa las ofertas nuevas
 */
async function processNewOffers() {
  log('Procesando nuevas ofertas...');
  
  const offers = loadOffers();
  if (offers.length === 0) {
    log('No hay ofertas nuevas para procesar');
    return;
  }
  
  let publishedCount = 0;
  let skippedCount = 0;
  
  for (const offer of offers) {
    try {
      // Verificamos si la oferta ya fue publicada
      if (isOfferAlreadyPublished(offer)) {
        log(`Oferta ya publicada anteriormente, omitiendo: ${offer.titulo}`);
        skippedCount++;
        continue;
      }
      
      // Publicamos la oferta
      const postId = await postOfferToFacebook(offer);
      if (postId) {
        publishedCount++;
      }
      
      // Pequeña pausa entre publicaciones
      await new Promise(resolve => setTimeout(resolve, 5000));
    } catch (error) {
      log(`Error procesando oferta ${offer.titulo}: ${error.message}`, 'error');
    }
  }
  
  log(`Procesamiento completado: ${publishedCount} ofertas publicadas, ${skippedCount} omitidas`);
  
  // Limpiamos el archivo de ofertas después de procesarlas
  clearOffersFile();
}

/**
 * Función principal
 */
async function main() {
  try {
    log('Bot iniciado');
    
    // Procesamos las ofertas existentes al inicio
    await processNewOffers();
    
    // Configuramos el observador de archivos
    const watcher = chokidar.watch(path.join(__dirname, '../scraper/ofertas.json'), {
      persistent: true,
      awaitWriteFinish: {
        stabilityThreshold: 2000,
        pollInterval: 100
      }
    });
    
    log('Observando cambios en ofertas.json...');
    
    // Cuando el archivo cambie, procesamos las nuevas ofertas
    watcher.on('change', async (path) => {
      log(`Cambio detectado en ${path}`);
      await processNewOffers();
    });
    
    // Mantenemos el cron como respaldo, pero con menor frecuencia
    cron.schedule('0 */12 * * *', async () => {
      log('Ejecución programada de respaldo iniciada');
      await processNewOffers();
    });
    
    log('Bot en ejecución. Presiona Ctrl+C para detener.');
  } catch (error) {
    log(`Error no controlado: ${error.message}`, 'error');
    process.exit(1);
  }
}

main();