const CACHE_NAME = 'ro-experience-v1.2';
const urlsToCache = [
  '/',
  '/static/js/libs/bootstrap/bootstrap.min.js',
  '/static/js/libs/bootstrap/bootstrap.min.css',
  '/static/images/logo.png',
  '/static/images/logo02.png',
  '/static/images/icon-192x192.png'
];

// Instalação - Cache dos recursos essenciais
self.addEventListener('install', function(event) {
  console.log('🚀 Service Worker instalando...');
  event.waitUntil(
    caches.open(CACHE_NAME)
      .then(function(cache) {
        console.log('📦 Cache aberto');
        return cache.addAll(urlsToCache);
      })
  );
});

// Intercepta requisições
self.addEventListener('fetch', function(event) {
  event.respondWith(
    caches.match(event.request)
      .then(function(response) {
        // Retorna do cache ou faz requisição
        if (response) {
          return response;
        }
        return fetch(event.request);
      })
  );
});

// Atualização do Service Worker
self.addEventListener('activate', function(event) {
  console.log('🔄 Service Worker ativado');
  event.waitUntil(
    caches.keys().then(function(cacheNames) {
      return Promise.all(
        cacheNames.map(function(cacheName) {
          if (cacheName !== CACHE_NAME) {
            console.log('🗑️ Removendo cache antigo:', cacheName);
            return caches.delete(cacheName);
          }
        })
      );
    })
  );
});