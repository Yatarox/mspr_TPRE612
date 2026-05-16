// rail-dashboard/cypress/support/e2e.js
// Ce fichier est chargé automatiquement avant chaque test

// Supprimer les logs XHR pour une console plus propre
const app = window.top;
if (!app.document.head.querySelector('[data-hide-command-log-request]')) {
  const style = app.document.createElement('style');
  style.innerHTML = '.command-name-request, .command-name-xhr { display: none }';
  style.setAttribute('data-hide-command-log-request', '');
  app.document.head.appendChild(style);
}

// Gestion des erreurs non capturées
Cypress.on('uncaught:exception', (err, runnable) => {
  console.log('⚠️ Uncaught exception:', err.message);
  return false;
});

// ============================================
// COMMANDES PERSONNALISÉES - AJOUTEZ CECI !
// ============================================

// Commande pour attendre le chargement du dashboard
Cypress.Commands.add('waitForDashboardLoad', () => {
  cy.visit('/');
  cy.get('[data-testid="dashboard-content"]', { timeout: 15000 }).should('be.visible');
});

// Commande pour attendre le dashboard (alias)
Cypress.Commands.add('waitForDashboard', () => {
  cy.get('[data-testid="dashboard-content"]', { timeout: 15000 }).should('be.visible');
});

// Commande pour la recherche
Cypress.Commands.add('searchTrips', (origin, destination) => {
  if (origin) cy.get('[data-testid="origin-input"]').type(origin);
  if (destination) cy.get('[data-testid="destination-input"]').type(destination);
  cy.get('[data-testid="search-button"]').click();
  cy.get('[data-testid="search-results"]', { timeout: 10000 }).should('be.visible');
});

// Commande pour mock API
Cypress.Commands.add('mockApiResponse', (endpoint, response) => {
  cy.intercept('GET', `**/api${endpoint}`, {
    statusCode: 200,
    body: response,
  }).as(`mock${endpoint.replace(/\//g, '')}`);
});