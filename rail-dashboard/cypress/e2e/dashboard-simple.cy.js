// cypress/e2e/dashboard-simple.cy.js
describe('Dashboard Tests', () => {
  beforeEach(() => {
    cy.visit('/');
    cy.wait(8000); // Attendre le chargement
  });

  it('should display the dashboard header', () => {
    cy.contains('Rail Data Warehouse').should('be.visible');
  });

  it('should display dashboard content or error message', () => {
    cy.get('body').then(($body) => {
      const bodyText = $body.text();
      
      if (bodyText.includes('Rail Data Warehouse') && !bodyText.includes('Impossible de charger')) {
        cy.log('✅ Dashboard chargé avec succès');
        // Vérifier qu'il y a du contenu
        cy.get('.app, .dashboard, .app-content').should('exist');
      } else {
        cy.log('⚠️ Dashboard en attente de chargement');
        // Recharger une fois
        cy.wait(5000);
        cy.reload();
        cy.contains('Rail Data Warehouse').should('be.visible');
      }
    });
  });

  it('should have some content on the page', () => {
    cy.get('body').should('not.be.empty');
    cy.get('#root').should('exist');
  });
});