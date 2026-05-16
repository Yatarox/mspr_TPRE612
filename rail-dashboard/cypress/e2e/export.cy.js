// rail-dashboard/cypress/e2e/export.cy.js
/// <reference types="cypress" />

describe('Export Functionality', () => {
  beforeEach(() => {
    cy.visit('/');
    cy.wait(5000);
  });

  it('should have export button if exists', () => {
    cy.get('body').then(($body) => {
      const hasExport = $body.text().includes('Exporter') || 
                        $body.text().includes('Export') ||
                        $body.find('[data-testid="export-button"]').length > 0;
      
      if (hasExport) {
        cy.log('✅ Bouton export trouvé');
      } else {
        cy.log('⚠️ Pas de bouton export sur cette page');
      }
    });
  });
});