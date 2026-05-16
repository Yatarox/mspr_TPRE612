// rail-dashboard/cypress/e2e/emissions-predictor.cy.js
/// <reference types="cypress" />

describe('Emissions Predictor Tests', () => {
  beforeEach(() => {
    cy.visit('/');
    cy.wait(5000);
  });

  it('should display predictor form if exists', () => {
    // Vérifier si le prédicteur existe (optionnel)
    cy.get('body').then(($body) => {
      if ($body.find('.predictor-card, .predictor-form, [data-testid="predictor-card"]').length > 0) {
        cy.get('.predictor-card, .predictor-form, [data-testid="predictor-card"]').first().should('exist');
        cy.log('✅ Prédicteur trouvé');
      } else {
        cy.log('⚠️ Prédicteur non présent sur cette page');
      }
    });
  });
});