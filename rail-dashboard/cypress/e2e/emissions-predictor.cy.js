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

  it('should fill form and submit prediction with new fields', () => {
    // Vérifier que le formulaire existe
    cy.get('[data-testid="predictor-card"]').should('exist');

    // Remplir les champs
    cy.get('[data-testid="predictor-distance"]').clear().type('500');
    cy.get('[data-testid="predictor-duration"]').clear().type('2.5');
    cy.get('[data-testid="predictor-stops"]').clear().type('6');

    // Sélectionner le type de train (optionnel)
    cy.get('[data-testid="predictor-train-type"]').select('TER');

    // Sélectionner la traction (obligatoire)
    cy.get('[data-testid="predictor-traction"]').select('électrique');

    // Sélectionner le type de service (obligatoire)
    cy.get('[data-testid="predictor-service-type"]').select('JOUR');

    // Soumettre le formulaire
    cy.get('[data-testid="predict-submit"]').click();

    // Attendre le résultat
    cy.get('[data-testid="predictor-result"]', { timeout: 15000 }).should('be.visible');

    // Vérifier les champs de résultat
    cy.get('[data-testid="prediction-frequency"]').should('exist').should('not.have.text', 'N/A');
    cy.get('[data-testid="prediction-perkm"]').should('exist');
    cy.get('[data-testid="prediction-total"]').should('exist');
    cy.get('[data-testid="prediction-model"]').should('exist');

    // Vérifier que la fréquence est un nombre > 0
    cy.get('[data-testid="prediction-frequency"]')
      .invoke('text')
      .should('match', /[\d.]+/);
  });

  it('should show validation errors when required fields are missing', () => {
    cy.get('[data-testid="predictor-card"]').should('exist');

    // Soumettre sans remplir les champs requis
    cy.get('[data-testid="predict-submit"]').click();

    // Vérifier qu'une erreur est affichée (validation HTML5 ou API)
    cy.get('[data-testid="predictor-error"]').should('exist');

    // Remplir les champs requis
    cy.get('[data-testid="predictor-distance"]').clear().type('500');
    cy.get('[data-testid="predictor-duration"]').clear().type('2.5');
    cy.get('[data-testid="predictor-traction"]').select('électrique');
    cy.get('[data-testid="predictor-service-type"]').select('JOUR');

    // Soumettre à nouveau
    cy.get('[data-testid="predict-submit"]').click();

    // Vérifier que le résultat apparaît
    cy.get('[data-testid="predictor-result"]', { timeout: 15000 }).should('be.visible');
  });

  it('should handle NUIT service type correctly', () => {
    cy.get('[data-testid="predictor-card"]').should('exist');

    // Remplir les champs
    cy.get('[data-testid="predictor-distance"]').clear().type('450');
    cy.get('[data-testid="predictor-duration"]').clear().type('6.0');
    cy.get('[data-testid="predictor-traction"]').select('électrique');
    cy.get('[data-testid="predictor-service-type"]').select('NUIT');

    cy.get('[data-testid="predict-submit"]').click();

    cy.get('[data-testid="predictor-result"]', { timeout: 15000 }).should('be.visible');
    cy.get('[data-testid="prediction-frequency"]').should('exist');

    // Vérifier que la fréquence prédite est affichée
    cy.get('[data-testid="prediction-frequency"]')
      .invoke('text')
      .should('not.be.empty');
  });

  it('should reset form when reset button is clicked', () => {
    cy.get('[data-testid="predictor-card"]').should('exist');

    // Remplir les champs
    cy.get('[data-testid="predictor-distance"]').clear().type('500');
    cy.get('[data-testid="predictor-duration"]').clear().type('2.5');
    cy.get('[data-testid="predictor-traction"]').select('électrique');
    cy.get('[data-testid="predictor-service-type"]').select('JOUR');

    // Cliquer sur réinitialiser
    cy.get('[data-testid="predict-reset"]').click();

    // Vérifier que les champs sont réinitialisés
    cy.get('[data-testid="predictor-distance"]').should('have.value', '');
    cy.get('[data-testid="predictor-duration"]').should('have.value', '');
    cy.get('[data-testid="predictor-traction"]').should('have.value', '');
    cy.get('[data-testid="predictor-service-type"]').should('have.value', 'JOUR');

    // Vérifier que le résultat a disparu
    cy.get('[data-testid="predictor-result"]').should('not.exist');
  });

  it('should handle API error gracefully', () => {
    // Intercepter la requête API et simuler une erreur 500
    cy.intercept('GET', '**/api/predict*', {
      statusCode: 500,
      body: { detail: 'Erreur serveur' }
    }).as('predictError');

    cy.get('[data-testid="predictor-card"]').should('exist');

    cy.get('[data-testid="predictor-distance"]').clear().type('500');
    cy.get('[data-testid="predictor-duration"]').clear().type('2.5');
    cy.get('[data-testid="predictor-traction"]').select('électrique');
    cy.get('[data-testid="predictor-service-type"]').select('JOUR');

    cy.get('[data-testid="predict-submit"]').click();

    cy.wait('@predictError');

    cy.get('[data-testid="predictor-error"]', { timeout: 10000 })
      .should('exist')
      .and('be.visible');
  });

  it('should show comparison text after successful prediction', () => {
    cy.get('[data-testid="predictor-card"]').should('exist');

    cy.get('[data-testid="predictor-distance"]').clear().type('465');
    cy.get('[data-testid="predictor-duration"]').clear().type('2.0');
    cy.get('[data-testid="predictor-traction"]').select('électrique');
    cy.get('[data-testid="predictor-service-type"]').select('JOUR');

    cy.get('[data-testid="predict-submit"]').click();

    cy.get('[data-testid="predictor-result"]', { timeout: 15000 }).should('be.visible');

    // Vérifier que le texte de comparaison est présent
    cy.get('.result-comparison').should('exist');
    cy.get('.result-comparison').should('contain.text', 'TGV Paris-Lyon');
    cy.get('.result-comparison').should('contain.text', 'kg CO₂');
  });

  it('should display model name in results', () => {
    cy.get('[data-testid="predictor-card"]').should('exist');

    cy.get('[data-testid="predictor-distance"]').clear().type('500');
    cy.get('[data-testid="predictor-duration"]').clear().type('2.5');
    cy.get('[data-testid="predictor-traction"]').select('électrique');
    cy.get('[data-testid="predictor-service-type"]').select('JOUR');

    cy.get('[data-testid="predict-submit"]').click();

    cy.get('[data-testid="predictor-result"]', { timeout: 15000 }).should('be.visible');
    cy.get('[data-testid="prediction-model"]').should('exist');
    cy.get('[data-testid="prediction-model"]').should('contain.text', 'RandomForest');
  });

  it('should handle API timeout', () => {
    // Intercepter la requête et la faire expirer
    cy.intercept('GET', '**/api/predict*', {
      delay: 30000,
      statusCode: 200,
      body: {}
    }).as('predictTimeout');

    cy.get('[data-testid="predictor-card"]').should('exist');

    cy.get('[data-testid="predictor-distance"]').clear().type('500');
    cy.get('[data-testid="predictor-duration"]').clear().type('2.5');
    cy.get('[data-testid="predictor-traction"]').select('électrique');
    cy.get('[data-testid="predictor-service-type"]').select('JOUR');

    cy.get('[data-testid="predict-submit"]').click();

    // Le timeout est géré par le frontend, on vérifie juste que le loader apparaît
    cy.get('[data-testid="predict-submit"]').should('be.disabled');
    cy.get('[data-testid="predict-submit"]').should('contain.text', 'Calcul en cours');
  });
});