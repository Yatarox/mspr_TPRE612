// cypress/e2e/debug.cy.js
describe('Debug - Voir la structure réelle', () => {
  it('Affiche le HTML et les classes', () => {
    cy.visit('/');
    cy.wait(5000);
    
    // Affiche le texte de la page
    cy.get('body').then(($body) => {
      cy.log('=== TEXTE DE LA PAGE ===');
      cy.log($body.text().substring(0, 500));
    });
    
    // Affiche toutes les classes présentes
    cy.get('[class]').then(($elements) => {
      const classes = new Set();
      $elements.each((i, el) => {
        el.className.split(' ').forEach(c => classes.add(c));
      });
      cy.log('=== CLASSES TROUVÉES ===');
      cy.log(Array.from(classes).slice(0, 20).join(', '));
    });
    
    // Vérifie si le dashboard est en erreur
    cy.get('body').then(($body) => {
      if ($body.text().includes('Impossible de charger')) {
        cy.log('⚠️ Dashboard en mode erreur');
        cy.log('Attendre que l\'API soit prête...');
      } else {
        cy.log('✅ Dashboard semble chargé');
      }
    });
  });
});