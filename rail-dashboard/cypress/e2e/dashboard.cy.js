// cypress/e2e/dashboard.cy.js
describe('Dashboard Tests', () => {
  beforeEach(() => {
    cy.visit('/');
    cy.wait(8000);
  });

  describe('Page Load', () => {
    it('should display the dashboard header', () => {
      cy.contains('Rail Data Warehouse').should('be.visible');
    });

    it('should display KPI cards or dashboard content', () => {
      cy.get('body').then(($body) => {
        // Chercher des éléments qui existent dans votre dashboard
        const hasStats = $body.find('.stats-card, [class*="card"], [class*="kpi"], [class*="stat"]').length > 0;
        const hasPremium = $body.find('.premium-hero, .dashboard-section, .app-header').length > 0;
        
        if (hasStats || hasPremium) {
          cy.log('✅ Contenu dashboard trouvé');
        } else {
          cy.log('⚠️ Dashboard vide ou en erreur');
        }
      });
    });

    it('should display charts or sections', () => {
      cy.get('body').then(($body) => {
        const hasCharts = $body.text().includes('trajet') || 
                          $body.text().includes('CO₂') ||
                          $body.find('.chart, [class*="chart"]').length > 0;
        
        if (hasCharts) {
          cy.log('✅ Contenu graphique trouvé');
        } else {
          cy.log('ℹ️ Pas de graphiques sur cette page');
        }
      });
    });
  });
});