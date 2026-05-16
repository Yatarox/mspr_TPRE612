/// <reference types="cypress" />

describe('API Tests', () => {
  it('L\'API doit répondre', () => {
    cy.request('http://api:8000/health').then((response) => {
      expect(response.status).to.eq(200);
      cy.log('✅ API accessible');
    });
  });

  it('L\'API doit retourner les statistiques', () => {
    cy.request('http://api:8000/api/stats/overview').then((response) => {
      expect(response.status).to.eq(200);
      expect(response.body).to.have.property('total_trips');
      cy.log('✅ Statistiques API OK');
    });
  });
});