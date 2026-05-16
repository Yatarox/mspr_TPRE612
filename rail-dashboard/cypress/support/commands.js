// Custom commands for dashboard testing

Cypress.Commands.add('login', (email, password) => {
  cy.session([email, password], () => {
    cy.visit('/login')
    cy.get('[data-testid="email-input"]').type(email)
    cy.get('[data-testid="password-input"]').type(password)
    cy.get('[data-testid="login-button"]').click()
    cy.url().should('include', '/dashboard')
  })
})

Cypress.Commands.add('waitForDashboardLoad', () => {
  cy.get('[data-testid="dashboard-loading"]', { timeout: 15000 }).should('not.exist')
  cy.get('[data-testid="dashboard-content"]').should('be.visible')
})

Cypress.Commands.add('mockApiResponse', (endpoint, response, method = 'GET') => {
  cy.intercept(method, `**/api${endpoint}`, {
    statusCode: 200,
    body: response,
  }).as(`mock${endpoint.replace(/\//g, '')}`)
})

Cypress.Commands.add('mockApiError', (endpoint, statusCode = 500, errorMessage = 'Server Error') => {
  cy.intercept('GET', `**/api${endpoint}`, {
    statusCode: statusCode,
    body: { detail: errorMessage },
  }).as(`error${endpoint.replace(/\//g, '')}`)
})

Cypress.Commands.add('searchTrips', (origin, destination, options = {}) => {
  if (origin) cy.get('[data-testid="origin-input"]').clear().type(origin)
  if (destination) cy.get('[data-testid="destination-input"]').clear().type(destination)
  
  if (options.trainType) {
    cy.get('[data-testid="train-type-select"]').select(options.trainType)
  }
  
  if (options.minDistance) {
    cy.get('[data-testid="min-distance-input"]').clear().type(options.minDistance)
  }
  
  if (options.maxDistance) {
    cy.get('[data-testid="max-distance-input"]').clear().type(options.maxDistance)
  }
  
  cy.get('[data-testid="search-button"]').click()
  cy.get('[data-testid="search-results"]', { timeout: 10000 }).should('be.visible')
})