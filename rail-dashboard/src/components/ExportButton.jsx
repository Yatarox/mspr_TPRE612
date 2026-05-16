import React, { useState } from 'react'
import * as XLSX from 'xlsx'
import { saveAs } from 'file-saver'

const ExportButton = ({ data, fileName = 'export' }) => {
  const [isExporting, setIsExporting] = useState(false)

  const exportToExcel = () => {
    setIsExporting(true)
    try {
      const workbook = XLSX.utils.book_new()
      
      if (data.overview) {
        const overviewSheet = XLSX.utils.json_to_sheet([data.overview])
        XLSX.utils.book_append_sheet(workbook, overviewSheet, 'Overview')
      }
      
      if (data.countryStats?.length) {
        const countrySheet = XLSX.utils.json_to_sheet(data.countryStats)
        XLSX.utils.book_append_sheet(workbook, countrySheet, 'Par Pays')
      }
      
      if (data.trainTypeStats?.length) {
        const trainSheet = XLSX.utils.json_to_sheet(data.trainTypeStats)
        XLSX.utils.book_append_sheet(workbook, trainSheet, 'Par Type de Train')
      }
      
      if (data.tractionStats?.length) {
        const tractionSheet = XLSX.utils.json_to_sheet(data.tractionStats)
        XLSX.utils.book_append_sheet(workbook, tractionSheet, 'Par Traction')
      }

      if (data.agencyStats?.length) {
        const agencySheet = XLSX.utils.json_to_sheet(data.agencyStats)
        XLSX.utils.book_append_sheet(workbook, agencySheet, 'Par Agence')
      }

      if (data.emissionsByRoute?.length) {
        const emissionsSheet = XLSX.utils.json_to_sheet(data.emissionsByRoute)
        XLSX.utils.book_append_sheet(workbook, emissionsSheet, 'Routes Émissives')
      }
      
      const excelBuffer = XLSX.write(workbook, { bookType: 'xlsx', type: 'array' })
      const blob = new Blob([excelBuffer], { type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' })
      saveAs(blob, `${fileName}_${new Date().toISOString().split('T')[0]}.xlsx`)
    } catch (error) {
      console.error('Export failed:', error)
    } finally {
      setIsExporting(false)
    }
  }

  return (
    <div className="export-dropdown" data-testid="export-dropdown">
      <button 
        className="secondary-button" 
        onClick={exportToExcel} 
        disabled={isExporting}
        data-testid="export-button"
      >
        {isExporting ? '⏳ Export...' : '📥 Exporter Excel'}
      </button>
    </div>
  )
}

export default ExportButton