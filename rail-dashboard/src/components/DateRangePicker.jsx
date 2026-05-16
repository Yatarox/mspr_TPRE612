import React, { useState, useRef, useEffect } from 'react'
import DatePicker from 'react-datepicker'
import 'react-datepicker/dist/react-datepicker.css'

const DateRangePicker = ({ value, onChange }) => {
  const [isOpen, setIsOpen] = useState(false)
  const wrapperRef = useRef(null)

  useEffect(() => {
    const handleClickOutside = (event) => {
      if (wrapperRef.current && !wrapperRef.current.contains(event.target)) {
        setIsOpen(false)
      }
    }
    document.addEventListener('mousedown', handleClickOutside)
    return () => document.removeEventListener('mousedown', handleClickOutside)
  }, [])

  const handleClear = () => {
    onChange({ start: null, end: null })
    setIsOpen(false)
  }

  const getDateLabel = () => {
    if (!value.start && !value.end) return 'Toutes les dates'
    if (value.start && !value.end) return `Depuis le ${value.start.toLocaleDateString('fr-FR')}`
    if (!value.start && value.end) return `Jusqu'au ${value.end.toLocaleDateString('fr-FR')}`
    return `${value.start.toLocaleDateString('fr-FR')} - ${value.end.toLocaleDateString('fr-FR')}`
  }

  return (
    <div className="date-range-picker" ref={wrapperRef} data-testid="date-range-picker">
      <button 
        className="date-range-trigger" 
        onClick={() => setIsOpen(!isOpen)}
        data-testid="date-range-trigger"
      >
        <span className="calendar-icon">📅</span>
        <span data-testid="date-range-label">{getDateLabel()}</span>
        <span className="chevron">{isOpen ? '▲' : '▼'}</span>
      </button>

      {isOpen && (
        <div className="date-range-dropdown" data-testid="date-range-dropdown">
          <div className="date-range-presets" data-testid="date-range-presets">
            <button onClick={() => {
              const today = new Date()
              onChange({ start: today, end: today })
              setIsOpen(false)
            }} data-testid="date-preset-today">Aujourd'hui</button>
            <button onClick={() => {
              const start = new Date()
              start.setDate(start.getDate() - 7)
              onChange({ start, end: new Date() })
              setIsOpen(false)
            }} data-testid="date-preset-7days">7 derniers jours</button>
            <button onClick={() => {
              const start = new Date()
              start.setDate(start.getDate() - 30)
              onChange({ start, end: new Date() })
              setIsOpen(false)
            }} data-testid="date-preset-30days">30 derniers jours</button>
            <button onClick={() => {
              const start = new Date()
              start.setMonth(start.getMonth() - 3)
              onChange({ start, end: new Date() })
              setIsOpen(false)
            }} data-testid="date-preset-3months">3 mois</button>
          </div>
          
          <div className="date-range-custom" data-testid="date-range-custom">
            <DatePicker
              selected={value.start}
              onChange={(date) => onChange({ ...value, start: date })}
              selectsStart
              startDate={value.start}
              endDate={value.end}
              placeholderText="Date début"
              dateFormat="dd/MM/yyyy"
              data-testid="date-start"
            />
            <span>à</span>
            <DatePicker
              selected={value.end}
              onChange={(date) => onChange({ ...value, end: date })}
              selectsEnd
              startDate={value.start}
              endDate={value.end}
              minDate={value.start}
              placeholderText="Date fin"
              dateFormat="dd/MM/yyyy"
              data-testid="date-end"
            />
          </div>
          
          <div className="date-range-actions">
            <button 
              className="secondary-button" 
              onClick={handleClear}
              data-testid="date-clear"
            >
              Effacer
            </button>
            <button 
              className="primary-button" 
              onClick={() => setIsOpen(false)}
              data-testid="date-apply"
            >
              Appliquer
            </button>
          </div>
        </div>
      )}
    </div>
  )
}

export default DateRangePicker