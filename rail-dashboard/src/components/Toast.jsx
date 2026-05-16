import React, { useEffect, useState } from 'react'

const Toast = ({ message, type = 'info', duration = 5000 }) => {
  const [isVisible, setIsVisible] = useState(true)

  useEffect(() => {
    const timer = setTimeout(() => setIsVisible(false), duration)
    return () => clearTimeout(timer)
  }, [duration])

  if (!isVisible) return null

  const icons = {
    success: '✅',
    error: '❌',
    warning: '⚠️',
    info: 'ℹ️'
  }

  return (
    <div className={`toast toast-${type}`} data-testid={`toast-${type}`}>
      <span className="toast-icon" data-testid="toast-icon">{icons[type]}</span>
      <span className="toast-message" data-testid="toast-message">{message}</span>
      <button 
        className="toast-close" 
        onClick={() => setIsVisible(false)}
        data-testid="toast-close"
      >
        ×
      </button>
    </div>
  )
}

export default Toast