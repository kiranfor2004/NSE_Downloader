import { create } from 'zustand'
import { devtools } from 'zustand/middleware'

// Types
interface AppState {
  // Loading states
  isLoading: boolean
  
  // Filter states
  filters: {
    dateRange: 'daily' | 'weekly' | 'monthly'
    category: string | null
    index: string | null
    symbol: string | null
    sortBy: 'symbol' | 'delivery_percentage' | 'volume' | 'value'
    sortOrder: 'asc' | 'desc'
  }
  
  // Modal states
  modals: {
    symbolDetail: boolean
    categoryDetail: boolean
    indexDetail: boolean
  }
  
  // Active data
  activeData: {
    symbol: string | null
    category: string | null
    index: string | null
    data: any[]
  }
  
  // View state
  view: {
    activeTab: 'overview' | 'symbols' | 'categories' | 'indices' | 'analytics'
    darkMode: boolean
  }
}

interface AppActions {
  // Loading actions
  setLoading: (loading: boolean) => void
  
  // Filter actions
  setFilter: (key: keyof AppState['filters'], value: any) => void
  resetFilters: () => void
  
  // Modal actions
  openModal: (modal: keyof AppState['modals']) => void
  closeModal: (modal: keyof AppState['modals']) => void
  closeAllModals: () => void
  
  // Active data actions
  setActiveSymbol: (symbol: string | null, data?: any[]) => void
  setActiveCategory: (category: string | null, data?: any[]) => void
  setActiveIndex: (index: string | null, data?: any[]) => void
  clearActiveData: () => void
  
  // View actions
  setActiveTab: (tab: AppState['view']['activeTab']) => void
  toggleDarkMode: () => void
}

type AppStore = AppState & AppActions

// Initial state
const initialState: AppState = {
  isLoading: false,
  
  filters: {
    dateRange: 'daily',
    category: null,
    index: null,
    symbol: null,
    sortBy: 'delivery_percentage',
    sortOrder: 'desc',
  },
  
  modals: {
    symbolDetail: false,
    categoryDetail: false,
    indexDetail: false,
  },
  
  activeData: {
    symbol: null,
    category: null,
    index: null,
    data: [],
  },
  
  view: {
    activeTab: 'overview',
    darkMode: true,
  },
}

// Create store
export const useAppStore = create<AppStore>()(
  devtools(
    (set, get) => ({
      ...initialState,
      
      // Loading actions
      setLoading: (loading: boolean) => {
        set({ isLoading: loading }, false, 'setLoading')
      },
      
      // Filter actions
      setFilter: (key, value) => {
        set(
          (state) => ({
            filters: {
              ...state.filters,
              [key]: value,
            },
          }),
          false,
          `setFilter:${key}`
        )
      },
      
      resetFilters: () => {
        set(
          {
            filters: {
              ...initialState.filters,
            },
          },
          false,
          'resetFilters'
        )
      },
      
      // Modal actions
      openModal: (modal) => {
        set(
          (state) => ({
            modals: {
              ...state.modals,
              [modal]: true,
            },
          }),
          false,
          `openModal:${modal}`
        )
      },
      
      closeModal: (modal) => {
        set(
          (state) => ({
            modals: {
              ...state.modals,
              [modal]: false,
            },
          }),
          false,
          `closeModal:${modal}`
        )
      },
      
      closeAllModals: () => {
        set(
          {
            modals: {
              symbolDetail: false,
              categoryDetail: false,
              indexDetail: false,
            },
          },
          false,
          'closeAllModals'
        )
      },
      
      // Active data actions
      setActiveSymbol: (symbol, data = []) => {
        set(
          {
            activeData: {
              ...get().activeData,
              symbol,
              data,
            },
          },
          false,
          'setActiveSymbol'
        )
      },
      
      setActiveCategory: (category, data = []) => {
        set(
          {
            activeData: {
              ...get().activeData,
              category,
              data,
            },
          },
          false,
          'setActiveCategory'
        )
      },
      
      setActiveIndex: (index, data = []) => {
        set(
          {
            activeData: {
              ...get().activeData,
              index,
              data,
            },
          },
          false,
          'setActiveIndex'
        )
      },
      
      clearActiveData: () => {
        set(
          {
            activeData: {
              symbol: null,
              category: null,
              index: null,
              data: [],
            },
          },
          false,
          'clearActiveData'
        )
      },
      
      // View actions
      setActiveTab: (activeTab) => {
        set(
          (state) => ({
            view: {
              ...state.view,
              activeTab,
            },
          }),
          false,
          'setActiveTab'
        )
      },
      
      toggleDarkMode: () => {
        set(
          (state) => ({
            view: {
              ...state.view,
              darkMode: !state.view.darkMode,
            },
          }),
          false,
          'toggleDarkMode'
        )
      },
    }),
    {
      name: 'nse-dashboard-store',
    }
  )
)

// Selectors for better performance
export const useFilters = () => useAppStore((state) => state.filters)
export const useModals = () => useAppStore((state) => state.modals)
export const useActiveData = () => useAppStore((state) => state.activeData)
export const useView = () => useAppStore((state) => state.view)
export const useIsLoading = () => useAppStore((state) => state.isLoading)