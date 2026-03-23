import React, { createContext, useContext, useEffect, useState } from 'react';

type Prices = Record<string, number>;

interface MarketContextType {
  prices: Prices;
}

const MarketContext = createContext<MarketContextType>({ prices: {} });

export const MarketProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const [prices, setPrices] = useState<Prices>({});

  useEffect(() => {
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsUrl = import.meta.env.DEV
      ? `ws://localhost:3000/ws`
      : `${protocol}//${window.location.host}/ws`;

    const connectWebSocket = () => {
      const ws = new WebSocket(wsUrl);

      ws.onmessage = (event) => {
        try {
          const message = JSON.parse(event.data);
          if (message.type === 'prices') {
            setPrices(message.data);
          }
        } catch (error) {
          console.error('Error parsing market data:', error);
        }
      };

      ws.onclose = () => {
        console.log('WebSocket disconnected. Reconnecting in 3s...');
        setTimeout(connectWebSocket, 3000);
      };

      ws.onerror = (error) => {
        console.error('WebSocket error:', error);
        ws.close();
      };

      return ws;
    };

    const ws = connectWebSocket();

    return () => {
      ws.close();
    };
  }, []);

  return (
    <MarketContext.Provider value={{ prices }}>
      {children}
    </MarketContext.Provider>
  );
};

export const useMarket = () => useContext(MarketContext);
