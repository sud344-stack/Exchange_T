use crate::models::Order;
use crate::AppState;
use tokio::time::{interval, Duration};
use tracing::{error, info};

pub struct MatchingEngine {
    state: AppState,
}

impl MatchingEngine {
    pub fn new(state: AppState) -> Self {
        Self { state }
    }

    pub async fn run(&self) {
        let mut interval = interval(Duration::from_millis(500));

        loop {
            interval.tick().await;

            if let Err(e) = self.match_orders().await {
                error!("Error matching orders: {}", e);
            }
        }
    }

    async fn match_orders(&self) -> Result<(), sqlx::Error> {
        let orders: Vec<Order> = sqlx::query_as::<_, Order>(
            r#"
            SELECT id, user_id, asset, side, type as order_type, price, quantity, status, created_at, updated_at
            FROM orders
            WHERE status = 'OPEN'
            "#
        )
        .fetch_all(&*self.state.db)
        .await?;

        for order in orders {
            let symbol = format!("{}USDT", order.asset);

            if let Some(current_price_ref) = self.state.market_data.prices.get(&symbol) {
                let current_price = *current_price_ref;
                let mut should_execute = false;

                if order.order_type == "MARKET" {
                    should_execute = true;
                } else if order.order_type == "LIMIT" {
                    if order.side == "BUY" && current_price <= order.price {
                        should_execute = true;
                    } else if order.side == "SELL" && current_price >= order.price {
                        should_execute = true;
                    }
                }

                if should_execute {
                    self.execute_trade(&order, current_price).await?;
                }
            }
        }

        Ok(())
    }

    async fn execute_trade(&self, order: &Order, execute_price: f64) -> Result<(), sqlx::Error> {
        let mut tx = self.state.db.begin().await?;

        sqlx::query(
            r#"
            UPDATE orders SET status = 'FILLED', updated_at = NOW() WHERE id = $1
            "#
        )
        .bind(order.id)
        .execute(&mut *tx)
        .await?;

        let quantity_change = if order.side == "BUY" {
            order.quantity
        } else {
            -order.quantity
        };

        sqlx::query(
            r#"
            INSERT INTO portfolios (id, user_id, asset, balance)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (user_id, asset)
            DO UPDATE SET balance = portfolios.balance + EXCLUDED.balance, updated_at = NOW()
            "#
        )
        .bind(uuid::Uuid::new_v4())
        .bind(order.user_id)
        .bind(&order.asset)
        .bind(quantity_change as f64)
        .execute(&mut *tx)
        .await?;

        let usdt_change = if order.side == "BUY" {
            -(execute_price * order.quantity)
        } else {
            execute_price * order.quantity
        };

        sqlx::query(
            r#"
            INSERT INTO portfolios (id, user_id, asset, balance)
            VALUES ($1, $2, 'USDT', $3)
            ON CONFLICT (user_id, asset)
            DO UPDATE SET balance = portfolios.balance + EXCLUDED.balance, updated_at = NOW()
            "#
        )
        .bind(uuid::Uuid::new_v4())
        .bind(order.user_id)
        .bind(usdt_change as f64)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        info!("Executed order {} for user {} at price {}", order.id, order.user_id, execute_price);

        Ok(())
    }
}
