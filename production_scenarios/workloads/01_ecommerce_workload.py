"""
E-Commerce Platform Workload Generator
Simulates realistic e-commerce traffic patterns
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pyrqg.dsl.core import Grammar, choice, template, ref, number, maybe, repeat

# Create e-commerce workload grammar
g = Grammar("ecommerce_workload")

# Main workload distribution (realistic e-commerce traffic)
g.rule("query",
    choice(
        ref("browse_products"),      # 40% - Product browsing
        ref("search_products"),      # 20% - Search queries
        ref("view_product"),         # 15% - Product details
        ref("cart_operations"),      # 10% - Cart management
        ref("checkout_flow"),        # 5%  - Order placement
        ref("account_queries"),      # 5%  - User account
        ref("admin_operations"),     # 5%  - Admin tasks
        weights=[40, 20, 15, 10, 5, 5, 5]
    )
)

# Product browsing patterns
g.rule("browse_products",
    choice(
        # Category browsing
        template("""SELECT p.product_id, p.name, p.price, p.sku,
    COALESCE(AVG(r.rating), 0) as avg_rating,
    COUNT(r.review_id) as review_count
FROM products p
LEFT JOIN reviews r ON p.product_id = r.product_id
WHERE p.category_id = {category_id} AND p.is_active = TRUE
GROUP BY p.product_id
ORDER BY {sort_order}
LIMIT {page_size} OFFSET {offset}"""),
        
        # Featured products
        template("""SELECT p.*, i.quantity - i.reserved_quantity as available
FROM products p
JOIN inventory i ON p.product_id = i.product_id
WHERE p.is_active = TRUE AND i.quantity > i.reserved_quantity
ORDER BY p.created_at DESC
LIMIT 20"""),
        
        # Price range filtering
        template("""SELECT p.product_id, p.name, p.price, c.name as category
FROM products p
JOIN categories c ON p.category_id = c.category_id
WHERE p.price BETWEEN {min_price} AND {max_price}
AND p.is_active = TRUE
ORDER BY p.price ASC
LIMIT 50""")
    )
)

# Search operations
g.rule("search_products",
    choice(
        # Full text search simulation
        template("""SELECT p.*, 
    ts_rank(to_tsvector('english', p.name || ' ' || COALESCE(p.description, '')), 
            plainto_tsquery('english', '{search_term}')) as rank
FROM products p
WHERE to_tsvector('english', p.name || ' ' || COALESCE(p.description, '')) 
    @@ plainto_tsquery('english', '{search_term}')
AND p.is_active = TRUE
ORDER BY rank DESC
LIMIT 20"""),
        
        # Simple LIKE search
        template("""SELECT p.product_id, p.name, p.price, p.sku
FROM products p
WHERE (LOWER(p.name) LIKE LOWER('%{search_term}%') 
    OR LOWER(p.description) LIKE LOWER('%{search_term}%'))
AND p.is_active = TRUE
LIMIT 50""")
    )
)

# Product detail views
g.rule("view_product",
    choice(
        # Product with reviews
        template("""SELECT p.*, 
    (SELECT JSON_AGG(
        JSON_BUILD_OBJECT(
            'rating', r.rating,
            'title', r.title,
            'comment', r.comment,
            'created_at', r.created_at
        ) ORDER BY r.created_at DESC
    ) FROM reviews r WHERE r.product_id = p.product_id LIMIT 10) as recent_reviews,
    (SELECT quantity - reserved_quantity FROM inventory WHERE product_id = p.product_id) as available
FROM products p
WHERE p.product_id = {product_id}"""),
        
        # Related products
        template("""SELECT p2.product_id, p2.name, p2.price
FROM products p1
JOIN products p2 ON p1.category_id = p2.category_id
WHERE p1.product_id = {product_id} 
AND p2.product_id != {product_id}
AND p2.is_active = TRUE
LIMIT 8""")
    )
)

# Cart operations
g.rule("cart_operations",
    choice(
        # Add to cart
        template("""WITH cart_check AS (
    SELECT cart_id FROM carts 
    WHERE customer_id = {customer_id} AND status = 'active'
    LIMIT 1
), cart_insert AS (
    INSERT INTO carts (customer_id) 
    SELECT {customer_id}
    WHERE NOT EXISTS (SELECT 1 FROM cart_check)
    RETURNING cart_id
)
INSERT INTO cart_items (cart_id, product_id, quantity)
SELECT COALESCE(cc.cart_id, ci.cart_id), {product_id}, {quantity}
FROM cart_check cc
FULL OUTER JOIN cart_insert ci ON TRUE
ON CONFLICT (cart_id, product_id) 
DO UPDATE SET quantity = cart_items.quantity + EXCLUDED.quantity"""),
        
        # View cart
        template("""SELECT ci.cart_item_id, p.product_id, p.name, p.price, ci.quantity,
    p.price * ci.quantity as subtotal,
    i.quantity - i.reserved_quantity as available
FROM carts c
JOIN cart_items ci ON c.cart_id = ci.cart_id
JOIN products p ON ci.product_id = p.product_id
LEFT JOIN inventory i ON p.product_id = i.product_id
WHERE c.customer_id = {customer_id} AND c.status = 'active'"""),
        
        # Update cart quantity
        template("""UPDATE cart_items 
SET quantity = {new_quantity}
WHERE cart_item_id = {cart_item_id}
AND cart_id IN (SELECT cart_id FROM carts WHERE customer_id = {customer_id})""")
    )
)

# Checkout flow
g.rule("checkout_flow",
    choice(
        # Create order from cart
        template("""WITH cart_total AS (
    SELECT c.cart_id, 
        SUM(p.price * ci.quantity) as subtotal,
        SUM(p.price * ci.quantity) * 0.08 as tax
    FROM carts c
    JOIN cart_items ci ON c.cart_id = ci.cart_id
    JOIN products p ON ci.product_id = p.product_id
    WHERE c.customer_id = {customer_id} AND c.status = 'active'
    GROUP BY c.cart_id
), new_order AS (
    INSERT INTO orders (customer_id, order_number, total_amount, tax_amount, status)
    SELECT {customer_id}, 
        'ORD-' || TO_CHAR(CURRENT_TIMESTAMP, 'YYYYMMDD') || '-' || NEXTVAL('orders_order_id_seq'),
        ct.subtotal + ct.tax,
        ct.tax,
        'pending'
    FROM cart_total ct
    RETURNING order_id
)
INSERT INTO order_items (order_id, product_id, quantity, unit_price)
SELECT no.order_id, ci.product_id, ci.quantity, p.price
FROM new_order no
CROSS JOIN cart_items ci
JOIN carts c ON ci.cart_id = c.cart_id
JOIN products p ON ci.product_id = p.product_id
WHERE c.customer_id = {customer_id}"""),
        
        # Reserve inventory
        template("""UPDATE inventory i
SET reserved_quantity = reserved_quantity + oi.quantity
FROM order_items oi
WHERE i.product_id = oi.product_id
AND oi.order_id = {order_id}""")
    )
)

# Account queries
g.rule("account_queries",
    choice(
        # Order history
        template("""SELECT o.order_id, o.order_number, o.status, o.total_amount, o.created_at,
    COUNT(oi.order_item_id) as item_count
FROM orders o
LEFT JOIN order_items oi ON o.order_id = oi.order_id
WHERE o.customer_id = {customer_id}
GROUP BY o.order_id
ORDER BY o.created_at DESC
LIMIT 20"""),
        
        # Customer profile
        template("""SELECT c.*, 
    COUNT(DISTINCT o.order_id) as total_orders,
    COALESCE(SUM(o.total_amount), 0) as lifetime_value,
    MAX(o.created_at) as last_order_date
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
WHERE c.customer_id = {customer_id}
GROUP BY c.customer_id""")
    )
)

# Admin operations
g.rule("admin_operations",
    choice(
        # Low stock alert
        template("""SELECT p.product_id, p.name, p.sku, i.quantity, i.reserved_quantity,
    i.reorder_level, (i.quantity - i.reserved_quantity) as available
FROM inventory i
JOIN products p ON i.product_id = p.product_id
WHERE (i.quantity - i.reserved_quantity) < i.reorder_level
AND p.is_active = TRUE
ORDER BY available ASC
LIMIT 50"""),
        
        # Sales analytics
        template("""SELECT DATE_TRUNC('day', o.created_at) as order_date,
    COUNT(DISTINCT o.order_id) as order_count,
    COUNT(DISTINCT o.customer_id) as unique_customers,
    SUM(o.total_amount) as revenue,
    AVG(o.total_amount) as avg_order_value
FROM orders o
WHERE o.created_at >= CURRENT_DATE - INTERVAL '{days} days'
AND o.status != 'cancelled'
GROUP BY DATE_TRUNC('day', o.created_at)
ORDER BY order_date DESC""")
    )
)

# Parameters
g.rule("category_id", number(1, 50))
g.rule("product_id", number(1, 10000))
g.rule("customer_id", number(1, 5000))
g.rule("cart_item_id", number(1, 100000))
g.rule("order_id", number(1, 50000))
g.rule("quantity", choice(1, 1, 1, 2, 2, 3, 4, 5))  # Most orders are 1-2 items
g.rule("page_size", choice(20, 50, 100))
g.rule("offset", choice(0, 20, 40, 60, 80, 100))
g.rule("min_price", choice(0, 10, 25, 50, 100))
g.rule("max_price", choice(50, 100, 250, 500, 1000))
g.rule("new_quantity", number(1, 10))
g.rule("days", choice(7, 30, 90))

# Sort orders
g.rule("sort_order", choice(
    "p.price ASC",
    "p.price DESC", 
    "p.created_at DESC",
    "avg_rating DESC",
    "p.name ASC"
))

# Search terms (common e-commerce searches)
g.rule("search_term", choice(
    "laptop", "phone", "camera", "headphones", "shoes",
    "dress", "watch", "bag", "book", "game",
    "tablet", "monitor", "keyboard", "mouse", "speaker"
))

# Export grammar
grammar = g