1. Webhook queue

-- migrations/004_webhook_queue.sql
CREATE TABLE webhook_queue (
  id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  event_id     TEXT        NOT NULL UNIQUE,
  type         TEXT        NOT NULL,
  payload      JSONB       NOT NULL,
  status       TEXT        NOT NULL DEFAULT 'pending',
  attempts     INTEGER     NOT NULL DEFAULT 0,
  max_attempts INTEGER     NOT NULL DEFAULT 5,
  next_retry   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_error   TEXT,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  processed_at TIMESTAMPTZ
);

CREATE INDEX idx_webhook_queue_status    ON webhook_queue(status, next_retry);
CREATE INDEX idx_webhook_queue_event_id  ON webhook_queue(event_id);

2. Reliable webhook receiver

 src/webhooks.js
const handleWebhook = async (req, res) => {
  const { STRIPE_SECRET_KEY, STRIPE_WEBHOOK_SECRET } = await getSecret(
    'placemux/prod/payments'
  );

  let event;
  try {
    event = stripe(STRIPE_SECRET_KEY).webhooks.constructEvent(
      req.body,
      req.headers['stripe-signature'],
      STRIPE_WEBHOOK_SECRET
    );
  } catch (err) {
    return res.status(400).json({ error: err.message });
  }

  const { rows } = await db.query(
    'SELECT id FROM webhook_queue WHERE event_id=$1',
    [event.id]
  );

  if (rows.length) return res.json({ received: true, duplicate: true });

  await db.query(
  INSERT INTO webhook_queue (event_id, type, payload)
     VALUES ($1,$2,$3),
    [event.id, event.type, JSON.stringify(event.data)]
  );

4. Metrics
 
src/metrics.js
const client = require('prom-client');

const webhookProcessed = new client.Counter({
  name:       'webhook_processed_total',
  labelNames: ['type', 'status']
});

const webhookDuration = new client.Histogram({
  name:       'webhook_duration_seconds',
  labelNames: ['type'],
  buckets:    [0.1, 0.5, 1, 2, 5]
});

const webhookQueueDepth = new client.Gauge({
  name: 'webhook_queue_depth',
  async collect() {
    const { rows } = await db.query(
      SELECT status, COUNT(*) FROM webhook_queue GROUP BY status
    );
    rows.forEach(r => this.set({ status: r.status }, parseInt(r.count)));
  }
});

const paymentDiscrepancies = new client.Gauge({
  name: 'payment_discrepancies_unresolved',
  async collect() {
    const { rows } = await db.query(
      'SELECT COUNT(*) FROM payment_discrepancies WHERE resolved=false'
    );
    this.set(parseInt(rows[0].count));
  }
});

module.exports = { webhookProcessed, webhookDuration, webhookQueueDepth, paymentDiscrepancies };

5. Alerts

# k8s/production/payment-alerts.yaml
apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: payment-alerts
  namespace: production
spec:
  groups:
    - name: payments
      rules:
        - alert: WebhookQueueBacklog
          expr: webhook_queue_depth{status="pending"} > 50
          for: 5m
          labels:
            severity: warning
          annotations:
            summary: "Webhook queue backlog > 50"
        - alert: WebhookHighFailureRate
          expr: |
            rate(webhook_processed_total{status="failed"}[5m]) /
            rate(webhook_processed_total[5m]) > 0.05
          for: 5m
          labels:
            severity: critical
          annotations:
            summary: "Webhook failure rate > 5%"
        - alert: WebhookStuck
          expr: webhook_queue_depth{status="failed"} > 0
          for: 15m
          labels:
            severity: critical
          annotations:
            summary: "Webhooks in failed state — manual review needed"
        - alert: PaymentDiscrepanciesHigh
          expr: payment_discrepancies_unresolved > 5
          for: 10m
          labels:
            severity: critical
          annotations:
            summary: "Unresolved payment discrepancies > 5"
        - alert: WebhookSlowProcessing
          expr: histogram_quantile(0.95, rate(webhook_duration_seconds_bucket[5m])) > 5
          for: 5m
          labels:
            severity: warning
          annotations:
            summary: "Webhook p95 processing > 5s"

  kubectl apply -f k8s/production/payment-alerts.yaml

  6. Dead letter + runbook

-- migrations/005_dead_letter.sql
CREATE TABLE webhook_dead_letter (
  id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  event_id    TEXT  NOT NULL,
  type        TEXT  NOT NULL,
  payload     JSONB NOT NULL,
  last_error  TEXT,
  attempts    INTEGER,
  failed_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

  res.json({ received: true, queued: true });
};

<img width="1717" height="916" alt="ss9201" src="https://github.com/user-attachments/assets/6a5ffe73-a122-4d23-9391-c7fad75412a6" />
