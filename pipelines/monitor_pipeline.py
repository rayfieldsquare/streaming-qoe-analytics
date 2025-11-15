import pandas as pd
import json
from datetime import datetime, timedelta
import os

class PipelineMonitor:
    """
    Monitor pipeline health and send alerts.
    """

    def __init__(self, reports_dir='quality_reports'):
        BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
        REPORT_DIR = os.path.join(BASE_DIR, 'reports')
        self.reports_dir = os.path.join(REPORT_DIR, reports_dir)
        if not os.path.exists(self.reports_dir):
            os.makedirs(self.reports_dir)

    def check_recent_runs(self, hours=24):
        """Check if pipeline has run successfully in last N hours."""
        cutoff_time = datetime.now() - timedelta(hours=hours)

        # Find recent reports
        recent_reports = []

        for filename in os.listdir(self.reports_dir):
            if filename.endswith('.json'):
                filepath = os.path.join(self.reports_dir, filename)
                with open(filepath, 'r') as f:
                    report = json.load(f)

                report_time = datetime.strptime(
                    report['pipeline_run_date'],
                    '%Y-%m-%d %H:%M:%S'
                )

                if report_time > cutoff_time:
                    recent_reports.append(report)

        if not recent_reports:
            self.send_alert(f"⚠️ No pipeline runs in last {hours} hours!")
            return False

        # Check if any failures
        failures = [r for r in recent_reports if r['pipeline_status'] != 'SUCCESS']

        if failures:
            self.send_alert(f"❌ {len(failures)} pipeline failures detected!")
            return False

        # Check quality scores
        low_quality = [r for r in recent_reports if r['quality_score'] < 95]

        if low_quality:
            self.send_alert(f"⚠️ {len(low_quality)} runs with low quality scores!")

        print(f"✅ Pipeline healthy: {len(recent_reports)} successful runs in last {hours}h")
        return True

    def send_alert(self, message):
        """Send alert (placeholder - would use Slack/email in production)."""
        print(f"\n🚨 ALERT: {message}")
        # In production:
        # slack_webhook.send(message)
        # or
        # send_email(to='team@netflix.com', subject='Pipeline Alert', body=message)


if __name__ == "__main__":
    monitor = PipelineMonitor()
    monitor.check_recent_runs(hours=24)
