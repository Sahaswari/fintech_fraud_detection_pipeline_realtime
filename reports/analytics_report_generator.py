"""
Fraud Detection Pipeline - Analytics Report Generator
Generates comprehensive fraud analysis reports with visualizations
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import psycopg2
from pathlib import Path

class FraudAnalytics:
    def __init__(self):
        """Initialize database connection"""
        self.conn = psycopg2.connect(
            host='localhost',
            port=5432,
            database='fraud_detection',
            user='fraud_user',
            password='fraud_pass'
        )
        
        # Set style
        sns.set_style('whitegrid')
        plt.rcParams['figure.figsize'] = (12, 6)
        
        # Create output directory
        self.output_dir = Path('reports')
        self.output_dir.mkdir(exist_ok=True)
        
        print("=" * 70)
        print("📊 FRAUD DETECTION ANALYTICS REPORT GENERATOR")
        print("=" * 70)
    
    def load_data(self, days_back=7):
        """Load transaction data from database"""
        print(f"\n📥 Loading data from last {days_back} days...")
        
        start_date = datetime.now() - timedelta(days=days_back)
        
        # Load valid transactions
        valid_query = f"""
            SELECT * FROM valid_transactions 
            WHERE timestamp >= '{start_date}'
            ORDER BY timestamp
        """
        self.valid_df = pd.read_sql(valid_query, self.conn)
        print(f"✓ Loaded {len(self.valid_df)} valid transactions")
        
        # Load fraud alerts
        fraud_query = f"""
            SELECT * FROM fraud_alerts 
            WHERE timestamp >= '{start_date}'
            ORDER BY timestamp
        """
        self.fraud_df = pd.read_sql(fraud_query, self.conn)
        print(f"✓ Loaded {len(self.fraud_df)} fraud alerts")
        
        # Convert timestamps
        self.valid_df['timestamp'] = pd.to_datetime(self.valid_df['timestamp'])
        self.fraud_df['timestamp'] = pd.to_datetime(self.fraud_df['timestamp'])
    
    def analyze_fraud_by_category(self):
        """Analysis 1: Fraud Attempts by Merchant Category"""
        print("\n📈 Analysis 1: Fraud by Merchant Category")
        
        if len(self.fraud_df) == 0:
            print("⚠️  No fraud data available")
            return None
        
        fraud_by_cat = self.fraud_df.groupby('merchant_category').agg({
            'transaction_id': 'count',
            'amount': ['sum', 'mean']
        }).round(2)
        
        fraud_by_cat.columns = ['Count', 'Total Amount', 'Avg Amount']
        fraud_by_cat = fraud_by_cat.sort_values('Count', ascending=False)
        
        # Create visualization
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
        
        # Bar chart - fraud count
        fraud_by_cat['Count'].plot(kind='bar', ax=ax1, color='#e74c3c')
        ax1.set_title('Fraud Attempts by Merchant Category', fontsize=14, fontweight='bold')
        ax1.set_xlabel('Merchant Category')
        ax1.set_ylabel('Number of Fraud Attempts')
        ax1.tick_params(axis='x', rotation=45)
        
        # Bar chart - fraud amount
        fraud_by_cat['Total Amount'].plot(kind='bar', ax=ax2, color='#c0392b')
        ax2.set_title('Total Fraud Amount by Category', fontsize=14, fontweight='bold')
        ax2.set_xlabel('Merchant Category')
        ax2.set_ylabel('Total Amount ($)')
        ax2.tick_params(axis='x', rotation=45)
        
        plt.tight_layout()
        plt.savefig(self.output_dir / 'fraud_by_category.png', dpi=300, bbox_inches='tight')
        print(f"✓ Saved: {self.output_dir / 'fraud_by_category.png'}")
        
        return fraud_by_cat
    
    def analyze_fraud_types(self):
        """Analysis 2: Fraud Types Distribution"""
        print("\n📈 Analysis 2: Fraud Types Distribution")
        
        if len(self.fraud_df) == 0:
            print("⚠️  No fraud data available")
            return None
        
        fraud_types = self.fraud_df.groupby('fraud_type').agg({
            'transaction_id': 'count',
            'amount': 'sum'
        }).round(2)
        
        fraud_types.columns = ['Count', 'Total Amount']
        
        # Create visualization
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
        
        # Pie chart - count
        ax1.pie(fraud_types['Count'], labels=fraud_types.index, autopct='%1.1f%%',
                startangle=90, colors=['#e74c3c', '#e67e22'])
        ax1.set_title('Fraud Types Distribution (Count)', fontsize=14, fontweight='bold')
        
        # Pie chart - amount
        ax2.pie(fraud_types['Total Amount'], labels=fraud_types.index, autopct='%1.1f%%',
                startangle=90, colors=['#c0392b', '#d35400'])
        ax2.set_title('Fraud Types Distribution (Amount)', fontsize=14, fontweight='bold')
        
        plt.tight_layout()
        plt.savefig(self.output_dir / 'fraud_types.png', dpi=300, bbox_inches='tight')
        print(f"✓ Saved: {self.output_dir / 'fraud_types.png'}")
        
        return fraud_types
    
    def analyze_time_patterns(self):
        """Analysis 3: Time-based patterns"""
        print("\n📈 Analysis 3: Time-based Patterns")
        
        # Combine all transactions
        all_trans = pd.concat([
            self.valid_df.assign(is_fraud=False),
            self.fraud_df.assign(is_fraud=True)
        ])
        
        all_trans['hour'] = all_trans['timestamp'].dt.hour
        all_trans['date'] = all_trans['timestamp'].dt.date
        
        # Hourly distribution
        hourly = all_trans.groupby(['hour', 'is_fraud']).size().unstack(fill_value=0)
        
        # Daily volume
        daily = all_trans.groupby(['date', 'is_fraud']).size().unstack(fill_value=0)
        
        # Create visualization
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(15, 10))
        
        # Hourly pattern
        hourly.plot(kind='bar', ax=ax1, color=['#2ecc71', '#e74c3c'])
        ax1.set_title('Transaction Volume by Hour of Day', fontsize=14, fontweight='bold')
        ax1.set_xlabel('Hour of Day')
        ax1.set_ylabel('Transaction Count')
        ax1.legend(['Valid', 'Fraud'])
        ax1.grid(axis='y', alpha=0.3)
        
        # Daily trend
        daily.plot(kind='line', ax=ax2, marker='o', color=['#2ecc71', '#e74c3c'])
        ax2.set_title('Daily Transaction Trend', fontsize=14, fontweight='bold')
        ax2.set_xlabel('Date')
        ax2.set_ylabel('Transaction Count')
        ax2.legend(['Valid', 'Fraud'])
        ax2.grid(alpha=0.3)
        plt.xticks(rotation=45)
        
        plt.tight_layout()
        plt.savefig(self.output_dir / 'time_patterns.png', dpi=300, bbox_inches='tight')
        print(f"✓ Saved: {self.output_dir / 'time_patterns.png'}")
        
        return hourly, daily
    
    def analyze_geographic_patterns(self):
        """Analysis 4: Geographic patterns"""
        print("\n📈 Analysis 4: Geographic Patterns")
        
        if len(self.fraud_df) == 0:
            print("⚠️  No fraud data available")
            return None
        
        fraud_by_location = self.fraud_df.groupby('location').agg({
            'transaction_id': 'count',
            'amount': 'sum'
        }).round(2)
        
        fraud_by_location.columns = ['Fraud Count', 'Total Amount']
        fraud_by_location = fraud_by_location.sort_values('Fraud Count', ascending=False)
        
        # Create visualization
        fig, ax = plt.subplots(figsize=(12, 6))
        
        fraud_by_location['Fraud Count'].plot(kind='barh', ax=ax, color='#e74c3c')
        ax.set_title('Fraud Attempts by Location', fontsize=14, fontweight='bold')
        ax.set_xlabel('Number of Fraud Attempts')
        ax.set_ylabel('Location')
        
        plt.tight_layout()
        plt.savefig(self.output_dir / 'fraud_by_location.png', dpi=300, bbox_inches='tight')
        print(f"✓ Saved: {self.output_dir / 'fraud_by_location.png'}")
        
        return fraud_by_location
    
    def generate_summary_report(self):
        """Generate comprehensive summary report"""
        print("\n📋 Generating Summary Report...")
        
        total_valid = len(self.valid_df)
        total_fraud = len(self.fraud_df)
        total_trans = total_valid + total_fraud
        
        valid_amount = self.valid_df['amount'].sum()
        fraud_amount = self.fraud_df['amount'].sum()
        total_amount = valid_amount + fraud_amount
        
        report = f"""
{'=' * 70}
                    FRAUD DETECTION ANALYTICS REPORT
                    Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
{'=' * 70}

EXECUTIVE SUMMARY
{'=' * 70}
Total Transactions Processed:     {total_trans:,}
Valid Transactions:               {total_valid:,} ({total_valid/total_trans*100:.2f}%)
Fraudulent Transactions:          {total_fraud:,} ({total_fraud/total_trans*100:.2f}%)

Total Transaction Volume:         ${total_amount:,.2f}
Valid Transaction Amount:         ${valid_amount:,.2f}
Fraudulent Transaction Amount:    ${fraud_amount:,.2f}

Average Valid Transaction:        ${valid_amount/total_valid if total_valid > 0 else 0:,.2f}
Average Fraud Transaction:        ${fraud_amount/total_fraud if total_fraud > 0 else 0:,.2f}

{'=' * 70}
TOP FRAUD CATEGORIES
{'=' * 70}
"""
        
        if len(self.fraud_df) > 0:
            top_cats = self.fraud_df['merchant_category'].value_counts().head(5)
            for cat, count in top_cats.items():
                report += f"{cat:<25} {count:>5} attempts\n"
        
        report += f"""
{'=' * 70}
FRAUD DETECTION EFFECTIVENESS
{'=' * 70}
Fraud Detection Rate:             {total_fraud/total_trans*100:.2f}%
Amount Saved (Fraud Prevented):   ${fraud_amount:,.2f}

{'=' * 70}
RECOMMENDATIONS
{'=' * 70}
"""
        
        if len(self.fraud_df) > 0:
            top_cat = self.fraud_df['merchant_category'].value_counts().idxmax()
            report += f"1. Increase monitoring for {top_cat} category\n"
            report += f"2. Review transactions over ${self.fraud_df['amount'].quantile(0.75):.2f}\n"
            report += f"3. Enhanced verification for high-risk locations\n"
        else:
            report += "No fraud detected in this period. Continue monitoring.\n"
        
        report += f"\n{'=' * 70}\n"
        
        # Save report
        report_file = self.output_dir / f"fraud_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        with open(report_file, 'w') as f:
            f.write(report)
        
        print(report)
        print(f"✓ Report saved: {report_file}")
        
        return report
    
    def run_full_analysis(self):
        """Run complete analysis pipeline"""
        print("\n🚀 Starting Full Analysis Pipeline...\n")
        
        # Load data
        self.load_data(days_back=7)
        
        # Run all analyses
        fraud_by_cat = self.analyze_fraud_by_category()
        fraud_types = self.analyze_fraud_types()
        time_patterns = self.analyze_time_patterns()
        geo_patterns = self.analyze_geographic_patterns()
        
        # Generate summary
        report = self.generate_summary_report()
        
        print("\n" + "=" * 70)
        print("✅ ANALYSIS COMPLETE!")
        print("=" * 70)
        print(f"📁 All reports saved in: {self.output_dir.absolute()}")
        print("=" * 70)
        
        self.conn.close()


if __name__ == "__main__":
    analytics = FraudAnalytics()
    analytics.run_full_analysis()